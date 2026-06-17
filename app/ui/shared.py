from __future__ import annotations

import html
import traceback
from pathlib import Path

import streamlit as st
from app.config import MAX_CHAT_HISTORY, PROCESSED_DIR, VECTORSTORE_DIR, UPLOAD_DIR
from app.models.schemas import ChatMessage, MessageRole
from app.services.analysis_service import analysis_service
from app.db.repository import repository
from app.utils.logger import get_logger

logger = get_logger(__name__)


def fmt_number(value: int | float) -> str:
    try:
        return f"{value:,}"
    except Exception:
        return str(value)


def md_to_html(text: str) -> str:
    """
    Convert lightweight markdown to safe HTML for chat bubbles.

    Code blocks are extracted and stashed BEFORE the html.escape() pass,
    then restored with their own escaping afterward. Doing it the other
    way around (escape first, then regex for ``` fences) means the fence
    markers and content are already entity-encoded by the time the fence
    regex runs, so it silently fails to match and the code block leaks
    through as raw escaped text instead of a <pre> block.
    """
    import re
    if not text:
        return ""

    # Step 1 — stash fenced code blocks with their RAW (unescaped) content
    code_blocks: list[str] = []
    _FENCE = re.compile(r"```(?:\w+\n)?(.*?)```", re.DOTALL)

    def _stash(m: re.Match) -> str:
        code_blocks.append(m.group(1).strip())
        return f"\x00CODEBLOCK{len(code_blocks) - 1}\x00"

    text = _FENCE.sub(_stash, text)

    # Step 2 — escape everything else
    t = html.escape(text)

    # Step 3 — restore code blocks, escaping their content independently
    for i, raw_code in enumerate(code_blocks):
        escaped_code = html.escape(raw_code)
        t = t.replace(
            f"\x00CODEBLOCK{i}\x00",
            '<pre style="background:var(--surface);padding:0.7rem 1rem;'
            'border-radius:6px;font-family:var(--f-mono);'
            f'font-size:0.8rem;overflow-x:auto;margin:0.5rem 0;">{escaped_code}</pre>',
        )

    # Step 4 — inline markdown on the already-escaped text
    t = re.sub(r"`([^`]+)`", r'<code>\1</code>', t)
    t = re.sub(r"\*\*(.+?)\*\*", r'<strong>\1</strong>', t)
    t = re.sub(r"\*(.+?)\*", r'<em>\1</em>', t)

    def _bullets(m: re.Match) -> str:
        items = re.findall(r"^[-•]\s+(.+)$", m.group(0), re.MULTILINE)
        return "<ul>" + "".join(f"<li>{i}</li>" for i in items) + "</ul>"

    def _nums(m: re.Match) -> str:
        items = re.findall(r"^\d+\.\s+(.+)$", m.group(0), re.MULTILINE)
        return "<ol>" + "".join(f"<li>{i}</li>" for i in items) + "</ol>"

    t = re.sub(r"(^[-•]\s+.+$\n?)+", _bullets, t, flags=re.MULTILINE)
    t = re.sub(r"(^\d+\.\s+.+$\n?)+", _nums, t, flags=re.MULTILINE)

    paragraphs = [p.strip() for p in re.split(r"\n\n+", t) if p.strip()]
    result = []
    for p in paragraphs:
        if p.startswith(("<ul>", "<ol>", "<pre>")):
            result.append(p)
        else:
            result.append(f"<p>{p.replace(chr(10), '<br>')}</p>")
    return "\n".join(result)


def delete_doc_cache(doc_id: str) -> None:
    try:
        analysis_service.delete_document(doc_id)
    except Exception as e:
        logger.error("Failed to delete document cache %s: %s", doc_id, e)


def delete_all_docs() -> None:
    try:
        repository.delete_all_documents()
        import shutil
        for folder in [PROCESSED_DIR, VECTORSTORE_DIR, UPLOAD_DIR]:
            p = Path(folder)
            if p.exists():
                shutil.rmtree(p, ignore_errors=True)
            p.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error("Failed to delete all document cache: %s", e)


def process_document(doc_id: str) -> None:
    with st.spinner("Processing document…"):
        analysis_service.process_document(doc_id, reprocess=True)
    st.success("Document processing triggered.")
    st.rerun()


def handle_chat(doc_id: str, question: str) -> None:
    try:
        st.session_state.chat_history.append(
            ChatMessage(role=MessageRole.USER, content=question)
        )
        if len(st.session_state.chat_history) > MAX_CHAT_HISTORY:
            st.session_state.chat_history = st.session_state.chat_history[-MAX_CHAT_HISTORY:]

        typing = st.empty()
        typing.markdown(
            '<div class="msg-wrap asst">'
            '<div class="msg-avatar asst">📚</div>'
            '<div class="typing-indicator">'
            '<div class="typing-dot"></div>'
            '<div class="typing-dot"></div>'
            '<div class="typing-dot"></div>'
            '</div></div>',
            unsafe_allow_html=True,
        )

        container = st.empty()
        full_reply = ""

        stream = analysis_service.chat_stream(
            doc_id=doc_id,
            question=question,
            history=st.session_state.chat_history[:-1],
        )

        for token in stream:
            if not token:
                continue
            full_reply += str(token)
            typing.empty()
            container.markdown(
                '<div class="msg-wrap asst">'
                '<div class="msg-avatar asst">📚</div>'
                f'<div class="msg-bubble asst">{md_to_html(full_reply)}'
                f'<span style="color:var(--accent);animation:pulse 1s infinite;">▌</span></div>'
                '</div>',
                unsafe_allow_html=True,
            )

        typing.empty()
        container.empty()

        st.session_state.chat_history.append(
            ChatMessage(role=MessageRole.ASSISTANT, content=full_reply or "⚠️ No response received."),
        )
        st.rerun()

    except Exception as exc:
        logger.error("Chat failed: %s\n%s", exc, traceback.format_exc())
        st.error("⚠️ Chat error. Please try again.")
        if st.session_state.chat_history and st.session_state.chat_history[-1].role == MessageRole.USER:
            st.session_state.chat_history.pop()


def handle_upload(pdf_file) -> None:
    try:
        # getvalue() is preferred over read() for Streamlit's UploadedFile:
        # it returns the buffer without advancing/consuming a stream
        # position, so it stays safe even if something else touched the
        # file object earlier in this run (e.g. a size-preview read).
        try:
            file_bytes = pdf_file.getvalue()
        except Exception:
            file_bytes = pdf_file.read()

        doc, err = analysis_service.save_upload(file_bytes=file_bytes, filename=pdf_file.name)
        if err or not doc:
            message = getattr(err, "detail", str(err)) if err else "Upload failed"
            st.error(message)
            return
        st.session_state.active_doc_id = doc.doc_id if hasattr(doc, "doc_id") else doc["doc_id"]
        st.success("Upload complete. Document is queued for processing.")
        st.rerun()
    except Exception as exc:
        logger.error("Upload failed: %s", exc)
        st.error("Upload failed. Please try again.")
