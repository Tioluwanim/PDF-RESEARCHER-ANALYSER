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
    """Convert lightweight markdown to safe HTML for chat bubbles."""
    import re
    if not text:
        return ""
    t = html.escape(text)
    # Fenced code blocks
    t = re.sub(
        r"```(?:\w+\n)?(.*?)```",
        lambda m: (
            '<pre style="background:var(--surface);padding:0.7rem 1rem;'
            'border-radius:6px;font-family:var(--f-mono);'
            f'font-size:0.8rem;overflow-x:auto;margin:0.5rem 0;">'
            f'{m.group(1).strip()}</pre>'
        ),
        t, flags=re.DOTALL,
    )
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
        doc, err = analysis_service.save_upload(file_bytes=pdf_file.read(), filename=pdf_file.name)
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


def render_sidebar() -> None:
    st.sidebar.markdown(
        """
        ## PDF Research Analyzer
        Upload documents, manage cache, and access the library from here.
        """,
    )

    uploaded_file = st.sidebar.file_uploader(
        "Upload a file",
        type=["pdf", "docx", "doc", "txt", "xlsx", "xls", "csv"],
        help="Supported formats: PDF, DOCX, TXT, XLSX, XLS, CSV",
        key="sidebar_upload",
    )

    if uploaded_file is not None:
        handle_upload(uploaded_file)

    st.sidebar.markdown("---")
    st.sidebar.caption("Maximum upload size: 50 MB")

    if st.sidebar.button("Delete all documents", key="delete_all_docs"):
        delete_all_docs()
        st.success("All documents and cached data have been removed.")
