from __future__ import annotations

import html
import streamlit as st
from app.models.schemas import ChatMessage, MessageRole, SectionType
from app.services.analysis_service import analysis_service
from app.ui.shared import fmt_number, md_to_html, process_document, handle_chat


def render_doc_header(info: dict, chunks: dict) -> None:
    title = info.get("metadata", {}).get("title") or "Untitled document"
    authors = ", ".join(info.get("metadata", {}).get("authors", []))
    journal = info.get("metadata", {}).get("journal")
    is_ocr = info.get("metadata", {}).get("language") == "ocr"

    pills_html = ""
    if journal:
        pills_html += f'<span class="doc-journal-pill">📰 {journal[:60]}</span>'
    if is_ocr:
        pills_html += '<span class="ocr-badge">🔍 OCR</span>'

    c1, c2, c3, c4 = st.columns([3, 1, 1, 1])
    with c1:
        st.markdown(
            f'<div class="doc-header-card">'
            f'  <div class="doc-title">{title[:110]}</div>'
            + (f'  <div class="doc-authors">{authors}</div>' if authors else "")
            + (f'  <div style="margin-top:0.5rem;">{pills_html}</div>' if pills_html else "")
            + f'</div>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f'<div class="stat-card">'
            f'  <div class="stat-label">Pages</div>'
            f'  <div class="stat-value">{info.get("metadata", {}).get("pages", 0)}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            f'<div class="stat-card">'
            f'  <div class="stat-label">Words</div>'
            f'  <div class="stat-value">{fmt_number(info.get("metadata", {}).get("words", 0))}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    with c4:
        chunk_label = "Chunks" + (" · OCR" if is_ocr else "")
        st.markdown(
            f'<div class="stat-card">'
            f'  <div class="stat-label">{chunk_label}</div>'
            f'  <div class="stat-value">{chunks.get("total", 0)}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )


def render_chat_tab(doc_id: str, info: dict) -> None:
    status = info.get("status")
    if status != "ready":
        st.warning(
            f"Document status is **{status}**. Processing must complete before chatting.",
            icon="⏳",
        )
        if st.button("▶ Process Now", type="primary", key="process_now"):
            process_document(doc_id)
        return

    history: list[ChatMessage] = st.session_state.chat_history

    if not history:
        st.markdown(
            '<div style="text-align:center;padding:3rem 1rem;color:var(--muted);'
            'font-size:0.875rem;font-style:italic;line-height:1.7;">'
            '💬 Ask any question about this paper to begin the conversation.</div>',
            unsafe_allow_html=True,
        )
    else:
        chat_html = '<div class="chat-container">'
        for msg in history:
            if msg.role == MessageRole.USER:
                chat_html += (
                    '<div class="msg-wrap user">'
                    f'<div class="msg-bubble user">{html.escape(msg.content)}</div>'
                    '<div class="msg-avatar user">You</div>'
                    '</div>'
                )
            else:
                chat_html += (
                    '<div class="msg-wrap asst">'
                    '<div class="msg-avatar asst">📚</div>'
                    f'<div class="msg-bubble asst">{md_to_html(msg.content)}</div>'
                    '</div>'
                )
        chat_html += '</div>'
        st.markdown(chat_html, unsafe_allow_html=True)

    c_in, c_btn, c_clr = st.columns([7, 1, 1])
    with c_in:
        question = st.text_input(
            "question",
            placeholder="Ask anything about this paper …",
            label_visibility="collapsed",
            key="chat_input",
        )
    with c_btn:
        send = st.button("Send", type="primary", width="stretch", key="send_btn")
    with c_clr:
        if st.button("Clear", width="stretch", disabled=not history, key="clear_btn"):
            st.session_state.chat_history = []
            st.rerun()

    if not history:
        st.markdown(
            '<div style="font-family:var(--f-mono);font-size:0.65rem;'
            'color:#a09890;text-transform:uppercase;letter-spacing:0.08em;'
            'margin:0.75rem 0 0.4rem;">Suggested questions</div>',
            unsafe_allow_html=True,
        )
        suggestions = [
            "What is this paper about?",
            "What methods were used?",
            "What are the main findings?",
            "What do the authors conclude?",
        ]
        cols = st.columns(len(suggestions))
        for col, s in zip(cols, suggestions):
            with col:
                if st.button(s, key=f"sugg_{hash(s)}", width="stretch"):
                    handle_chat(doc_id, s)
                    st.rerun()

    if send and question and question.strip():
        handle_chat(doc_id, question.strip())


def render_sections_tab(doc_id: str, info: dict) -> None:
    sections = info.get("sections", [])
    if not sections:
        st.info("No sections detected in this document.")
        return

    section_order = [
        SectionType.ABSTRACT, SectionType.INTRODUCTION, SectionType.METHODS,
        SectionType.RESULTS, SectionType.DISCUSSION, SectionType.CONCLUSION,
        SectionType.REFERENCES, SectionType.OTHER,
    ]
    s_map = {s["type"]: s for s in sections}
    available = [s for s in section_order if s.value in s_map]
    if not available:
        st.info("No sections detected.")
        return

    labels = [s.value.capitalize() for s in available]
    idx = st.selectbox(
        "Section", range(len(labels)),
        format_func=lambda i: labels[i],
        label_visibility="collapsed",
    )
    if idx is None:
        return

    sel = available[idx]
    meta = s_map[sel.value]

    c1, c2, c3 = st.columns(3)
    c1.metric("Section", meta["type"].capitalize())
    c2.metric("Words", fmt_number(meta.get("word_count", 0)))
    c3.metric("Page", meta.get("page_start", 0) + 1)
    st.markdown("<br>", unsafe_allow_html=True)

    content = analysis_service.get_section_content(doc_id, sel)
    if content:
        truncated = content[:5000]
        st.markdown(
            f'<div class="section-block">{html.escape(truncated)}'
            f'{"…" if len(content) > 5000 else ""}</div>',
            unsafe_allow_html=True,
        )
        if len(content) > 5000:
            st.caption(f"Showing first 5,000 of {fmt_number(len(content))} characters.")
    else:
        st.info("Section content not available.")


def render_search_tab(doc_id: str) -> None:
    c1, c2 = st.columns([6, 1])
    with c1:
        query = st.text_input(
            "q", placeholder="Search within this paper …",
            label_visibility="collapsed", key="search_q",
        )
    with c2:
        top_k = st.selectbox("k", [3, 5, 10], index=1, label_visibility="collapsed")

    if st.button("Search", type="primary", key="search_btn"):
        if not query.strip():
            st.warning("Please enter a search query.", icon="⚠️")
        else:
            with st.spinner("Searching …"):
                results = analysis_service.semantic_search(
                    doc_id=doc_id, query=query.strip(), top_k=top_k,
                )

            if not results.results:
                st.info("No results found. Try a broader query or different keywords.")
                return

            st.markdown(
                f'<div style="font-family:var(--f-mono);'
                f'font-size:0.7rem;color:var(--muted);margin-bottom:0.75rem;">'
                f'{results.total_found} result{"s" if results.total_found != 1 else ""}'
                f' &nbsp;·&nbsp; {results.search_time_ms:.0f}ms</div>',
                unsafe_allow_html=True,
            )

            for r in results.results:
                pct = int(r.score * 100)
                text = html.escape(r.chunk.content[:450])
                dots = "…" if len(r.chunk.content) > 450 else ""
                st.markdown(f"""
                <div class="result-card">
                    <div style="display:flex;justify-content:space-between;
                                align-items:center;margin-bottom:0.4rem;">
                        <span class="result-section-tag">{r.chunk.section_type.value}</span>
                        <span class="result-score">{pct}% match</span>
                    </div>
                    <div class="result-text">{text}{dots}</div>
                </div>
                """, unsafe_allow_html=True)


def render_info_tab(info: dict) -> None:
    meta = info.get("metadata", {})
    chunks = info.get("chunks", {})
    secs = info.get("sections", [])

    st.markdown(
        '<div style="font-family:var(--f-display);font-size:1.1rem;'
        'font-weight:600;margin-bottom:1rem;">Document Metadata</div>',
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns(2)
    with c1:
        _meta_block("Title", meta.get("title") or "—")
        _meta_block("Authors", ", ".join(meta.get("authors", [])) or "—")
        _meta_block("Journal", meta.get("journal") or "—")
        _meta_block("Publisher", meta.get("publisher") or "—")
        lang = meta.get("language", "en")
        lang_display = "🔍 OCR Processed" if lang == "ocr" else lang.upper()
        _meta_block("Language / Mode", lang_display)
    with c2:
        _meta_block("Pages", str(meta.get("pages", 0)), large=True)
        _meta_block("Words", fmt_number(meta.get("words", 0)), large=True)
        _meta_block("DOI", meta.get("doi") or "—")
        _meta_block("ISSN", meta.get("issn") or "—")
        vol = meta.get("volume", "")
        issue = meta.get("issue", "")
        vol_issue = ((f"Vol {vol}" if vol else "") + (f", No {issue}" if issue else "")) or "—"
        _meta_block("Vol / Issue", vol_issue)
        _meta_block("File Size", meta.get("file_size", "—"))

    kws = meta.get("keywords", [])
    if kws:
        st.markdown(
            '<div style="font-family:var(--f-display);font-size:1rem;'
            'font-weight:600;margin:1rem 0 0.5rem;">Keywords</div>',
            unsafe_allow_html=True,
        )
        kw_html = " ".join(
            f'<span style="display:inline-block;background:var(--surface);'
            f'border:1px solid var(--border);border-radius:20px;'
            f'padding:0.18rem 0.65rem;font-family:var(--f-mono);'
            f'font-size:0.68rem;margin:0.2rem 0.2rem 0 0;color:var(--ink);">{html.escape(k)}</span>'
            for k in kws
        )
        st.markdown(kw_html, unsafe_allow_html=True)

    st.markdown(
        '<div style="font-family:var(--f-display);font-size:1.1rem;'
        'font-weight:600;margin:1.5rem 0 0.75rem;">Sections Detected</div>',
        unsafe_allow_html=True,
    )
    if secs:
        rows = "".join(
            f'<div class="section-row">'
            f'<span style="font-weight:500;">{s["type"].capitalize()}</span>'
            f'<span style="font-family:var(--f-mono);font-size:0.72rem;color:var(--muted);">'
            f'{fmt_number(s["word_count"])} words · p.{s["page_start"]+1}</span>'
            f'</div>'
            for s in secs
        )
        st.markdown(
            f'<div style="border:1px solid var(--border);border-radius:10px;overflow:hidden;">{rows}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("No sections detected.")

    st.markdown(
        '<div style="font-family:var(--f-display);font-size:1.1rem;'
        'font-weight:600;margin:1.5rem 0 0.75rem;">Vector Index</div>',
        unsafe_allow_html=True,
    )
    ci, cv = st.columns(2)
    with ci:
        _meta_block("Total Chunks", str(chunks.get("total", 0)), large=True)
    with cv:
        _meta_block("Indexed Vectors", str(chunks.get("indexed", 0)), large=True)

    created = info.get("created_at", "")[:19].replace("T", " ")
    if created:
        st.markdown(
            f'<div style="margin-top:1rem;font-family:var(--f-mono);'
            f'font-size:0.68rem;color:var(--muted);">Created {created} UTC</div>',
            unsafe_allow_html=True,
        )


def _meta_block(label: str, value: str, large: bool = False) -> None:
    val_cls = "meta-value-lg" if large else "meta-value"
    st.markdown(
        f'<div class="meta-block">'
        f'<div class="meta-label">{label}</div>'
        f'<div class="{val_cls}">{html.escape(str(value))}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


def render_empty_state() -> None:
    st.markdown("""
    <div class="empty-state">
        <div class="empty-icon-ring">📄</div>
        <div class="empty-title">Drop a paper to begin</div>
        <div class="empty-sub">
            Upload a PDF, DOCX, TXT, or spreadsheet from the sidebar.
            Your document will be extracted, indexed, and ready to chat with in seconds.
        </div>
        <div style="display:flex;gap:1rem;flex-wrap:wrap;justify-content:center;">
            <div class="feature-chip">
                <div class="feature-chip-icon">💬</div>
                <div class="feature-chip-label">Ask questions</div>
            </div>
            <div class="feature-chip">
                <div class="feature-chip-icon">🔍</div>
                <div class="feature-chip-label">Semantic search</div>
            </div>
            <div class="feature-chip">
                <div class="feature-chip-icon">📑</div>
                <div class="feature-chip-label">Section view</div>
            </div>
            <div class="feature-chip">
                <div class="feature-chip-icon">📤</div>
                <div class="feature-chip-label">Export metadata</div>
            </div>
        </div>
        <div style="margin-top:2rem;font-family:var(--f-mono);font-size:0.65rem;
                    color:var(--muted-2);letter-spacing:0.08em;">
            PDF · DOCX · TXT · XLSX · CSV &nbsp;·&nbsp; Up to 50MB
        </div>
    </div>
    """, unsafe_allow_html=True)