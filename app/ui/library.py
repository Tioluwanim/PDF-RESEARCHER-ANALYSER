from __future__ import annotations

import html

import streamlit as st

from app.config import SIMILARITY_THRESHOLD
from app.models.schemas import ChatMessage, MessageRole, SectionType
from app.services.analysis_service import analysis_service
from app.services.drive_service import drive_service


SECTION_OPTIONS = [
    "Any",
    "abstract",
    "introduction",
    "methods",
    "results",
    "discussion",
    "conclusion",
    "references",
    "other",
]


def render_library_search_tab() -> None:
    st.markdown(
        '<div class="section-header">'
        '<div style="font-size:1.25rem;font-weight:700;">Research library</div>'
        '<div style="color:var(--muted);margin-top:0.25rem;">'
        'Catalog, sync, search, chat, export readiness, and processing logs.</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    catalog, sync, search, chat, exports, logs = st.tabs(
        ["Catalog", "Sync", "Search", "Chat", "Exports", "Logs"]
    )
    with catalog:
        _render_catalog()
    with sync:
        _render_sync()
    with search:
        _render_search()
    with chat:
        _render_library_chat()
    with exports:
        _render_export_readiness()
    with logs:
        _render_logs()


def _render_catalog() -> None:
    stats = analysis_service.get_library_stats()
    docs = analysis_service.list_documents()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Documents", stats.get("total_documents", len(docs)))
    c2.metric("Ready", stats.get("ready_documents", 0))
    c3.metric("Failed", stats.get("failed_documents", 0))
    c4.metric("Pages", stats.get("total_pages", 0))
    c5.metric("Chunks", stats.get("total_chunks", 0))

    if not docs:
        st.info("No documents yet. Upload files or sync Google Drive to build the library.")
        return

    rows = []
    for doc in docs:
        rows.append(
            {
                "Status": doc.get("status", ""),
                "Filename": doc.get("filename", ""),
                "Title": doc.get("title", ""),
                "Authors": ", ".join(doc.get("authors") or []),
                "Pages": doc.get("pages", 0),
                "Chunks": doc.get("chunks", 0),
                "Source": doc.get("source", ""),
                "Updated": doc.get("updated_at", ""),
                "Error": doc.get("last_error") or "",
            }
        )
    st.dataframe(rows, use_container_width=True, hide_index=True)

    selected = st.selectbox(
        "Open document",
        [""] + [doc["filename"] for doc in docs],
        key="catalog_open_doc",
    )
    if selected and st.button("Open selected document", key="catalog_open_btn"):
        doc_id = next(doc["doc_id"] for doc in docs if doc["filename"] == selected)
        st.session_state.active_doc_id = doc_id
        st.session_state.app_mode = "📄 Single PDF"
        st.rerun()


def _render_sync() -> None:
    c1, c2 = st.columns([2, 1])
    with c1:
        st.markdown("**Google Drive ingestion**")
        if drive_service.is_configured:
            st.caption(f"Configured folder: {drive_service.folder_id}")
        else:
            st.warning("Drive sync is not configured. Set GOOGLE_DRIVE_FOLDER_ID and GOOGLE_CREDENTIALS_PATH.")
    with c2:
        if st.button("Rebuild library index", use_container_width=True, key="library_rebuild_index"):
            ok = analysis_service.rebuild_library_index()
            st.success("Library index rebuilt.") if ok else st.warning("No ready embedded chunks found.")

    if st.button("Sync Drive and process changes", type="primary", key="library_drive_sync"):
        progress = st.empty()

        def on_file_found(name: str, idx: int, total: int) -> None:
            progress.info(f"Checking {idx}/{total}: {name}")

        result = analysis_service.sync_drive(on_file_found=on_file_found)
        jobs = analysis_service.process_pending_ingestion_jobs(limit=100)
        analysis_service.rebuild_library_index()
        progress.empty()
        st.success(
            f"Sync complete: {result.get('new', 0)} changed, "
            f"{result.get('skipped', 0)} skipped, {result.get('failed', 0)} failed. "
            f"Ingestion: {jobs.get('succeeded', 0)} succeeded, {jobs.get('failed', 0)} failed."
        )

    runs = analysis_service.get_recent_sync_runs(limit=10)
    if runs:
        st.markdown("**Recent sync runs**")
        st.dataframe(
            [
                {
                    "Started": r.started_at.isoformat() if r.started_at else "",
                    "Status": r.status,
                    "Total": r.total_files,
                    "Changed": r.new_files,
                    "Skipped": r.skipped_files,
                    "Failed": r.failed_files,
                    "Error": r.error_message or "",
                }
                for r in runs
            ],
            use_container_width=True,
            hide_index=True,
        )


def _render_search() -> None:
    docs = analysis_service.list_documents()
    doc_options = ["All documents"] + [doc["filename"] for doc in docs]
    doc_map = {doc["filename"]: doc["doc_id"] for doc in docs}

    query = st.text_input("Library query", placeholder="Search across the whole library...", key="library_search_q")
    c1, c2, c3 = st.columns([3, 2, 2])
    with c1:
        selected_doc = st.selectbox("Document filter", doc_options, key="library_search_doc")
    with c2:
        section_opt = st.selectbox("Section filter", SECTION_OPTIONS, index=0, key="library_search_section")
    with c3:
        top_k = st.selectbox("Top k", [3, 5, 10], index=1, key="library_search_k")

    author = st.text_input("Author filter", key="library_search_author")
    year = st.text_input("Year filter", key="library_search_year")

    if st.button("Search library", type="primary", key="library_search_btn"):
        if not query.strip():
            st.warning("Enter a query to search the library.", icon="⚠️")
            return
        doc_id_filter = [doc_map[selected_doc]] if selected_doc != "All documents" else None
        section_type = _section_type(section_opt)

        with st.spinner("Searching library..."):
            results = analysis_service.library_search(
                query=query.strip(),
                top_k=top_k,
                threshold=SIMILARITY_THRESHOLD,
                doc_ids=doc_id_filter,
                author=author.strip() or None,
                year=year.strip() or None,
                section_type=section_type,
            )

        if not results.results:
            st.info("No results found. Try a broader query or different filters.")
            return

        st.caption(f"{results.total_found} result(s) in {results.search_time_ms:.0f}ms")
        doc_labels = {doc["doc_id"]: doc["filename"] for doc in docs}
        for r in results.results:
            pct = int(r.score * 100)
            title = html.escape(doc_labels.get(r.chunk.doc_id, r.chunk.doc_id))
            text = html.escape(r.chunk.content[:500])
            st.markdown(
                f"""
                <div class="result-card">
                    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.4rem;">
                        <div>
                            <span class="result-section-tag">{r.chunk.section_type.value}</span>
                            <span style="margin-left:0.5rem;color:var(--muted);">{title}</span>
                        </div>
                        <span class="result-score">{pct}% match</span>
                    </div>
                    <div style="font-family:var(--f-mono);font-size:0.68rem;color:var(--muted);margin-bottom:0.35rem;">
                        Source: {r.chunk.doc_id} · page {r.chunk.page_number} · chunk {r.chunk.chunk_index + 1}
                    </div>
                    <div class="result-text">{text}{'...' if len(r.chunk.content) > 500 else ''}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def _render_library_chat() -> None:
    docs = analysis_service.list_documents()
    doc_options = ["All documents"] + [doc["filename"] for doc in docs]
    doc_map = {doc["filename"]: doc["doc_id"] for doc in docs}
    selected_doc = st.selectbox("Scope", doc_options, key="library_chat_doc")
    top_k = st.selectbox("Context chunks", [5, 8, 12], index=1, key="library_chat_k")
    question = st.text_input("Ask the library", placeholder="Compare findings across the synced PDFs...", key="library_chat_q")

    if "library_chat_history" not in st.session_state:
        st.session_state.library_chat_history = []

    if st.button("Ask", type="primary", key="library_chat_btn"):
        if not question.strip():
            st.warning("Enter a question.", icon="⚠️")
            return
        doc_ids = [doc_map[selected_doc]] if selected_doc != "All documents" else None
        history = st.session_state.library_chat_history
        history.append(ChatMessage(role=MessageRole.USER, content=question.strip()))
        reply = ""
        placeholder = st.empty()
        with st.spinner("Reading across the library..."):
            for token in analysis_service.library_chat_stream(
                question=question.strip(),
                history=history[:-1],
                top_k=top_k,
                doc_ids=doc_ids,
            ):
                reply += str(token)
                placeholder.markdown(reply)
        history.append(ChatMessage(role=MessageRole.ASSISTANT, content=reply or "No response."))

    for msg in st.session_state.library_chat_history[-8:]:
        role = "You" if msg.role == MessageRole.USER else "Library"
        st.markdown(f"**{role}:** {html.escape(msg.content)}")


def _render_export_readiness() -> None:
    docs = analysis_service.list_documents()
    ready = [doc for doc in docs if doc.get("status") == "ready"]
    st.metric("Export-ready documents", len(ready))
    st.caption("Use the Export mode to generate XLSX, DOCX, CSV, or integration JSON.")
    if ready:
        st.dataframe(
            [
                {
                    "Filename": doc.get("filename"),
                    "Title": doc.get("title"),
                    "Authors": ", ".join(doc.get("authors") or []),
                    "Pages": doc.get("pages"),
                    "Chunks": doc.get("chunks"),
                }
                for doc in ready
            ],
            use_container_width=True,
            hide_index=True,
        )


def _render_logs() -> None:
    rows = analysis_service.get_recent_logs(limit=100)
    if not rows:
        st.info("No processing logs yet.")
        return
    st.dataframe(rows, use_container_width=True, hide_index=True)


def _section_type(value: str) -> SectionType | None:
    if value == "Any":
        return None
    try:
        return SectionType(value)
    except ValueError:
        return None
