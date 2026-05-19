from __future__ import annotations

import html
import os
from pathlib import Path

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
        """
        <div class="section-header">
            <div style="font-size:1.25rem;font-weight:700;">
                Research library
            </div>
            <div style="color:var(--muted);margin-top:0.25rem;">
                Catalog, sync, semantic search, AI chat, export readiness,
                and processing logs.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    catalog, sync, search, chat, exports, logs = st.tabs(
        [
            "Catalog",
            "Sync",
            "Search",
            "Chat",
            "Exports",
            "Logs",
        ]
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


# =============================================================================
# CATALOG
# =============================================================================


def _render_catalog() -> None:
    stats = analysis_service.get_library_stats()
    docs = analysis_service.list_documents()

    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric(
        "Documents",
        stats.get("total_documents", len(docs)),
    )

    c2.metric(
        "Ready",
        stats.get("ready_documents", 0),
    )

    c3.metric(
        "Failed",
        stats.get("failed_documents", 0),
    )

    c4.metric(
        "Pages",
        stats.get("total_pages", 0),
    )

    c5.metric(
        "Chunks",
        stats.get("total_chunks", 0),
    )

    if not docs:
        st.info(
            "No documents yet. Upload files or sync Google Drive "
            "to build the research library."
        )
        return

    rows: list[dict] = []

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

    st.dataframe(
        rows,
        width="stretch",
        hide_index=True,
    )

    selected = st.selectbox(
        "Open document",
        [""] + [doc["filename"] for doc in docs],
        key="catalog_open_doc",
    )

    if selected and st.button(
        "Open selected document",
        key="catalog_open_btn",
    ):
        doc_id = next(
            doc["doc_id"]
            for doc in docs
            if doc["filename"] == selected
        )

        st.session_state.active_doc_id = doc_id
        st.session_state.app_mode = "📄 Single PDF"

        st.rerun()


# =============================================================================
# SYNC
# =============================================================================


def _render_sync() -> None:
    auth_state = drive_service.get_auth_state()

    st.markdown("### Google Drive Sync")

    # ──────────────────────────────────────────────────────────────────────
    # Folder input
    # ──────────────────────────────────────────────────────────────────────

    current_folder = drive_service.folder_id or ""

    col_url, col_save = st.columns([5, 1])

    with col_url:
        folder_url = st.text_input(
            "Google Drive folder URL or file URL",
            value=current_folder,
            placeholder=(
                "https://drive.google.com/drive/folders/..."
            ),
            help=(
                "Paste a Google Drive folder URL or a direct file URL."
            ),
            key="drive_folder_url_input",
        )

    with col_save:
        st.markdown(
            "<div style='padding-top:1.85rem;'>",
            unsafe_allow_html=True,
        )

        save_clicked = st.button(
            "Save",
            width="stretch",
            key="drive_folder_save",
        )

        st.markdown(
            "</div>",
            unsafe_allow_html=True,
        )

    if save_clicked:
        if not folder_url.strip():
            st.warning(
                "Please paste a Google Drive folder URL first."
            )
        else:
            drive_service.set_folder_id(folder_url.strip())

            parsed = drive_service.folder_id

            if parsed:
                os.environ["GOOGLE_DRIVE_FOLDER_ID"] = parsed

                st.success(
                    f"✓ Google Drive resource saved: `{parsed}`"
                )

                st.rerun()

            else:
                st.error(
                    "Could not parse a valid Google Drive ID "
                    "from the provided URL."
                )

    st.markdown(
        "<hr style='margin:0.75rem 0;'>",
        unsafe_allow_html=True,
    )

    # ──────────────────────────────────────────────────────────────────────
    # Auth state
    # ──────────────────────────────────────────────────────────────────────

    if auth_state.status == "missing_folder":
        st.info(
            "Paste your Google Drive folder URL above and click Save."
        )

    elif auth_state.status == "missing_credentials":
        st.error(auth_state.message)

        st.code(
            """
Required options:

1. OAuth login (recommended)
   - GOOGLE_OAUTH_CLIENT_JSON
   - GOOGLE_OAUTH_REDIRECT_URI

OR

2. Service account
   - GOOGLE_CREDENTIALS_PATH
   - Share folder with the service account email
            """.strip()
        )

    elif auth_state.status == "oauth_login_required":
        st.warning(
            "Google OAuth is configured, but login is required."
        )

        if auth_state.authorization_url:
            st.link_button(
                "Sign in with Google",
                auth_state.authorization_url,
                use_container_width=True,
            )

        st.caption(
            "After authorizing the app, copy the authorization "
            "code and paste it below."
        )

        auth_code = st.text_input(
            "Authorization code",
            type="password",
            key="google_oauth_code",
        )

        if st.button(
            "Complete Google Login",
            type="primary",
            key="google_oauth_complete_btn",
        ):
            if not auth_code.strip():
                st.warning(
                    "Paste the authorization code first."
                )
            else:
                with st.spinner(
                    "Completing Google authentication..."
                ):
                    result = drive_service.exchange_authorization_code(
                        auth_code.strip()
                    )

                if result.get("success"):
                    st.success(
                        "Google Drive authentication completed."
                    )
                    st.rerun()

                else:
                    st.error(
                        result.get(
                            "error",
                            "OAuth authentication failed.",
                        )
                    )

    elif auth_state.status == "ready":
        st.success(
            f"Drive authentication ready "
            f"({auth_state.auth_mode})."
        )

        folder_info = drive_service.get_folder_info()

        if folder_info and not folder_info.get("error"):
            mime_type = folder_info.get("mimeType")

            if mime_type == "application/vnd.google-apps.folder":
                st.caption(
                    f"Folder: {folder_info.get('name', 'Unknown')}"
                )
            else:
                st.caption(
                    f"File: {folder_info.get('name', 'Unknown')}"
                )

        else:
            st.warning(
                folder_info.get(
                    "error",
                    "Could not read Google Drive metadata.",
                )
            )

        service_email = drive_service.get_service_account_email()

        if service_email:
            st.caption(
                f"Service account: `{service_email}`"
            )

    st.markdown(
        "<hr style='margin:1rem 0;'>",
        unsafe_allow_html=True,
    )

    # ──────────────────────────────────────────────────────────────────────
    # Actions
    # ──────────────────────────────────────────────────────────────────────

    col1, col2 = st.columns([2, 1])

    with col1:
        st.caption(
            "Sync new or changed Drive documents into the library."
        )

    with col2:
        if st.button(
            "Rebuild library index",
            width="stretch",
            key="library_rebuild_index",
        ):
            ok = analysis_service.rebuild_library_index()

            if ok:
                st.success("Library index rebuilt.")
            else:
                st.warning(
                    "No embedded chunks found to index."
                )

    sync_disabled = auth_state.status != "ready"

    if st.button(
        "Sync Drive and process changes",
        type="primary",
        disabled=sync_disabled,
        key="library_drive_sync",
        help=(
            "Configure Drive authentication first."
            if sync_disabled
            else None
        ),
    ):
        progress = st.empty()

        def on_file_found(
            name: str,
            idx: int,
            total: int,
        ) -> None:
            progress.info(
                f"Checking file {idx}/{total}: {name}"
            )

        def on_job_progress(
            step: str,
            pct: int,
        ) -> None:
            progress.info(
                f"[Ingestion {pct}%] {step}"
            )

        with st.spinner("Syncing Google Drive..."):
            result = analysis_service.sync_drive(
                on_file_found=on_file_found,
            )

        if result.get("error"):
            progress.empty()

            st.error(result["error"])

            auth_url = result.get("authorization_url")

            if auth_url:
                st.link_button(
                    "Authenticate with Google",
                    auth_url,
                    use_container_width=True,
                )

            return

        jobs = analysis_service.process_pending_ingestion_jobs(
            limit=100,
            on_progress=on_job_progress,
        )

        progress.info("Rebuilding semantic index...")

        analysis_service.rebuild_library_index()

        progress.empty()

        if result.get("total", 0) == 0:
            st.warning(
                "No supported files were discovered in the "
                "Google Drive folder."
            )

        st.success(
            f"Sync complete: "
            f"{result.get('new', 0)} new, "
            f"{result.get('updated', 0)} updated, "
            f"{result.get('skipped', 0)} skipped, "
            f"{result.get('failed', 0)} failed."
        )

        st.info(
            f"Ingestion jobs: "
            f"{jobs.get('succeeded', 0)} succeeded, "
            f"{jobs.get('failed', 0)} failed."
        )

    # ──────────────────────────────────────────────────────────────────────
    # Recent sync runs
    # ──────────────────────────────────────────────────────────────────────

    runs = analysis_service.get_recent_sync_runs(limit=10)

    if runs:
        st.markdown("### Recent sync runs")

        st.dataframe(
            [
                {
                    "Started": (
                        r.started_at.isoformat()
                        if r.started_at
                        else ""
                    ),
                    "Status": r.status,
                    "Total": r.total_files,
                    "New": r.new_files,
                    "Skipped": r.skipped_files,
                    "Failed": r.failed_files,
                    "Error": r.error_message or "",
                }
                for r in runs
            ],
            width="stretch",
            hide_index=True,
        )


# =============================================================================
# SEARCH
# =============================================================================


def _render_search() -> None:
    docs = analysis_service.list_documents()

    doc_options = ["All documents"] + [
        doc["filename"]
        for doc in docs
    ]

    doc_map = {
        doc["filename"]: doc["doc_id"]
        for doc in docs
    }

    query = st.text_input(
        "Library query",
        placeholder="Search across the whole research library...",
        key="library_search_q",
    )

    c1, c2, c3 = st.columns([3, 2, 2])

    with c1:
        selected_doc = st.selectbox(
            "Document filter",
            doc_options,
            key="library_search_doc",
        )

    with c2:
        section_opt = st.selectbox(
            "Section filter",
            SECTION_OPTIONS,
            index=0,
            key="library_search_section",
        )

    with c3:
        top_k = st.selectbox(
            "Top k",
            [3, 5, 10, 15],
            index=1,
            key="library_search_k",
        )

    author = st.text_input(
        "Author filter",
        key="library_search_author",
    )

    year = st.text_input(
        "Year filter",
        key="library_search_year",
    )

    if st.button(
        "Search library",
        type="primary",
        key="library_search_btn",
    ):
        if not query.strip():
            st.warning(
                "Enter a query to search the library."
            )
            return

        doc_id_filter = (
            [doc_map[selected_doc]]
            if selected_doc != "All documents"
            else None
        )

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
            st.info(
                "No matching results found. "
                "Try broader keywords."
            )
            return

        st.caption(
            f"{results.total_found} result(s) "
            f"in {results.search_time_ms:.0f}ms"
        )

        doc_labels = {
            doc["doc_id"]: doc["filename"]
            for doc in docs
        }

        for r in results.results:
            pct = int(r.score * 100)

            title = html.escape(
                doc_labels.get(
                    r.chunk.doc_id,
                    r.chunk.doc_id,
                )
            )

            text = html.escape(
                r.chunk.content[:500]
            )

            st.markdown(
                f"""
                <div class="result-card">
                    <div style="
                        display:flex;
                        justify-content:space-between;
                        align-items:center;
                        margin-bottom:0.4rem;
                    ">
                        <div>
                            <span class="result-section-tag">
                                {r.chunk.section_type.value}
                            </span>

                            <span style="
                                margin-left:0.5rem;
                                color:var(--muted);
                            ">
                                {title}
                            </span>
                        </div>

                        <span class="result-score">
                            {pct}% match
                        </span>
                    </div>

                    <div style="
                        font-family:var(--f-mono);
                        font-size:0.68rem;
                        color:var(--muted);
                        margin-bottom:0.35rem;
                    ">
                        Source: {r.chunk.doc_id}
                        · page {r.chunk.page_number}
                        · chunk {r.chunk.chunk_index + 1}
                    </div>

                    <div class="result-text">
                        {text}
                        {'...' if len(r.chunk.content) > 500 else ''}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )


# =============================================================================
# CHAT
# =============================================================================


def _render_library_chat() -> None:
    docs = analysis_service.list_documents()

    doc_options = ["All documents"] + [
        doc["filename"]
        for doc in docs
    ]

    doc_map = {
        doc["filename"]: doc["doc_id"]
        for doc in docs
    }

    selected_doc = st.selectbox(
        "Scope",
        doc_options,
        key="library_chat_doc",
    )

    top_k = st.selectbox(
        "Context chunks",
        [5, 8, 12],
        index=1,
        key="library_chat_k",
    )

    question = st.text_input(
        "Ask the library",
        placeholder=(
            "Compare findings across synced research papers..."
        ),
        key="library_chat_q",
    )

    if "library_chat_history" not in st.session_state:
        st.session_state.library_chat_history = []

    if st.button(
        "Ask",
        type="primary",
        key="library_chat_btn",
    ):
        if not question.strip():
            st.warning("Enter a question.")
            return

        doc_ids = (
            [doc_map[selected_doc]]
            if selected_doc != "All documents"
            else None
        )

        history = st.session_state.library_chat_history

        history.append(
            ChatMessage(
                role=MessageRole.USER,
                content=question.strip(),
            )
        )

        reply = ""

        placeholder = st.empty()

        with st.spinner(
            "Reading across the library..."
        ):
            for token in analysis_service.library_chat_stream(
                question=question.strip(),
                history=history[:-1],
                top_k=top_k,
                doc_ids=doc_ids,
            ):
                reply += str(token)

                placeholder.markdown(reply)

        history.append(
            ChatMessage(
                role=MessageRole.ASSISTANT,
                content=reply or "No response.",
            )
        )

    st.markdown("### Conversation")

    for msg in st.session_state.library_chat_history[-8:]:
        role = (
            "You"
            if msg.role == MessageRole.USER
            else "Library"
        )

        st.markdown(
            f"**{role}:** {html.escape(msg.content)}"
        )


# =============================================================================
# EXPORTS
# =============================================================================


def _render_export_readiness() -> None:
    docs = analysis_service.list_documents()

    ready = [
        doc
        for doc in docs
        if doc.get("status") == "ready"
    ]

    st.metric(
        "Export-ready documents",
        len(ready),
    )

    st.caption(
        "Use Export mode to generate XLSX, DOCX, CSV, "
        "or integration-ready JSON."
    )

    if ready:
        st.dataframe(
            [
                {
                    "Filename": doc.get("filename"),
                    "Title": doc.get("title"),
                    "Authors": ", ".join(
                        doc.get("authors") or []
                    ),
                    "Pages": doc.get("pages"),
                    "Chunks": doc.get("chunks"),
                }
                for doc in ready
            ],
            width="stretch",
            hide_index=True,
        )


# =============================================================================
# LOGS
# =============================================================================


def _render_logs() -> None:
    rows = analysis_service.get_recent_logs(limit=100)

    if not rows:
        st.info("No processing logs yet.")
        return

    st.dataframe(
        rows,
        width="stretch",
        hide_index=True,
    )


# =============================================================================
# HELPERS
# =============================================================================


def _section_type(
    value: str,
) -> SectionType | None:
    if value == "Any":
        return None

    try:
        return SectionType(value)

    except ValueError:
        return None