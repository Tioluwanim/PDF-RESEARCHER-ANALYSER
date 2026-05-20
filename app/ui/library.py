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
        use_container_width=True,
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
    """
    Complete Google Drive setup wizard + sync UI.

    Layout:
      Step 1 — Paste Drive folder URL (always visible)
      Step 2 — Auth status with contextual help
               a) Service account: show email to share with + status
               b) OAuth: Sign-in button + auto-detect ?code= redirect
      Step 3 — Sync button (enabled only when ready)
      Step 4 — Recent sync history
    """
    import os as _os
    from pathlib import Path as _Path

    auth_state = drive_service.get_auth_state()

    # ── Auto-handle OAuth ?code= redirect ────────────────────────────────────
    try:
        params = st.query_params
        oauth_code = params.get("code", "")
        if oauth_code:
            # Use the exact redirect URI stored when the auth URL was built.
            # Google requires this to match byte-for-byte.
            stored_redirect = st.session_state.get(
                "_oauth_redirect_uri",
                drive_service._get_oauth_redirect_uri(),
            )
            with st.spinner("Completing Google authentication…"):
                result = drive_service.exchange_authorization_code(
                    str(oauth_code),
                    redirect_uri=stored_redirect,
                )
            if result.get("success"):
                st.query_params.clear()
                st.session_state.pop("_oauth_redirect_uri", None)
                st.success("✓ Google account connected. You can now sync Drive.")
                st.rerun()
            else:
                st.error(f"OAuth failed: {result.get('error', 'Unknown error')}")
    except Exception:
        pass  # query_params not available in all contexts

    st.markdown("### Google Drive Sync")
    st.caption(
        "Connect your library's Google Drive folder so documents are synced automatically."
    )

    # ── Step 1: Folder URL ────────────────────────────────────────────────────
    st.markdown("#### Step 1 — Your Drive folder")

    current_folder = drive_service.folder_id or ""
    col_url, col_save = st.columns([5, 1])
    with col_url:
        folder_input = st.text_input(
            "Google Drive folder URL",
            value=current_folder,
            placeholder="https://drive.google.com/drive/folders/1BxiMVs0XRA5…",
            help="Paste the URL of your shared Drive folder. Works with folder URLs, file URLs, or raw IDs.",
            key="drive_folder_url_input",
        )
    with col_save:
        st.markdown("<div style='padding-top:1.75rem;'>", unsafe_allow_html=True)
        if st.button("Save", use_container_width=True, key="drive_folder_save"):
            if not folder_input.strip():
                st.warning("Paste a Google Drive folder URL first.")
            else:
                drive_service.set_folder_id(folder_input.strip())
                parsed = drive_service.folder_id
                if parsed:
                    _os.environ["GOOGLE_DRIVE_FOLDER_ID"] = parsed
                    st.success(f"✓ Folder saved: `{parsed}`")
                    st.rerun()
                else:
                    st.error(
                        "Could not parse a folder ID from that URL. "
                        "Example: `https://drive.google.com/drive/folders/1BxiMVs0XRA5…`"
                    )
        st.markdown("</div>", unsafe_allow_html=True)

    if current_folder:
        st.caption(f"Active folder ID: `{current_folder}`")

    st.divider()

    # ── Step 2: Authentication ────────────────────────────────────────────────
    st.markdown("#### Step 2 — Authentication")

    # Re-read auth state after possible folder save
    auth_state = drive_service.get_auth_state()

    if auth_state.status == "missing_folder":
        st.info("📂 Save your Drive folder URL above to continue.")

    elif auth_state.status == "missing_credentials":
        st.warning("No Google credentials found. Choose one of the two options below.", icon="🔑")

        tab_sa, tab_oauth = st.tabs(["Option A — Service Account", "Option B — OAuth (Sign in with Google)"])

        with tab_sa:
            st.markdown(
                """
**Service Account** is the simplest option for a shared library folder.

**Setup steps:**
1. Go to [Google Cloud Console](https://console.cloud.google.com) → **APIs & Services** → **Enable APIs** → enable **Google Drive API**
2. **IAM & Admin** → **Service Accounts** → **Create Service Account**
3. Click the service account → **Keys** → **Add Key** → **JSON** → download file
4. Add the JSON to Streamlit secrets as:

```toml
[gcp_service_account]
type = "service_account"
project_id = "your-project"
private_key_id = "..."
private_key = "-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----\n"
client_email = "your-sa@your-project.iam.gserviceaccount.com"
client_id = "..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
```

5. Share your Drive folder with the `client_email` address (Viewer access).
                """.strip()
            )

        with tab_oauth:
            st.markdown(
                """
**OAuth** lets a Google user authorise the app directly.

**Setup steps:**
1. Go to [Google Cloud Console](https://console.cloud.google.com) → **APIs & Services** → **Credentials**
2. **Create Credentials** → **OAuth Client ID** → **Web application**
3. Add your app URL to **Authorised redirect URIs**
4. Download the client JSON and add it to Streamlit secrets as:

```toml
[google_oauth_client]
type = "authorized_user"   # or leave out
client_id = "….apps.googleusercontent.com"
client_secret = "GOCSPX-…"
redirect_uris = ["https://your-app.streamlit.app/"]
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
```

5. Set `GOOGLE_OAUTH_REDIRECT_URI` in secrets to your app URL.
                """.strip()
            )

    elif auth_state.status == "oauth_login_required":
        st.warning("Google OAuth is configured — sign in to authorise access.", icon="🔐")

        auth_info = drive_service.get_authorization_url()
        auth_url  = auth_info.get("authorization_url")

        # Show the exact redirect URI being used so the user can add it to GCP
        redirect_uri = drive_service._get_oauth_redirect_uri()
        st.info(
            f"**Before clicking Sign in**, make sure this exact URL is in your "
            f"Google Cloud Console → Credentials → OAuth Client → "
            f"**Authorised redirect URIs**:\n\n`{redirect_uri}`",
            icon="⚠️",
        )

        if auth_url:
            # Store the redirect URI so exchange can use the exact same value
            st.session_state["_oauth_redirect_uri"] = redirect_uri
            st.link_button("🔐 Sign in with Google", auth_url, use_container_width=True)

            st.markdown("---")
            st.caption(
                "After approving, Google redirects back to this page and login "
                "completes automatically. If it doesn't, paste the `code=` value below."
            )
            manual_code = st.text_input(
                "Authorization code (manual fallback)",
                type="password",
                key="google_oauth_code_manual",
                placeholder="4/0AY0e-g7…",
            )
            if st.button("Complete login manually", key="oauth_manual_btn"):
                if manual_code.strip():
                    with st.spinner("Exchanging authorization code…"):
                        result = drive_service.exchange_authorization_code(manual_code.strip())
                    if result.get("success"):
                        st.success("✓ Google Drive authenticated.")
                        st.rerun()
                    else:
                        st.error(f"Failed: {result.get('error')}")
                else:
                    st.warning("Paste the authorization code first.")
        else:
            st.error(auth_info.get("error", "Could not generate authorization URL."))

    elif auth_state.status == "ready":
        mode_label = {
            "oauth_user":       "OAuth (signed-in user)",
            "service_account":  "Service Account",
        }.get(auth_state.auth_mode or "", auth_state.auth_mode or "unknown")

        st.success(f"✓ Authenticated via {mode_label}", icon="✅")

        # Show folder name
        folder_info = drive_service.get_folder_info()
        if folder_info and not folder_info.get("error"):
            mime = folder_info.get("mimeType", "")
            icon = "📁" if "folder" in mime else "📄"
            st.caption(f"{icon} {folder_info.get('name', 'Unknown')}")
        elif folder_info.get("error"):
            st.warning(folder_info["error"])

        # Show service account email as sharing reminder
        sa_email = drive_service.get_service_account_email()
        if sa_email:
            st.info(
                f"Make sure your Drive folder is shared with: `{sa_email}` (Viewer)",
                icon="📧",
            )

    st.divider()

    # ── Step 3: Sync actions ──────────────────────────────────────────────────
    st.markdown("#### Step 3 — Sync")

    sync_ready = auth_state.status == "ready"

    col_sync, col_rebuild = st.columns([3, 1])
    with col_rebuild:
        if st.button("Rebuild index", use_container_width=True, key="library_rebuild_index"):
            ok = analysis_service.rebuild_library_index()
            st.success("Library index rebuilt.") if ok else st.warning("No embedded chunks found.")

    with col_sync:
        if st.button(
            "🔄 Sync Drive and process new files",
            type="primary",
            use_container_width=True,
            disabled=not sync_ready,
            key="library_drive_sync",
            help="Complete Steps 1 & 2 first." if not sync_ready else "Sync new or changed PDFs from Drive.",
        ):
            progress = st.empty()

            def _on_file_found(name: str, idx: int, total: int) -> None:
                progress.info(f"Checking {idx}/{total}: {name}")

            def _on_job_progress(step: str, pct: int) -> None:
                progress.info(f"Processing {pct}%: {step}")

            with st.spinner("Syncing Google Drive…"):
                result = analysis_service.sync_drive(on_file_found=_on_file_found)

            if result.get("error"):
                progress.empty()
                st.error(result["error"])
                if result.get("authorization_url"):
                    st.link_button("Authenticate with Google", result["authorization_url"], use_container_width=True)
            else:
                jobs = analysis_service.process_pending_ingestion_jobs(
                    limit=100, on_progress=_on_job_progress
                )
                progress.info("Rebuilding semantic index…")
                analysis_service.rebuild_library_index()
                progress.empty()

                if result.get("total", 0) == 0:
                    st.warning("No supported files found in the Drive folder.")
                else:
                    st.success(
                        f"✓ Sync complete — "
                        f"{result.get('new', 0)} new · "
                        f"{result.get('updated', 0)} updated · "
                        f"{result.get('skipped', 0)} unchanged · "
                        f"{result.get('failed', 0)} failed"
                    )
                if jobs.get("succeeded", 0) or jobs.get("failed", 0):
                    st.info(
                        f"Processing: {jobs.get('succeeded', 0)} succeeded · "
                        f"{jobs.get('failed', 0)} failed"
                    )

    # ── Step 4: Sync history ──────────────────────────────────────────────────
    runs = analysis_service.get_recent_sync_runs(limit=10)
    if runs:
        st.markdown("#### Recent sync runs")
        st.dataframe(
            [
                {
                    "Started":  r.started_at.isoformat() if r.started_at else "",
                    "Status":   r.status,
                    "Total":    r.total_files,
                    "New":      r.new_files,
                    "Updated":  getattr(r, "updated_files", 0),
                    "Skipped":  r.skipped_files,
                    "Failed":   r.failed_files,
                    "Error":    r.error_message or "",
                }
                for r in runs
            ],
            use_container_width=True,
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
            use_container_width=True,
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
        use_container_width=True,
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