from __future__ import annotations

import streamlit as st
from app.services.analysis_service import analysis_service
from app.services.drive_service import drive_service
from app.ui.shared import delete_all_docs, delete_doc_cache, handle_upload


def render_sidebar() -> None:
    st.sidebar.title("Library Explorer")
    st.sidebar.markdown(
        "Use the sidebar to manage uploads, sync with Drive, and switch between documents."
    )

    uploaded_file = st.sidebar.file_uploader(
        "Upload a PDF",
        type=["pdf", "docx", "doc", "txt", "xlsx", "xls", "csv"],
        label_visibility="collapsed",
        key="sidebar_uploader",
    )
    if uploaded_file:
        handle_upload(uploaded_file)

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Drive sync**")
    if drive_service.is_configured:
        st.sidebar.caption(f"Folder: {drive_service.folder_id}")
        if st.sidebar.button("Sync Google Drive", type="primary", key="sidebar_drive_sync"):
            with st.sidebar.status("Syncing Drive folder...", expanded=True) as status:
                result = analysis_service.sync_drive(
                    on_file_found=lambda name, idx, total: st.write(f"{idx}/{total}: {name}"),
                )
                jobs = analysis_service.process_pending_ingestion_jobs(limit=50)
                status.update(label="Drive sync complete", state="complete")
            st.sidebar.success(
                f"{result.get('new', 0)} changed, {result.get('skipped', 0)} skipped, "
                f"{jobs.get('succeeded', 0)} processed."
            )
            st.rerun()
    else:
        st.sidebar.caption("Set GOOGLE_DRIVE_FOLDER_ID and service-account credentials to enable sync.")

    docs = analysis_service.list_documents()
    if docs:
        selected = st.sidebar.selectbox(
            "Select document",
            options=[doc["filename"] for doc in docs],
            index=0,
            key="sidebar_doc_select",
        )
        selected_id = next((doc["doc_id"] for doc in docs if doc["filename"] == selected), None)
        if selected_id:
            st.session_state.active_doc_id = selected_id

    if st.sidebar.button("Delete all documents", key="sidebar_delete_all"):
        delete_all_docs()
        st.session_state.active_doc_id = None
        st.session_state.document_info = {}
        st.rerun()

    if st.sidebar.button("Clear cache for active doc", key="sidebar_clear_cache"):
        active_doc = st.session_state.get("active_doc_id")
        if active_doc:
            delete_doc_cache(active_doc)
            st.success("Cache cleared.")

    provider_status = analysis_service.get_provider_status()
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Model / API status**")
    st.sidebar.write(provider_status)
