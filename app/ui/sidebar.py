"""
sidebar.py - Sidebar UI for PDF Research Analyzer.

Responsibilities:
- App branding / logo block
- File uploader (single file → triggers handle_upload)
- Document selector (list all docs, set active_doc_id)
- Google Drive folder configuration (runtime input + sync button)
- Cache management (clear active doc, delete all)
- Provider / API status display

All heavy logic lives in shared.py or the service layer.
"""

from __future__ import annotations

import os

import streamlit as st

from app.services.analysis_service import analysis_service
from app.ui.shared import delete_all_docs, delete_doc_cache, handle_upload
from app.utils.logger import get_logger

logger = get_logger(__name__)


def render_sidebar() -> None:
    """Render the full application sidebar."""

    # ── Branding ──────────────────────────────────────────────────────────────
    st.sidebar.markdown(
        """
        <div class="sidebar-logo">
            <div class="sidebar-logo-mark">📄</div>
            <div class="sidebar-app-name">PDF Research</div>
            <div class="sidebar-app-sub">Analyzer · AI Powered</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Upload ────────────────────────────────────────────────────────────────
    st.sidebar.markdown(
        '<span class="sb-label">Upload</span>',
        unsafe_allow_html=True,
    )
    uploaded_file = st.sidebar.file_uploader(
        "Upload a file",
        type=["pdf", "docx", "doc", "txt", "xlsx", "xls", "csv"],
        help="PDF · DOCX · TXT · XLSX · CSV — up to 50 MB",
        label_visibility="collapsed",
        key="sidebar_uploader",
    )
    if uploaded_file is not None:
        # Guard: only process once per unique file name to avoid re-running on
        # every Streamlit rerun while the same file is still in the uploader.
        if uploaded_file.name != st.session_state.get("last_uploaded_name"):
            st.session_state.last_uploaded_name = uploaded_file.name
            handle_upload(uploaded_file)

    # ── Document selector ─────────────────────────────────────────────────────
    docs = analysis_service.list_documents()
    if docs:
        st.sidebar.markdown(
            '<span class="sb-label" style="margin-top:1rem;">Documents</span>',
            unsafe_allow_html=True,
        )

        doc_names = [doc["filename"] for doc in docs]
        active_id = st.session_state.get("active_doc_id")

        # Resolve current selectbox index from active_doc_id
        try:
            current_idx = next(
                i for i, d in enumerate(docs) if d["doc_id"] == active_id
            )
        except StopIteration:
            current_idx = 0

        selected_name = st.sidebar.selectbox(
            "Select document",
            options=doc_names,
            index=current_idx,
            label_visibility="collapsed",
            key="sidebar_doc_select",
        )

        # Only update active_doc_id when the user explicitly changes selection
        selected_id = next(
            (d["doc_id"] for d in docs if d["filename"] == selected_name), None
        )
        if selected_id and selected_id != active_id:
            st.session_state.active_doc_id = selected_id
            st.session_state.chat_history = []   # clear chat when switching docs
            st.rerun()

        # Status badge for selected doc
        selected_doc = next(
            (d for d in docs if d["filename"] == selected_name), None
        )
        if selected_doc:
            status = selected_doc.get("status", "")
            badge_color = {
                "ready":      "var(--success)",
                "failed":     "var(--accent)",
                "extracting": "var(--warn)",
                "embedding":  "var(--warn)",
                "uploaded":   "var(--muted)",
            }.get(status, "var(--muted)")
            st.sidebar.markdown(
                f'<div style="font-family:var(--f-mono);font-size:0.62rem;'
                f'color:{badge_color};margin:0.2rem 0 0.6rem 0.1rem;">'
                f'● {status or "unknown"}</div>',
                unsafe_allow_html=True,
            )

    # ── Google Drive ──────────────────────────────────────────────────────────
    from app.ui.drive_tab import render_drive_sidebar
    render_drive_sidebar()

    # ── Cache management ──────────────────────────────────────────────────────
    st.sidebar.markdown("<hr>", unsafe_allow_html=True)
    st.sidebar.markdown(
        '<span class="sb-label">Manage</span>',
        unsafe_allow_html=True,
    )

    active_doc = st.session_state.get("active_doc_id")
    col_a, col_b = st.sidebar.columns(2)

    with col_a:
        if st.button(
            "Clear doc",
            disabled=not active_doc,
            width="stretch",
            key="sidebar_clear_cache",
            help="Remove embeddings and index for the active document",
        ):
            delete_doc_cache(active_doc)
            st.session_state.chat_history = []
            st.sidebar.success("Cache cleared.")
            st.rerun()

    with col_b:
        if st.button(
            "Delete all",
            width="stretch",
            key="sidebar_delete_all",
            type="primary",
            help="Remove ALL documents and cached data — cannot be undone",
        ):
            st.session_state["_confirm_delete_all"] = True

    # Confirmation step to prevent accidental mass deletion
    if st.session_state.get("_confirm_delete_all"):
        st.sidebar.warning("This will delete **all** documents. Are you sure?")
        c1, c2 = st.sidebar.columns(2)
        with c1:
            if st.button("Yes, delete", type="primary", width="stretch", key="confirm_yes"):
                delete_all_docs()
                st.session_state.active_doc_id = None
                st.session_state.chat_history = []
                st.session_state.library_chat_history = []
                st.session_state["_confirm_delete_all"] = False
                st.rerun()
        with c2:
            if st.button("Cancel", width="stretch", key="confirm_no"):
                st.session_state["_confirm_delete_all"] = False
                st.rerun()

    # ── Provider / API status ─────────────────────────────────────────────────
    st.sidebar.markdown("<hr>", unsafe_allow_html=True)
    st.sidebar.markdown(
        '<span class="sb-label">AI Providers</span>',
        unsafe_allow_html=True,
    )

    try:
        provider_status = analysis_service.get_provider_status()
    except Exception:
        provider_status = {}

    for provider_name, info in provider_status.items():
        configured = info.get("configured", False)
        model = info.get("model", "—")
        dot_cls = "provider-dot-on" if configured else "provider-dot-off"
        st.sidebar.markdown(
            f'<div class="provider-row">'
            f'  <span class="provider-name">'
            f'    <span class="{dot_cls}">●</span> {provider_name}'
            f'  </span>'
            f'  <span class="provider-model">{model[:22]}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    if not provider_status:
        st.sidebar.caption(
            "No AI providers configured. Set OPENROUTER_API_KEY or HUGGINGFACE_API_KEY."
        )

    # ── App version footer ────────────────────────────────────────────────────
    try:
        from app.config import APP_VERSION
        st.sidebar.markdown(
            f'<div style="font-family:var(--f-mono);font-size:0.55rem;'
            f'color:var(--sb-muted);margin-top:1.5rem;text-align:center;'
            f'letter-spacing:0.08em;">v{APP_VERSION}</div>',
            unsafe_allow_html=True,
        )
    except Exception:
        pass
