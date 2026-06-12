"""
main.py - PDF Research Analyzer

Thin Streamlit entry point:
- page config
- startup checks
- CSS injection
- session state init
- sidebar + mode routing
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st

from app.config import (
    STREAMLIT_LAYOUT,
    STREAMLIT_PAGE_ICON,
    STREAMLIT_PAGE_TITLE,
    MAX_CHAT_HISTORY,
    get_config_summary,
    validate_config,
)
from app.db.repository import init_db
from app.services.analysis_service import analysis_service
from app.ui import (
    render_batch_tab,
    render_chat_tab,
    render_doc_header,
    render_drive_tab,
    render_drive_sidebar,
    render_empty_state,
    render_export_tab,
    render_info_tab,
    render_library_search_tab,
    render_search_tab,
    render_sections_tab,
    render_sidebar,
)
from app.ui.styles import inject_global_css
from app.utils.logger import get_logger, log_startup

logger = get_logger(__name__)


st.set_page_config(
    page_title=STREAMLIT_PAGE_TITLE,
    page_icon=STREAMLIT_PAGE_ICON,
    layout=STREAMLIT_LAYOUT,
    initial_sidebar_state="expanded",
)


def _init_session() -> None:
    defaults = {
        "active_doc_id": None,
        "chat_history": [],
        "library_chat_history": [],
        "startup_done": False,
        "app_mode": "📄 Single PDF",
        "export_data": {},
        "batch_done": False,
        "last_uploaded_name": None,
        "_confirm_delete_all": False,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

    if "chat_history" in st.session_state and len(st.session_state.chat_history) > MAX_CHAT_HISTORY:
        st.session_state.chat_history = st.session_state.chat_history[-MAX_CHAT_HISTORY:]


def _run_startup() -> None:
    if st.session_state.startup_done:
        return

    warnings = validate_config()

    try:
        init_db()
    except Exception as exc:
        warnings.append(f"Database startup failed: {exc}")

    log_startup(get_config_summary(), warnings)
    st.session_state.startup_done = True

    for warning in warnings:
        st.warning(warning, icon="⚠️")


def _render_app_header() -> None:
    st.markdown(
        """
        <div class="app-header">
          <div class="app-title-block">
            <div class="app-title">PDF <span>Research</span> Analyzer</div>
            <div class="app-subtitle">
              Semantic Search · Section Detection · AI Chat · OCR · Multi-format Export
            </div>
          </div>
          <div class="app-badge">✦ AI Powered</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_main() -> None:
    _render_app_header()

    modes = ["📄 Single PDF", "🔎 Library Search", "📚 Batch Upload", "☁️ Drive Sync", "📤 Export"]
    current_mode = st.session_state.get("app_mode", "📄 Single PDF")

    mode = st.radio(
        "mode",
        modes,
        index=modes.index(current_mode) if current_mode in modes else 0,
        horizontal=True,
        label_visibility="collapsed",
        key="mode_radio",
    )

    if mode != current_mode:
        st.session_state.app_mode = mode
        st.session_state.export_data = {}

    st.markdown("<hr>", unsafe_allow_html=True)

    if mode == "🔎 Library Search":
        render_library_search_tab()
        return

    if mode == "📚 Batch Upload":
        render_batch_tab()
        return

    if mode == "☁️ Drive Sync":
        render_drive_tab()
        return

    if mode == "📤 Export":
        render_export_tab()
        return

    doc_id = st.session_state.get("active_doc_id")
    if not doc_id:
        render_empty_state()
        return

    info = analysis_service.get_document_info(doc_id)
    if isinstance(info, dict) and "error" in info:
        st.error(info["error"])
        st.session_state.active_doc_id = None
        return

    render_doc_header(info, info.get("chunks", {}))
    st.markdown("<hr>", unsafe_allow_html=True)

    tab_chat, tab_sections, tab_search, tab_info = st.tabs(
        ["💬 Chat", "📑 Sections", "🔍 Search", "ℹ️ Info"]
    )

    with tab_chat:
        render_chat_tab(doc_id, info)

    with tab_sections:
        render_sections_tab(doc_id, info)

    with tab_search:
        render_search_tab(doc_id)

    with tab_info:
        render_info_tab(info)


def main() -> None:
    _init_session()
    _run_startup()
    inject_global_css()
    render_sidebar()
    _render_main()


if __name__ == "__main__":
    main()
