"""
main.py - Streamlit UI for PDF Research Analyzer
Clean, production-ready interface with sidebar, chat, sections, and search.
"""

import streamlit as st
from pathlib import Path

from app.config import (
    STREAMLIT_PAGE_TITLE,
    STREAMLIT_PAGE_ICON,
    STREAMLIT_LAYOUT,
    MAX_CHAT_HISTORY,
    validate_config,
    get_config_summary,
)
from app.models.schemas import (
    ChatMessage,
    MessageRole,
    SectionType,
    DocumentStatus,
)
from app.services.analysis_service import analysis_service
from app.utils.logger import get_logger, log_startup

logger = get_logger(__name__)


# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title = STREAMLIT_PAGE_TITLE,
    page_icon  = STREAMLIT_PAGE_ICON,
    layout     = STREAMLIT_LAYOUT,
    initial_sidebar_state = "expanded",
)


# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600&display=swap');

    :root {
        --ink:       #0f0e0d;
        --paper:     #faf8f4;
        --accent:    #c8401a;
        --muted:     #8a857d;
        --border:    #e2ddd6;
        --surface:   #f2ede6;
        --success:   #2d6a4f;
        --warning:   #e07c3a;
        --code-bg:   #1e1c1a;
    }

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
        background-color: var(--paper);
        color: var(--ink);
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: var(--ink) !important;
        border-right: none;
    }
    section[data-testid="stSidebar"] * {
        color: #e8e2d9 !important;
    }
    section[data-testid="stSidebar"] .stButton > button {
        background: transparent;
        border: 1px solid #3a3632;
        color: #e8e2d9 !important;
        border-radius: 4px;
        font-family: 'DM Mono', monospace;
        font-size: 0.75rem;
        padding: 0.35rem 0.75rem;
        transition: all 0.2s ease;
        width: 100%;
        text-align: left;
    }
    section[data-testid="stSidebar"] .stButton > button:hover {
        background: #2a2622;
        border-color: var(--accent);
    }

    /* Main title */
    .main-title {
        font-family: 'DM Serif Display', serif;
        font-size: 2.6rem;
        color: var(--ink);
        letter-spacing: -0.02em;
        line-height: 1.1;
        margin-bottom: 0.15rem;
    }
    .main-subtitle {
        font-family: 'DM Mono', monospace;
        font-size: 0.78rem;
        color: var(--muted);
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 2rem;
    }

    /* Status pills */
    .status-pill {
        display: inline-block;
        padding: 0.2rem 0.65rem;
        border-radius: 20px;
        font-family: 'DM Mono', monospace;
        font-size: 0.7rem;
        font-weight: 500;
        letter-spacing: 0.04em;
        text-transform: uppercase;
    }
    .status-ready    { background: #d8f3dc; color: var(--success); }
    .status-failed   { background: #fde8e2; color: var(--accent); }
    .status-pending  { background: #fff3e0; color: var(--warning); }
    .status-default  { background: var(--surface); color: var(--muted); }

    /* Chat messages */
    .chat-user {
        background: var(--ink);
        color: #f0ece4;
        padding: 1rem 1.25rem;
        border-radius: 16px 16px 4px 16px;
        margin: 0.75rem 0 0.75rem 3rem;
        font-size: 0.92rem;
        line-height: 1.6;
    }
    .chat-assistant {
        background: var(--surface);
        color: var(--ink);
        padding: 1rem 1.25rem;
        border-radius: 4px 16px 16px 16px;
        margin: 0.75rem 3rem 0.75rem 0;
        font-size: 0.92rem;
        line-height: 1.6;
        border-left: 3px solid var(--accent);
    }

    /* Source cards */
    .source-card {
        background: var(--paper);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 0.75rem 1rem;
        margin: 0.4rem 0;
        font-size: 0.82rem;
    }
    .source-section-tag {
        font-family: 'DM Mono', monospace;
        font-size: 0.68rem;
        color: var(--accent);
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }

    /* Section viewer */
    .section-block {
        background: var(--surface);
        border-radius: 8px;
        padding: 1.25rem 1.5rem;
        border-left: 4px solid var(--accent);
        font-size: 0.9rem;
        line-height: 1.75;
        white-space: pre-wrap;
    }

    /* Metadata grid */
    .meta-item {
        background: var(--surface);
        border-radius: 8px;
        padding: 0.85rem 1rem;
        border: 1px solid var(--border);
    }
    .meta-label {
        font-family: 'DM Mono', monospace;
        font-size: 0.68rem;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin-bottom: 0.25rem;
    }
    .meta-value {
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--ink);
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab"] {
        font-family: 'DM Mono', monospace;
        font-size: 0.78rem;
        letter-spacing: 0.04em;
        text-transform: uppercase;
    }
    .stTabs [aria-selected="true"] {
        color: var(--accent) !important;
        border-bottom-color: var(--accent) !important;
    }

    /* Input fields */
    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea {
        border: 1.5px solid var(--border);
        border-radius: 8px;
        font-family: 'DM Sans', sans-serif;
        background: var(--paper);
    }
    .stTextInput > div > div > input:focus,
    .stTextArea > div > div > textarea:focus {
        border-color: var(--accent);
        box-shadow: 0 0 0 2px rgba(200,64,26,0.1);
    }

    /* Primary button */
    .stButton > button[kind="primary"] {
        background: var(--accent);
        color: white;
        border: none;
        border-radius: 8px;
        font-family: 'DM Sans', sans-serif;
        font-weight: 600;
        padding: 0.6rem 1.5rem;
        transition: all 0.2s ease;
    }
    .stButton > button[kind="primary"]:hover {
        background: #a8320f;
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(200,64,26,0.3);
    }

    /* Dividers */
    hr { border-color: var(--border); margin: 1.5rem 0; }

    /* Progress bar */
    .stProgress > div > div { background-color: var(--accent) !important; }

    /* Uploader */
    .stFileUploader {
        border: 2px dashed var(--border);
        border-radius: 12px;
        background: var(--paper);
        transition: border-color 0.2s;
    }
    .stFileUploader:hover { border-color: var(--accent); }

    /* Scrollbar */
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

    /* Hide streamlit branding but keep sidebar toggle */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    header { visibility: hidden; }

    /* Always show the sidebar collapse/expand button */
    [data-testid="collapsedControl"] {
        display: block !important;
        visibility: visible !important;
        opacity: 1 !important;
        color: var(--accent) !important;
    }
    button[kind="headerNoPadding"] {
        display: block !important;
        visibility: visible !important;
    }
</style>
""", unsafe_allow_html=True)


# ── Session State Init ────────────────────────────────────────────────────────
def init_session():
    defaults = {
        "active_doc_id"     : None,
        "chat_history"      : [],     # list[ChatMessage]
        "processing"        : False,
        "startup_done"      : False,
        "last_uploaded_name": None,   # prevents re-processing on every rerun
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


# ── Startup ───────────────────────────────────────────────────────────────────
def run_startup():
    if not st.session_state.startup_done:
        warnings = validate_config()
        log_startup(get_config_summary(), warnings)
        st.session_state.startup_done = True
        if warnings:
            for w in warnings:
                st.warning(f"⚠️ {w}", icon="⚠️")


# ── Helpers ───────────────────────────────────────────────────────────────────
def status_pill(status: str) -> str:
    cls_map = {
        "ready"     : "status-ready",
        "failed"    : "status-failed",
        "uploading" : "status-pending",
        "extracting": "status-pending",
        "embedding" : "status-pending",
        "extracted" : "status-pending",
    }
    cls = cls_map.get(status, "status-default")
    return f'<span class="status-pill {cls}">{status}</span>'


def fmt_number(n: int) -> str:
    return f"{n:,}"


# ── Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar():
    with st.sidebar:
        # Logo / Title
        st.markdown("""
        <div style="padding: 1rem 0 1.5rem;">
            <div style="font-family:'DM Serif Display',serif; font-size:1.4rem; color:#f0ece4;">
                📄 PDF Analyzer
            </div>
            <div style="font-family:'DM Mono',monospace; font-size:0.65rem;
                        color:#6b6560; letter-spacing:0.1em; text-transform:uppercase;
                        margin-top:0.2rem;">
                Research Assistant
            </div>
        </div>
        """, unsafe_allow_html=True)

        # ── Upload ────────────────────────────────────────────────────────────
        st.markdown('<div style="font-family:\'DM Mono\',monospace; font-size:0.7rem; color:#8a857d; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:0.5rem;">Upload PDF</div>', unsafe_allow_html=True)

        uploaded = st.file_uploader(
            label      = "Drop PDF here",
            type       = ["pdf"],
            label_visibility = "collapsed",
        )

        if uploaded:
            # Only process if this is a NEW file (not a rerun of the same upload)
            if uploaded.name != st.session_state.get("last_uploaded_name"):
                st.session_state.last_uploaded_name = uploaded.name
                _handle_upload(uploaded)

        st.markdown("<hr style='border-color:#2a2622; margin:1.25rem 0;'>", unsafe_allow_html=True)

        # ── Document List ─────────────────────────────────────────────────────
        st.markdown('<div style="font-family:\'DM Mono\',monospace; font-size:0.7rem; color:#8a857d; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:0.75rem;">Documents</div>', unsafe_allow_html=True)

        docs = analysis_service.list_documents()
        if not docs:
            st.markdown('<div style="font-size:0.8rem; color:#4a4642; font-style:italic;">No documents yet</div>', unsafe_allow_html=True)
        else:
            for doc in docs:
                is_active = doc["doc_id"] == st.session_state.active_doc_id
                icon      = "▶ " if is_active else "  "
                label     = f"{icon}{doc['filename'][:28]}"
                if st.button(label, key=f"doc_{doc['doc_id']}"):
                    st.session_state.active_doc_id = doc["doc_id"]
                    st.session_state.chat_history  = []
                    st.rerun()

        st.markdown("<hr style='border-color:#2a2622; margin:1.25rem 0;'>", unsafe_allow_html=True)

        # ── Provider Status ───────────────────────────────────────────────────
        st.markdown('<div style="font-family:\'DM Mono\',monospace; font-size:0.7rem; color:#8a857d; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:0.6rem;">LLM Providers</div>', unsafe_allow_html=True)

        providers = analysis_service.get_provider_status()
        for name, info in providers.items():
            dot   = "🟢" if info["configured"] else "🔴"
            label = info["model"].split("/")[-1][:22]
            st.markdown(
                f'<div style="font-family:\'DM Mono\',monospace; font-size:0.72rem; '
                f'color:#8a857d; margin:0.25rem 0;">'
                f'{dot} {name.upper()} <span style="color:#4a4642;">— {label}</span></div>',
                unsafe_allow_html=True,
            )


def _handle_upload(uploaded_file):
    """Handles file upload, processing, and state update."""
    file_bytes = uploaded_file.read()

    with st.spinner("Uploading ..."):
        doc, error = analysis_service.save_upload(
            file_bytes = file_bytes,
            filename   = uploaded_file.name,
        )

    if error:
        st.error(f"Upload failed: {error.detail}")
        return

    st.success(f"✓ {uploaded_file.name} uploaded")
    st.session_state.active_doc_id = doc.doc_id
    st.session_state.chat_history  = []

    # Auto-process
    _process_document(doc.doc_id)


def _process_document(doc_id: str):
    """Runs the full pipeline with a progress bar."""
    progress_bar  = st.progress(0, text="Starting ...")
    status_text   = st.empty()

    def on_progress(step: str, pct: int):
        progress_bar.progress(pct / 100, text=step)
        status_text.markdown(
            f'<div style="font-family:\'DM Mono\',monospace; font-size:0.75rem; '
            f'color:#8a857d;">{step}</div>',
            unsafe_allow_html=True,
        )

    result = analysis_service.process_document(
        doc_id      = doc_id,
        on_progress = on_progress,
    )

    progress_bar.empty()
    status_text.empty()

    if result.status == DocumentStatus.READY:
        st.success(
            f"✓ Ready — {result.page_count}p · "
            f"{fmt_number(result.word_count)} words · "
            f"{result.chunk_count} chunks · "
            f"{len(result.sections_found)} sections"
        )
        st.rerun()
    else:
        st.error(f"Processing failed: {result.message}")


# ── Main Content ──────────────────────────────────────────────────────────────
def render_main():
    # Header
    st.markdown("""
    <div class="main-title">PDF Research Analyzer</div>
    <div class="main-subtitle">Semantic search · Section detection · Chat with your paper</div>
    """, unsafe_allow_html=True)

    doc_id = st.session_state.active_doc_id

    if not doc_id:
        _render_empty_state()
        return

    # Load document info
    info = analysis_service.get_document_info(doc_id)
    if "error" in info:
        st.error(info["error"])
        return

    # Document header
    _render_doc_header(info)

    st.markdown("<hr>", unsafe_allow_html=True)

    # Tabs
    tab_chat, tab_sections, tab_search, tab_info = st.tabs([
        "💬  Chat",
        "📑  Sections",
        "🔍  Search",
        "ℹ️  Info",
    ])

    with tab_chat:
        _render_chat_tab(doc_id, info)

    with tab_sections:
        _render_sections_tab(doc_id, info)

    with tab_search:
        _render_search_tab(doc_id)

    with tab_info:
        _render_info_tab(info)


def _render_empty_state():
    st.markdown("""
    <div style="display:flex; flex-direction:column; align-items:center;
                justify-content:center; padding:5rem 2rem; text-align:center;">
        <div style="font-size:3.5rem; margin-bottom:1rem;">📄</div>
        <div style="font-family:'DM Serif Display',serif; font-size:1.6rem;
                    color:#0f0e0d; margin-bottom:0.5rem;">
            Upload a research paper to begin
        </div>
        <div style="font-size:0.9rem; color:#8a857d; max-width:400px; line-height:1.6;">
            Drop a PDF in the sidebar. The system will extract text,
            detect sections, and index it for semantic search and chat.
        </div>
    </div>
    """, unsafe_allow_html=True)


def _render_doc_header(info: dict):
    meta = info["metadata"]
    col1, col2, col3, col4 = st.columns([3, 1, 1, 1])

    with col1:
        title = meta.get("title") or info["filename"]
        st.markdown(
            f'<div style="font-family:\'DM Serif Display\',serif; font-size:1.4rem; '
            f'line-height:1.3; margin-bottom:0.3rem;">{title[:120]}</div>',
            unsafe_allow_html=True,
        )
        authors = meta.get("authors", [])
        if authors:
            st.markdown(
                f'<div style="font-size:0.82rem; color:#8a857d;">'
                f'{", ".join(authors[:4])}</div>',
                unsafe_allow_html=True,
            )

    with col2:
        st.markdown(
            f'<div class="meta-item"><div class="meta-label">Pages</div>'
            f'<div class="meta-value">{meta.get("pages", 0)}</div></div>',
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            f'<div class="meta-item"><div class="meta-label">Words</div>'
            f'<div class="meta-value">{fmt_number(meta.get("words", 0))}</div></div>',
            unsafe_allow_html=True,
        )
    with col4:
        chunks = info.get("chunks", {})
        st.markdown(
            f'<div class="meta-item"><div class="meta-label">Chunks</div>'
            f'<div class="meta-value">{chunks.get("total", 0)}</div></div>',
            unsafe_allow_html=True,
        )

# ── Chat Tab ──────────────────────────────────────────────────────────────────
def _render_chat_tab(doc_id: str, info: dict):
    status = info.get("status")

    if status != "ready":
        st.warning(
            f"Document status is **{status}**. "
            "Processing must complete before chatting.",
            icon="⏳",
        )
        if st.button("▶ Process Now", type="primary"):
            _process_document(doc_id)
        return

    # Render chat history
    history = st.session_state.chat_history
    for msg in history:
        if msg.role == MessageRole.USER:
            st.markdown(
                f'<div class="chat-user">{msg.content}</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<div class="chat-assistant">{msg.content}</div>',
                unsafe_allow_html=True,
            )

    # Chat input
    col_input, col_btn = st.columns([6, 1])
    with col_input:
        question = st.text_input(
            label="Ask a question",
            placeholder="What are the main findings of this paper?",
            label_visibility="collapsed",
            key="chat_input",
        )
    with col_btn:
        send = st.button("Send", type="primary", use_container_width=True)

    # Clear chat
    if history:
        if st.button("Clear conversation", key="clear_chat"):
            st.session_state.chat_history = []
            st.rerun()

    # ✅ FIXED: Only send when button is clicked
    if send and question and question.strip():
        _handle_chat(doc_id, question.strip())


# ── Chat Handler ──────────────────────────────────────────────────────────────
def _handle_chat(doc_id: str, question: str):
    """Runs streaming chat and updates session history safely."""

    try:
        # Add user message
        user_msg = ChatMessage(role=MessageRole.USER, content=question)
        st.session_state.chat_history.append(user_msg)

        # Trim history
        if len(st.session_state.chat_history) > MAX_CHAT_HISTORY:
            st.session_state.chat_history = st.session_state.chat_history[-MAX_CHAT_HISTORY:]

        # Show user message immediately
        st.markdown(
            f'<div class="chat-user">{question}</div>',
            unsafe_allow_html=True,
        )

        # Stream assistant response
        response_container = st.empty()
        full_response = ""

        stream = analysis_service.chat_stream(
            doc_id=doc_id,
            question=question,
            history=st.session_state.chat_history[:-1],
        )

        for token in stream:
            if token is None:
                continue

            full_response += str(token)

            response_container.markdown(
                f'<div class="chat-assistant">{full_response}▌</div>',
                unsafe_allow_html=True,
            )

        # Final render
        response_container.markdown(
            f'<div class="chat-assistant">{full_response}</div>',
            unsafe_allow_html=True,
        )

        # Save assistant message
        assistant_msg = ChatMessage(
            role=MessageRole.ASSISTANT,
            content=full_response or "⚠️ No response received.",
        )
        st.session_state.chat_history.append(assistant_msg)

    except Exception as e:
        logger.error(f"Chat failed: {e}")

        st.error("⚠️ AI providers failed. Please try again.")

        # Save fallback message
        st.session_state.chat_history.append(
            ChatMessage(
                role=MessageRole.ASSISTANT,
                content="⚠️ AI providers are currently unavailable.",
            )
        )

# ── Sections Tab ──────────────────────────────────────────────────────────────
def _render_sections_tab(doc_id: str, info: dict):
    sections = info.get("sections", [])

    if not sections:
        st.info("No sections detected in this document.")
        return

    section_map = {s["type"]: s for s in sections}
    section_order = [
        SectionType.ABSTRACT,
        SectionType.INTRODUCTION,
        SectionType.METHODS,
        SectionType.RESULTS,
        SectionType.DISCUSSION,
        SectionType.CONCLUSION,
        SectionType.REFERENCES,
        SectionType.OTHER,
    ]

    # Section selector
    available  = [s for s in section_order if s.value in section_map]
    labels     = [s.value.capitalize() for s in available]
    selected_i = st.selectbox(
        "Select section",
        range(len(labels)),
        format_func = lambda i: labels[i],
        label_visibility = "collapsed",
    )

    if selected_i is None:
        return

    selected_type = available[selected_i]
    sec_info      = section_map[selected_type.value]

    # Section stats
    col1, col2, col3 = st.columns(3)
    col1.metric("Section",    sec_info["type"].capitalize())
    col2.metric("Word Count", fmt_number(sec_info.get("word_count", 0)))
    col3.metric("Page Start", sec_info.get("page_start", 0) + 1)

    st.markdown("<br>", unsafe_allow_html=True)

    # Section content
    content = analysis_service.get_section_content(doc_id, selected_type)
    if content:
        st.markdown(
            f'<div class="section-block">{content[:4000]}'
            f'{"..." if len(content) > 4000 else ""}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("Section content not available.")


# ── Search Tab ────────────────────────────────────────────────────────────────
def _render_search_tab(doc_id: str):
    col1, col2 = st.columns([5, 1])
    with col1:
        query = st.text_input(
            "Search query",
            placeholder = "e.g. gradient descent optimization",
            label_visibility = "collapsed",
        )
    with col2:
        top_k = st.selectbox("Results", [3, 5, 10], index=1, label_visibility="collapsed")

    search_btn = st.button("Search", type="primary")

    if search_btn and query.strip():
        with st.spinner("Searching ..."):
            results = analysis_service.semantic_search(
                doc_id = doc_id,
                query  = query.strip(),
                top_k  = top_k,
            )

        if not results.results:
            st.info("No results found. Try a different query or lower the similarity threshold.")
            return

        st.markdown(
            f'<div style="font-family:\'DM Mono\',monospace; font-size:0.75rem; '
            f'color:#8a857d; margin-bottom:1rem;">'
            f'{results.total_found} results in {results.search_time_ms:.0f}ms</div>',
            unsafe_allow_html=True,
        )

        for r in results.results:
            score_pct = int(r.score * 100)
            st.markdown(f"""
            <div class="source-card">
                <div style="display:flex; justify-content:space-between; margin-bottom:0.4rem;">
                    <span class="source-section-tag">{r.chunk.section_type.value}</span>
                    <span style="font-family:'DM Mono',monospace; font-size:0.7rem;
                                 color:#2d6a4f; background:#d8f3dc; padding:0.1rem 0.5rem;
                                 border-radius:10px;">
                        {score_pct}% match
                    </span>
                </div>
                <div style="font-size:0.85rem; line-height:1.6; color:#2a2622;">
                    {r.chunk.content[:400]}{"..." if len(r.chunk.content) > 400 else ""}
                </div>
            </div>
            """, unsafe_allow_html=True)


# ── Info Tab ──────────────────────────────────────────────────────────────────
def _render_info_tab(info: dict):
    meta   = info.get("metadata", {})
    chunks = info.get("chunks", {})

    st.markdown("#### Document Metadata")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"""
        <div class="meta-item" style="margin-bottom:0.75rem;">
            <div class="meta-label">Title</div>
            <div style="font-size:0.9rem; font-weight:500;">{meta.get("title") or "—"}</div>
        </div>
        <div class="meta-item" style="margin-bottom:0.75rem;">
            <div class="meta-label">Authors</div>
            <div style="font-size:0.9rem;">{", ".join(meta.get("authors", [])) or "—"}</div>
        </div>
        <div class="meta-item">
            <div class="meta-label">Language</div>
            <div style="font-size:0.9rem;">{meta.get("language", "en").upper()}</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="meta-item" style="margin-bottom:0.75rem;">
            <div class="meta-label">Pages</div>
            <div class="meta-value">{meta.get("pages", 0)}</div>
        </div>
        <div class="meta-item" style="margin-bottom:0.75rem;">
            <div class="meta-label">Words</div>
            <div class="meta-value">{fmt_number(meta.get("words", 0))}</div>
        </div>
        <div class="meta-item">
            <div class="meta-label">File Size</div>
            <div class="meta-value" style="font-size:0.95rem;">{meta.get("file_size", "—")}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>#### Sections Detected", unsafe_allow_html=True)
    sections = info.get("sections", [])
    if sections:
        for sec in sections:
            st.markdown(f"""
            <div style="display:flex; justify-content:space-between; align-items:center;
                        padding:0.5rem 0.75rem; border-bottom:1px solid #e2ddd6;
                        font-size:0.85rem;">
                <span style="font-weight:500;">{sec['type'].capitalize()}</span>
                <span style="font-family:'DM Mono',monospace; font-size:0.75rem;
                             color:#8a857d;">{fmt_number(sec['word_count'])} words</span>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No sections detected.")

    st.markdown("<br>#### Vector Index", unsafe_allow_html=True)
    st.markdown(f"""
    <div style="display:flex; gap:1rem; flex-wrap:wrap;">
        <div class="meta-item" style="flex:1; min-width:120px;">
            <div class="meta-label">Total Chunks</div>
            <div class="meta-value">{chunks.get("total", 0)}</div>
        </div>
        <div class="meta-item" style="flex:1; min-width:120px;">
            <div class="meta-label">Indexed Vectors</div>
            <div class="meta-value">{chunks.get("indexed", 0)}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div style="margin-top:1rem; font-family:'DM Mono',monospace; font-size:0.72rem; color:#8a857d;">
        Created: {info.get("created_at", "")[:19].replace("T", " ")} UTC &nbsp;·&nbsp;
        Updated: {info.get("updated_at", "")[:19].replace("T", " ")} UTC
    </div>
    """, unsafe_allow_html=True)


# ── App Entry ─────────────────────────────────────────────────────────────────
def main():
    init_session()
    run_startup()
    render_sidebar()
    render_main()


if __name__ == "__main__":
    main()