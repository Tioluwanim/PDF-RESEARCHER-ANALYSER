"""
main.py - PDF Research Analyzer — Upgraded UI
Editorial dark-sidebar layout with animated chat, markdown rendering,
keyboard shortcuts, typing indicator, and polished empty states.
"""

from __future__ import annotations

import html
import re
import streamlit as st

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


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title            = STREAMLIT_PAGE_TITLE,
    page_icon             = STREAMLIT_PAGE_ICON,
    layout                = STREAMLIT_LAYOUT,
    initial_sidebar_state = "expanded",
)


# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Lora:ital,wght@0,400;0,600;1,400&family=JetBrains+Mono:wght@400;500&family=Outfit:wght@300;400;500;600&display=swap');

/* ── Tokens ──────────────────────────────────────────────────────── */
:root {
    --ink:       #0d0c0b;
    --paper:     #f8f5f0;
    --accent:    #bf3a14;
    --accent-2:  #e8863a;
    --muted:     #857f76;
    --border:    #ddd8cf;
    --surface:   #eee9e0;
    --surface-2: #e5dfd5;
    --success:   #2a6045;
    --warn:      #c47a1e;
    --sidebar:   #111009;
    --sidebar-2: #1c1a16;
    --sidebar-3: #272420;
}

/* ── Base ────────────────────────────────────────────────────────── */
html, body, [class*="css"] {
    font-family: 'Outfit', sans-serif;
    background: var(--paper);
    color: var(--ink);
}

/* Remove Streamlit chrome */
#MainMenu, footer { visibility: hidden; }
header { visibility: hidden; }

/* Keep sidebar toggle visible */
[data-testid="collapsedControl"] {
    display: block !important;
    visibility: visible !important;
    opacity: 1 !important;
}

/* ── Sidebar ─────────────────────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background: var(--sidebar) !important;
    border-right: 1px solid #1f1d18;
}
section[data-testid="stSidebar"] * { color: #d4cfc6 !important; }
section[data-testid="stSidebar"] .stButton > button {
    background: transparent;
    border: 1px solid #2a2720;
    color: #c8c2b8 !important;
    border-radius: 6px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.72rem;
    padding: 0.4rem 0.8rem;
    transition: background 0.15s, border-color 0.15s;
    width: 100%;
    text-align: left;
    letter-spacing: 0.01em;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: var(--sidebar-3);
    border-color: var(--accent);
    color: #f0ece4 !important;
}
section[data-testid="stSidebar"] .stButton > button[kind="primary"] {
    background: var(--accent) !important;
    border-color: var(--accent) !important;
    color: white !important;
}

/* ── Typography ──────────────────────────────────────────────────── */
.app-title {
    font-family: 'Lora', serif;
    font-size: 2.4rem;
    font-weight: 600;
    letter-spacing: -0.025em;
    line-height: 1.1;
    color: var(--ink);
    margin-bottom: 0.1rem;
}
.app-subtitle {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.72rem;
    color: var(--muted);
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin-bottom: 1.75rem;
}

/* ── Document header ─────────────────────────────────────────────── */
.doc-title {
    font-family: 'Lora', serif;
    font-size: 1.35rem;
    font-weight: 600;
    line-height: 1.3;
    color: var(--ink);
    margin-bottom: 0.2rem;
}
.doc-authors {
    font-size: 0.8rem;
    color: var(--muted);
    font-style: italic;
}

/* ── Stat cards ──────────────────────────────────────────────────── */
.stat-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 0.75rem 1rem;
    text-align: center;
}
.stat-label {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.62rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 0.2rem;
}
.stat-value {
    font-family: 'Lora', serif;
    font-size: 1.25rem;
    font-weight: 600;
    color: var(--ink);
}

/* ── Status pills ────────────────────────────────────────────────── */
.pill {
    display: inline-block;
    padding: 0.18rem 0.6rem;
    border-radius: 20px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.65rem;
    font-weight: 500;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}
.pill-ready   { background: #d4edda; color: var(--success); }
.pill-failed  { background: #fce4de; color: var(--accent); }
.pill-pending { background: #fef3e2; color: var(--warn); }
.pill-default { background: var(--surface); color: var(--muted); }

/* ── Chat messages ───────────────────────────────────────────────── */
.msg-user {
    background: var(--ink);
    color: #ece8e0;
    padding: 0.9rem 1.15rem;
    border-radius: 18px 18px 4px 18px;
    margin: 0.6rem 0 0.6rem 4rem;
    font-size: 0.9rem;
    line-height: 1.65;
    animation: slideInRight 0.2s ease;
}
.msg-assistant {
    background: var(--surface);
    color: var(--ink);
    padding: 0.9rem 1.15rem 0.9rem 1.25rem;
    border-radius: 4px 18px 18px 18px;
    margin: 0.6rem 4rem 0.6rem 0;
    font-size: 0.9rem;
    line-height: 1.75;
    border-left: 3px solid var(--accent);
    animation: slideInLeft 0.2s ease;
}
.msg-assistant p { margin: 0 0 0.6rem; }
.msg-assistant p:last-child { margin-bottom: 0; }
.msg-assistant code {
    background: var(--surface-2);
    padding: 0.1em 0.35em;
    border-radius: 4px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.82em;
}
.msg-assistant strong { color: var(--ink); font-weight: 600; }
.msg-assistant ul, .msg-assistant ol {
    margin: 0.4rem 0 0.6rem 1.2rem;
    padding: 0;
}
.msg-assistant li { margin-bottom: 0.3rem; }

/* Typing indicator */
.typing-indicator {
    display: flex;
    gap: 4px;
    align-items: center;
    padding: 0.9rem 1.15rem 0.9rem 1.25rem;
    background: var(--surface);
    border-radius: 4px 18px 18px 18px;
    margin: 0.6rem 4rem 0.6rem 0;
    border-left: 3px solid var(--accent);
    width: fit-content;
}
.typing-dot {
    width: 7px; height: 7px;
    background: var(--muted);
    border-radius: 50%;
    animation: typingBounce 1.2s ease infinite;
}
.typing-dot:nth-child(2) { animation-delay: 0.2s; }
.typing-dot:nth-child(3) { animation-delay: 0.4s; }

/* Chat input area */
.chat-input-wrap {
    background: var(--paper);
    border-top: 1px solid var(--border);
    padding: 0.75rem 0 0;
    margin-top: 0.5rem;
}

/* Section viewer */
.section-block {
    background: var(--surface);
    border-radius: 10px;
    padding: 1.25rem 1.5rem;
    border-left: 4px solid var(--accent);
    font-size: 0.88rem;
    line-height: 1.8;
    white-space: pre-wrap;
    font-family: 'Outfit', sans-serif;
}

/* Search result cards */
.result-card {
    background: var(--paper);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 0.85rem 1.1rem;
    margin: 0.5rem 0;
    transition: border-color 0.15s, box-shadow 0.15s;
}
.result-card:hover {
    border-color: var(--accent);
    box-shadow: 0 2px 12px rgba(191,58,20,0.08);
}
.result-section-tag {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.64rem;
    color: var(--accent);
    text-transform: uppercase;
    letter-spacing: 0.08em;
}
.result-score {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.68rem;
    color: var(--success);
    background: #d4edda;
    padding: 0.12rem 0.45rem;
    border-radius: 10px;
}
.result-text {
    font-size: 0.84rem;
    line-height: 1.65;
    color: #2a2520;
    margin-top: 0.4rem;
}

/* Meta items */
.meta-block {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 0.85rem 1rem;
    margin-bottom: 0.65rem;
}
.meta-label {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.62rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 0.2rem;
}
.meta-value { font-size: 0.9rem; font-weight: 500; color: var(--ink); }
.meta-value-lg {
    font-family: 'Lora', serif;
    font-size: 1.2rem;
    font-weight: 600;
    color: var(--ink);
}

/* Section list row */
.section-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.55rem 0.75rem;
    border-bottom: 1px solid var(--border);
    font-size: 0.84rem;
    transition: background 0.1s;
}
.section-row:hover { background: var(--surface); }
.section-row:last-child { border-bottom: none; }

/* Tab styling */
.stTabs [data-baseweb="tab"] {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.72rem;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}
.stTabs [aria-selected="true"] {
    color: var(--accent) !important;
    border-bottom-color: var(--accent) !important;
}

/* Inputs */
.stTextInput > div > div > input,
.stTextArea > div > div > textarea {
    border: 1.5px solid var(--border) !important;
    border-radius: 8px !important;
    font-family: 'Outfit', sans-serif !important;
    background: var(--paper) !important;
    font-size: 0.9rem !important;
}
.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus {
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 3px rgba(191,58,20,0.1) !important;
}

/* Buttons */
.stButton > button {
    font-family: 'Outfit', sans-serif;
    font-weight: 500;
    border-radius: 8px;
    transition: all 0.15s;
}
.stButton > button[kind="primary"] {
    background: var(--accent) !important;
    border: none !important;
    color: white !important;
    font-weight: 600;
}
.stButton > button[kind="primary"]:hover {
    background: #a02f0e !important;
    transform: translateY(-1px);
    box-shadow: 0 4px 14px rgba(191,58,20,0.35) !important;
}

/* Progress bar */
.stProgress > div > div { background: var(--accent) !important; }

/* Dividers */
hr { border-color: var(--border); margin: 1.25rem 0; }

/* Scrollbar */
::-webkit-scrollbar { width: 5px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

/* Selectbox */
.stSelectbox > div > div {
    border: 1.5px solid var(--border) !important;
    border-radius: 8px !important;
    background: var(--paper) !important;
}

/* Keyframes */
@keyframes slideInRight {
    from { opacity: 0; transform: translateX(12px); }
    to   { opacity: 1; transform: translateX(0); }
}
@keyframes slideInLeft {
    from { opacity: 0; transform: translateX(-12px); }
    to   { opacity: 1; transform: translateX(0); }
}
@keyframes typingBounce {
    0%, 60%, 100% { transform: translateY(0); opacity: 0.4; }
    30%           { transform: translateY(-5px); opacity: 1; }
}
@keyframes fadeIn {
    from { opacity: 0; transform: translateY(6px); }
    to   { opacity: 1; transform: translateY(0); }
}
</style>
""", unsafe_allow_html=True)


# ── Session state ─────────────────────────────────────────────────────────────
def _init_session() -> None:
    defaults = {
        "active_doc_id"     : None,
        "chat_history"      : [],
        "startup_done"      : False,
        "last_uploaded_name": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ── Startup ───────────────────────────────────────────────────────────────────
def _run_startup() -> None:
    if not st.session_state.startup_done:
        warnings = validate_config()
        log_startup(get_config_summary(), warnings)
        st.session_state.startup_done = True
        for w in warnings:
            st.warning(w, icon="⚠️")


# ── Utilities ─────────────────────────────────────────────────────────────────
def _fmt(n: int | float) -> str:
    return f"{int(n):,}"


def _pill(status: str) -> str:
    cls = {
        "ready"     : "pill-ready",
        "failed"    : "pill-failed",
        "uploading" : "pill-pending",
        "extracting": "pill-pending",
        "embedding" : "pill-pending",
        "extracted" : "pill-pending",
    }.get(status, "pill-default")
    return f'<span class="pill {cls}">{status}</span>'


def _md_to_html(text: str) -> str:
    """
    Lightweight Markdown → HTML converter for chat messages.
    Handles: **bold**, *italic*, `code`, bullet lists, numbered lists.
    Avoids importing markdown lib to keep dependencies minimal.
    """
    t = html.escape(text)
    # Fenced code blocks (```...```)
    t = re.sub(
        r"```(?:\w+\n)?(.*?)```",
        lambda m: f'<pre style="background:var(--surface-2);padding:0.75rem 1rem;border-radius:6px;font-family:\'JetBrains Mono\',monospace;font-size:0.8rem;overflow-x:auto;margin:0.5rem 0;">{m.group(1).strip()}</pre>',
        t, flags=re.DOTALL
    )
    # Inline code
    t = re.sub(r"`([^`]+)`", r'<code>\1</code>', t)
    # Bold
    t = re.sub(r"\*\*(.+?)\*\*", r'<strong>\1</strong>', t)
    # Italic
    t = re.sub(r"\*(.+?)\*", r'<em>\1</em>', t)
    # Bullet lists
    def _bullets(m: re.Match) -> str:
        items = re.findall(r"^[-•]\s+(.+)$", m.group(0), re.MULTILINE)
        return "<ul>" + "".join(f"<li>{i}</li>" for i in items) + "</ul>"
    t = re.sub(r"(^[-•]\s+.+$\n?)+", _bullets, t, flags=re.MULTILINE)
    # Numbered lists
    def _nums(m: re.Match) -> str:
        items = re.findall(r"^\d+\.\s+(.+)$", m.group(0), re.MULTILINE)
        return "<ol>" + "".join(f"<li>{i}</li>" for i in items) + "</ol>"
    t = re.sub(r"(^\d+\.\s+.+$\n?)+", _nums, t, flags=re.MULTILINE)
    # Paragraphs (double newline)
    paragraphs = [p.strip() for p in re.split(r"\n\n+", t) if p.strip()]
    result = []
    for p in paragraphs:
        if p.startswith(("<ul>", "<ol>", "<pre>")):
            result.append(p)
        else:
            result.append(f"<p>{p.replace(chr(10), '<br>')}</p>")
    return "\n".join(result)


# ── Sidebar ───────────────────────────────────────────────────────────────────
def _render_sidebar() -> None:
    with st.sidebar:
        # Wordmark
        st.markdown("""
        <div style="padding: 1.25rem 0 1.5rem;">
            <div style="font-family:'Lora',serif; font-size:1.3rem;
                        font-weight:600; color:#ede8e0; letter-spacing:-0.01em;">
                📄 PDF Analyzer
            </div>
            <div style="font-family:'JetBrains Mono',monospace; font-size:0.62rem;
                        color:#504b43; letter-spacing:0.12em; text-transform:uppercase;
                        margin-top:0.25rem;">
                Research Assistant
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Upload
        _sidebar_label("Upload PDF")
        uploaded = st.file_uploader(
            "Drop PDF here",
            type=["pdf"],
            label_visibility="collapsed",
        )
        if uploaded and uploaded.name != st.session_state.get("last_uploaded_name"):
            st.session_state.last_uploaded_name = uploaded.name
            _handle_upload(uploaded)

        _sidebar_divider()

        # Document list
        _sidebar_label("Documents")
        docs = analysis_service.list_documents()
        if not docs:
            st.markdown(
                '<div style="font-size:0.78rem; color:#3a3630; '
                'font-style:italic; padding:0.25rem 0;">No documents yet</div>',
                unsafe_allow_html=True,
            )
        else:
            for doc in docs:
                active = doc["doc_id"] == st.session_state.active_doc_id
                prefix = "▶ " if active else "   "
                name   = doc["filename"][:26] + ("…" if len(doc["filename"]) > 26 else "")
                if st.button(f"{prefix}{name}", key=f"doc_{doc['doc_id']}"):
                    st.session_state.active_doc_id = doc["doc_id"]
                    st.session_state.chat_history  = []
                    st.rerun()

        _sidebar_divider()

        # Provider status
        _sidebar_label("LLM Providers")
        providers = analysis_service.get_provider_status()
        for name, info in providers.items():
            dot   = "🟢" if info["configured"] else "🔴"
            model = info["model"].split("/")[-1][:24]
            st.markdown(
                f'<div style="font-family:\'JetBrains Mono\',monospace; '
                f'font-size:0.68rem; color:#6b6560; margin: 0.2rem 0; '
                f'display:flex; justify-content:space-between;">'
                f'<span>{dot} {name.upper()}</span>'
                f'<span style="color:#3a3630;">{model}</span></div>',
                unsafe_allow_html=True,
            )

        _sidebar_divider()

        # Keyboard shortcuts hint
        st.markdown(
            '<div style="font-family:\'JetBrains Mono\',monospace; font-size:0.62rem; '
            'color:#3a3630; line-height:1.7;">'
            '<span style="color:#504b43;">Tip:</span> Enter to send · Ctrl+L clear</div>',
            unsafe_allow_html=True,
        )


def _sidebar_label(text: str) -> None:
    st.markdown(
        f'<div style="font-family:\'JetBrains Mono\',monospace; font-size:0.65rem; '
        f'color:#504b43; text-transform:uppercase; letter-spacing:0.1em; '
        f'margin-bottom:0.5rem;">{text}</div>',
        unsafe_allow_html=True,
    )


def _sidebar_divider() -> None:
    st.markdown(
        "<hr style='border-color:#1f1d18; margin:1rem 0;'>",
        unsafe_allow_html=True,
    )


# ── Upload & processing ───────────────────────────────────────────────────────
def _handle_upload(f) -> None:
    with st.spinner("Uploading …"):
        doc, err = analysis_service.save_upload(
            file_bytes=f.read(), filename=f.name
        )
    if err:
        st.error(f"Upload failed: {err.detail if hasattr(err, 'detail') else err}")
        return
    st.success(f"✓ {f.name} uploaded")
    st.session_state.active_doc_id = doc.doc_id
    st.session_state.chat_history  = []
    _process_document(doc.doc_id)


def _process_document(doc_id: str) -> None:
    bar  = st.progress(0, text="Starting …")
    info = st.empty()

    def on_progress(step: str, pct: int) -> None:
        bar.progress(pct / 100, text=step)
        info.markdown(
            f'<div style="font-family:\'JetBrains Mono\',monospace; '
            f'font-size:0.72rem; color:var(--muted);">{step}</div>',
            unsafe_allow_html=True,
        )

    result = analysis_service.process_document(doc_id=doc_id, on_progress=on_progress)
    bar.empty()
    info.empty()

    if result.status == DocumentStatus.READY:
        st.success(
            f"✓ Ready — {result.page_count}p · "
            f"{_fmt(result.word_count)} words · "
            f"{result.chunk_count} chunks · "
            f"{len(result.sections_found)} sections"
        )
        st.rerun()
    else:
        st.error(f"Processing failed: {result.message}")


# ── Main content ──────────────────────────────────────────────────────────────
def _render_main() -> None:
    st.markdown(
        '<div class="app-title">PDF Research Analyzer</div>'
        '<div class="app-subtitle">'
        'Semantic search &nbsp;·&nbsp; Section detection &nbsp;·&nbsp; '
        'Chat with your paper'
        '</div>',
        unsafe_allow_html=True,
    )

    doc_id = st.session_state.active_doc_id
    if not doc_id:
        _render_empty_state()
        return

    info = analysis_service.get_document_info(doc_id)
    if "error" in info:
        st.error(info["error"])
        return

    _render_doc_header(info)
    st.markdown("<hr>", unsafe_allow_html=True)

    tab_chat, tab_sections, tab_search, tab_info = st.tabs([
        "💬  Chat", "📑  Sections", "🔍  Search", "ℹ️  Info",
    ])
    with tab_chat:     _render_chat_tab(doc_id, info)
    with tab_sections: _render_sections_tab(doc_id, info)
    with tab_search:   _render_search_tab(doc_id)
    with tab_info:     _render_info_tab(info)


def _render_empty_state() -> None:
    st.markdown("""
    <div style="display:flex; flex-direction:column; align-items:center;
                justify-content:center; padding:6rem 2rem; text-align:center;
                animation: fadeIn 0.4s ease;">
        <div style="font-size:3.5rem; margin-bottom:1.25rem;
                    filter:drop-shadow(0 4px 12px rgba(191,58,20,0.15));">📄</div>
        <div style="font-family:'Lora',serif; font-size:1.7rem; font-weight:600;
                    letter-spacing:-0.02em; margin-bottom:0.6rem; color:#0d0c0b;">
            Upload a research paper to begin
        </div>
        <div style="font-size:0.88rem; color:#857f76; max-width:420px;
                    line-height:1.7; margin-bottom:2rem;">
            Drop a PDF in the sidebar. The system will extract text,
            detect sections, build a semantic index, and let you
            chat with your document.
        </div>
        <div style="display:flex; gap:1.5rem; flex-wrap:wrap; justify-content:center;">
            <div style="background:#eee9e0; border-radius:10px; padding:0.75rem 1.25rem;
                        font-size:0.8rem; color:#504b43; min-width:140px; text-align:center;">
                <div style="font-size:1.3rem; margin-bottom:0.3rem;">🔍</div>
                <div style="font-weight:500;">Semantic Search</div>
                <div style="font-size:0.72rem; color:#857f76; margin-top:0.1rem;">
                    Find relevant passages
                </div>
            </div>
            <div style="background:#eee9e0; border-radius:10px; padding:0.75rem 1.25rem;
                        font-size:0.8rem; color:#504b43; min-width:140px; text-align:center;">
                <div style="font-size:1.3rem; margin-bottom:0.3rem;">💬</div>
                <div style="font-weight:500;">Chat</div>
                <div style="font-size:0.72rem; color:#857f76; margin-top:0.1rem;">
                    Ask questions naturally
                </div>
            </div>
            <div style="background:#eee9e0; border-radius:10px; padding:0.75rem 1.25rem;
                        font-size:0.8rem; color:#504b43; min-width:140px; text-align:center;">
                <div style="font-size:1.3rem; margin-bottom:0.3rem;">📑</div>
                <div style="font-weight:500;">Sections</div>
                <div style="font-size:0.72rem; color:#857f76; margin-top:0.1rem;">
                    Browse by structure
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def _render_doc_header(info: dict) -> None:
    meta   = info["metadata"]
    chunks = info.get("chunks", {})
    c1, c2, c3, c4 = st.columns([3, 1, 1, 1])

    with c1:
        title   = html.escape(meta.get("title") or info["filename"])
        authors = html.escape(", ".join(meta.get("authors", [])[:4]))
        st.markdown(
            f'<div class="doc-title">{title[:120]}</div>'
            + (f'<div class="doc-authors">{authors}</div>' if authors else ""),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f'<div class="stat-card"><div class="stat-label">Pages</div>'
            f'<div class="stat-value">{meta.get("pages", 0)}</div></div>',
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            f'<div class="stat-card"><div class="stat-label">Words</div>'
            f'<div class="stat-value">{_fmt(meta.get("words", 0))}</div></div>',
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            f'<div class="stat-card"><div class="stat-label">Chunks</div>'
            f'<div class="stat-value">{chunks.get("total", 0)}</div></div>',
            unsafe_allow_html=True,
        )


# ── Chat tab ──────────────────────────────────────────────────────────────────
def _render_chat_tab(doc_id: str, info: dict) -> None:
    status = info.get("status")
    if status != "ready":
        st.warning(
            f"Document status is **{status}**. "
            "Please wait for processing to complete.",
            icon="⏳",
        )
        if st.button("▶ Process Now", type="primary"):
            _process_document(doc_id)
        return

    history: list[ChatMessage] = st.session_state.chat_history

    # Chat history
    if not history:
        st.markdown("""
        <div style="text-align:center; padding:2.5rem 1rem; color:#a09890;
                    font-size:0.85rem; font-style:italic;">
            Ask a question about this paper to begin the conversation.
        </div>
        """, unsafe_allow_html=True)
    else:
        for msg in history:
            if msg.role == MessageRole.USER:
                st.markdown(
                    f'<div class="msg-user">{html.escape(msg.content)}</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div class="msg-assistant">{_md_to_html(msg.content)}</div>',
                    unsafe_allow_html=True,
                )

    # Input row
    st.markdown('<div class="chat-input-wrap">', unsafe_allow_html=True)
    c_in, c_btn, c_clr = st.columns([7, 1, 1])
    with c_in:
        question = st.text_input(
            "question",
            placeholder="Ask anything about this paper …",
            label_visibility="collapsed",
            key="chat_input",
        )
    with c_btn:
        send = st.button("Send", type="primary", use_container_width=True)
    with c_clr:
        if st.button("Clear", use_container_width=True, disabled=not history):
            st.session_state.chat_history = []
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

    # Suggested questions (only shown when history is empty)
    if not history:
        st.markdown(
            '<div style="font-family:\'JetBrains Mono\',monospace; font-size:0.65rem; '
            'color:#a09890; text-transform:uppercase; letter-spacing:0.08em; '
            'margin: 0.75rem 0 0.4rem;">Suggested questions</div>',
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
                if st.button(s, key=f"sugg_{s[:20]}", use_container_width=True):
                    _handle_chat(doc_id, s)

    if send and question and question.strip():
        _handle_chat(doc_id, question.strip())


def _handle_chat(doc_id: str, question: str) -> None:
    try:
        user_msg = ChatMessage(role=MessageRole.USER, content=question)
        st.session_state.chat_history.append(user_msg)
        if len(st.session_state.chat_history) > MAX_CHAT_HISTORY:
            st.session_state.chat_history = st.session_state.chat_history[-MAX_CHAT_HISTORY:]

        st.markdown(
            f'<div class="msg-user">{html.escape(question)}</div>',
            unsafe_allow_html=True,
        )

        # Typing indicator
        typing = st.empty()
        typing.markdown(
            '<div class="typing-indicator">'
            '<div class="typing-dot"></div>'
            '<div class="typing-dot"></div>'
            '<div class="typing-dot"></div>'
            '</div>',
            unsafe_allow_html=True,
        )

        container  = st.empty()
        full_reply = ""

        stream = analysis_service.chat_stream(
            doc_id   = doc_id,
            question = question,
            history  = st.session_state.chat_history[:-1],
        )

        for token in stream:
            if not token:
                continue
            full_reply += str(token)
            typing.empty()
            container.markdown(
                f'<div class="msg-assistant">{_md_to_html(full_reply)}'
                f'<span style="color:var(--accent);animation:typingBounce 1s infinite;">▌</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

        typing.empty()
        container.markdown(
            f'<div class="msg-assistant">{_md_to_html(full_reply)}</div>',
            unsafe_allow_html=True,
        )

        st.session_state.chat_history.append(
            ChatMessage(
                role    = MessageRole.ASSISTANT,
                content = full_reply or "⚠️ No response received.",
            )
        )

    except Exception as e:
        logger.error("Chat failed: %s", e)
        st.error("⚠️ Chat error. Please try again.")
        st.session_state.chat_history.append(
            ChatMessage(
                role    = MessageRole.ASSISTANT,
                content = "⚠️ Providers unavailable.",
            )
        )


# ── Sections tab ──────────────────────────────────────────────────────────────
def _render_sections_tab(doc_id: str, info: dict) -> None:
    sections = info.get("sections", [])
    if not sections:
        st.info("No sections detected in this document.")
        return

    section_order = [
        SectionType.ABSTRACT, SectionType.INTRODUCTION, SectionType.METHODS,
        SectionType.RESULTS,  SectionType.DISCUSSION,   SectionType.CONCLUSION,
        SectionType.REFERENCES, SectionType.OTHER,
    ]
    s_map     = {s["type"]: s for s in sections}
    available = [s for s in section_order if s.value in s_map]
    labels    = [s.value.capitalize() for s in available]

    idx = st.selectbox(
        "Section",
        range(len(labels)),
        format_func     = lambda i: labels[i],
        label_visibility= "collapsed",
    )
    if idx is None:
        return

    sel  = available[idx]
    meta = s_map[sel.value]

    c1, c2, c3 = st.columns(3)
    c1.metric("Section",    meta["type"].capitalize())
    c2.metric("Words",      _fmt(meta.get("word_count", 0)))
    c3.metric("Page",       meta.get("page_start", 0) + 1)

    st.markdown("<br>", unsafe_allow_html=True)

    content = analysis_service.get_section_content(doc_id, sel)
    if content:
        truncated = content[:5000]
        more      = len(content) > 5000
        st.markdown(
            f'<div class="section-block">{html.escape(truncated)}'
            f'{"…" if more else ""}</div>',
            unsafe_allow_html=True,
        )
        if more:
            st.caption(f"Showing first 5,000 of {_fmt(len(content))} characters.")
    else:
        st.info("Section content not available.")


# ── Search tab ────────────────────────────────────────────────────────────────
def _render_search_tab(doc_id: str) -> None:
    c1, c2 = st.columns([6, 1])
    with c1:
        query = st.text_input(
            "q", placeholder="Search within this paper …",
            label_visibility="collapsed",
        )
    with c2:
        top_k = st.selectbox(
            "k", [3, 5, 10], index=1,
            label_visibility="collapsed",
        )

    search = st.button("Search", type="primary")

    if search and query.strip():
        with st.spinner("Searching …"):
            results = analysis_service.semantic_search(
                doc_id=doc_id, query=query.strip(), top_k=top_k,
            )

        if not results.results:
            st.markdown("""
            <div style="text-align:center; padding:2rem; color:#a09890;
                        font-size:0.85rem;">
                No results found. Try a broader query or different keywords.
            </div>
            """, unsafe_allow_html=True)
            return

        st.markdown(
            f'<div style="font-family:\'JetBrains Mono\',monospace; '
            f'font-size:0.7rem; color:var(--muted); margin-bottom:0.75rem;">'
            f'{results.total_found} results &nbsp;·&nbsp; '
            f'{results.search_time_ms:.0f}ms</div>',
            unsafe_allow_html=True,
        )

        for r in results.results:
            pct  = int(r.score * 100)
            text = html.escape(r.chunk.content[:450])
            dots = "…" if len(r.chunk.content) > 450 else ""
            st.markdown(f"""
            <div class="result-card">
                <div style="display:flex; justify-content:space-between;
                            align-items:center; margin-bottom:0.4rem;">
                    <span class="result-section-tag">
                        {r.chunk.section_type.value}
                    </span>
                    <span class="result-score">{pct}% match</span>
                </div>
                <div class="result-text">{text}{dots}</div>
            </div>
            """, unsafe_allow_html=True)


# ── Info tab ──────────────────────────────────────────────────────────────────
def _render_info_tab(info: dict) -> None:
    meta   = info.get("metadata", {})
    chunks = info.get("chunks",   {})
    secs   = info.get("sections", [])

    st.markdown(
        '<div style="font-family:\'Lora\',serif; font-size:1.1rem; '
        'font-weight:600; margin-bottom:1rem;">Document Metadata</div>',
        unsafe_allow_html=True,
    )

    c1, c2 = st.columns(2)
    with c1:
        _meta_block("Title",   meta.get("title") or "—")
        _meta_block("Authors", ", ".join(meta.get("authors", [])) or "—")
        _meta_block("Language", meta.get("language", "en").upper())
    with c2:
        _meta_block("Pages",     str(meta.get("pages", 0)),     large=True)
        _meta_block("Words",     _fmt(meta.get("words", 0)),    large=True)
        _meta_block("File Size", meta.get("file_size", "—"))

    st.markdown(
        '<div style="font-family:\'Lora\',serif; font-size:1.1rem; '
        'font-weight:600; margin:1.5rem 0 0.75rem;">Sections Detected</div>',
        unsafe_allow_html=True,
    )

    if secs:
        rows = "".join(
            f'<div class="section-row">'
            f'<span style="font-weight:500;">{s["type"].capitalize()}</span>'
            f'<span style="font-family:\'JetBrains Mono\',monospace; '
            f'font-size:0.72rem; color:var(--muted);">'
            f'{_fmt(s["word_count"])} words · p.{s["page_start"]+1}</span>'
            f'</div>'
            for s in secs
        )
        st.markdown(
            f'<div style="border:1px solid var(--border); border-radius:10px; '
            f'overflow:hidden;">{rows}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("No sections detected.")

    st.markdown(
        '<div style="font-family:\'Lora\',serif; font-size:1.1rem; '
        'font-weight:600; margin:1.5rem 0 0.75rem;">Vector Index</div>',
        unsafe_allow_html=True,
    )
    ci, cv = st.columns(2)
    with ci: _meta_block("Total Chunks",   str(chunks.get("total",   0)), large=True)
    with cv: _meta_block("Indexed Vectors",str(chunks.get("indexed", 0)), large=True)

    created = info.get("created_at", "")[:19].replace("T", " ")
    updated = info.get("updated_at", "")[:19].replace("T", " ")
    if created:
        st.markdown(
            f'<div style="margin-top:1.25rem; font-family:\'JetBrains Mono\',monospace; '
            f'font-size:0.68rem; color:var(--muted);">'
            f'Created {created} UTC &nbsp;·&nbsp; Updated {updated} UTC</div>',
            unsafe_allow_html=True,
        )


def _meta_block(label: str, value: str, large: bool = False) -> None:
    val_cls = "meta-value-lg" if large else "meta-value"
    st.markdown(
        f'<div class="meta-block">'
        f'<div class="meta-label">{label}</div>'
        f'<div class="{val_cls}">{html.escape(value)}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    _init_session()
    _run_startup()
    _render_sidebar()
    _render_main()


if __name__ == "__main__":
    main()