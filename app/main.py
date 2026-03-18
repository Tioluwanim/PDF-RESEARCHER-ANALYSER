"""
main.py - PDF Research Analyzer
Production UI: single PDF, batch (1-50), export (XLSX/DOCX/CSV/JSON).
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

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
from app.services.batch_service    import batch_service
from app.services.export_service   import export_service
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
@import url('https://fonts.googleapis.com/css2?family=Lora:ital,wght@0,400;0,600;1,400&family=JetBrains+Mono:wght@400;500&family=Outfit:wght@300;400;500;600&display=swap&font-display=swap');

:root {
    --ink:       #0d0c0b;
    --paper:     #f8f5f0;
    --accent:    #bf3a14;
    --muted:     #857f76;
    --border:    #ddd8cf;
    --surface:   #eee9e0;
    --surface-2: #e5dfd5;
    --success:   #2a6045;
    --warn:      #c47a1e;
    --sidebar:   #111009;
    --sidebar-3: #272420;
}

html, body, [class*="css"] {
    font-family: 'Outfit', system-ui, -apple-system, sans-serif;
    background: var(--paper);
    color: var(--ink);
}
.app-title, .doc-title, .meta-value-lg, .stat-value {
    font-family: 'Lora', Georgia, 'Times New Roman', serif;
}
.app-subtitle, .stat-label, .meta-label, .result-section-tag,
.result-score, [class*="Mono"] {
    font-family: 'JetBrains Mono', 'Courier New', monospace;
}

/* Chrome removal */
#MainMenu { visibility: hidden; }
footer    { visibility: hidden; }
header[data-testid="stHeader"] { background: transparent !important; }
.stDeployButton              { display: none !important; }
[data-testid="stToolbar"]    { display: none !important; }
[data-testid="stDecoration"] { display: none !important; }
[data-testid="stStatusWidget"]{ display: none !important; }

/* Sidebar toggle — always visible */
[data-testid="collapsedControl"] {
    visibility: visible !important;
    display:    flex    !important;
    opacity:    1       !important;
    z-index:    99999   !important;
    pointer-events: auto !important;
}
[data-testid="collapsedControl"] * {
    visibility: visible !important;
    pointer-events: auto !important;
    color: var(--accent) !important;
}

/* Sidebar */
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

/* Typography */
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
    margin-bottom: 1.5rem;
}

/* Stat cards */
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
.doc-title {
    font-family: 'Lora', serif;
    font-size: 1.35rem;
    font-weight: 600;
    line-height: 1.3;
    color: var(--ink);
    margin-bottom: 0.2rem;
}
.doc-authors { font-size: 0.8rem; color: var(--muted); font-style: italic; }

/* Chat */
.msg-user {
    background: var(--ink);
    color: #ece8e0;
    padding: 0.9rem 1.15rem;
    border-radius: 18px 18px 4px 18px;
    margin: 0.6rem 0 0.6rem 4rem;
    font-size: 0.9rem;
    line-height: 1.65;
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
}
.msg-assistant p  { margin: 0 0 0.6rem; }
.msg-assistant p:last-child { margin-bottom: 0; }
.msg-assistant code {
    background: var(--surface-2);
    padding: 0.1em 0.35em;
    border-radius: 4px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.82em;
}
.msg-assistant strong { font-weight: 600; }
.msg-assistant ul, .msg-assistant ol { margin: 0.4rem 0 0.6rem 1.2rem; padding: 0; }
.msg-assistant li { margin-bottom: 0.3rem; }

/* Typing indicator */
.typing-indicator {
    display: flex; gap: 4px; align-items: center;
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

/* Section viewer */
.section-block {
    background: var(--surface);
    border-radius: 10px;
    padding: 1.25rem 1.5rem;
    border-left: 4px solid var(--accent);
    font-size: 0.88rem;
    line-height: 1.8;
    white-space: pre-wrap;
}

/* Search cards */
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

/* Meta blocks */
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
.meta-value     { font-size: 0.9rem; font-weight: 500; color: var(--ink); }
.meta-value-lg  { font-family: 'Lora', serif; font-size: 1.2rem; font-weight: 600; color: var(--ink); }

/* Section list */
.section-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.55rem 0.75rem;
    border-bottom: 1px solid var(--border);
    font-size: 0.84rem;
}
.section-row:last-child { border-bottom: none; }

/* Batch result row */
.batch-row {
    display: flex; gap: 1rem; align-items: center;
    padding: 0.45rem 0.75rem;
    border-bottom: 1px solid var(--border);
    font-size: 0.82rem;
}
.batch-row:last-child { border-bottom: none; }

/* Tabs */
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
.stTextArea  > div > div > textarea {
    border: 1.5px solid var(--border) !important;
    border-radius: 8px !important;
    font-family: 'Outfit', sans-serif !important;
    background: var(--paper) !important;
    font-size: 0.9rem !important;
}
.stTextInput > div > div > input:focus,
.stTextArea  > div > div > textarea:focus {
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

/* Misc */
.stProgress > div > div { background: var(--accent) !important; }
hr { border-color: var(--border); margin: 1.25rem 0; }
::-webkit-scrollbar       { width: 5px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
.stSelectbox > div > div  {
    border: 1.5px solid var(--border) !important;
    border-radius: 8px !important;
    background: var(--paper) !important;
}
.stMultiSelect > div { border-radius: 8px !important; }

@keyframes typingBounce {
    0%, 60%, 100% { transform: translateY(0);   opacity: 0.4; }
    30%            { transform: translateY(-5px); opacity: 1;   }
}
@keyframes fadeIn {
    from { opacity: 0; transform: translateY(6px); }
    to   { opacity: 1; transform: translateY(0);   }
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
        "app_mode"          : "📄 Single PDF",
        # Export: store generated bytes so rerun doesn't lose them
        "export_data"       : {},   # format → (bytes, filename)
        # Batch: store last results so rerun doesn't re-run batch
        "batch_done"        : False,
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
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return "0"


def _md_to_html(text: str) -> str:
    """Lightweight Markdown → safe HTML for chat bubbles."""
    t = html.escape(text)
    # Fenced code blocks
    t = re.sub(
        r"```(?:\w+\n)?(.*?)```",
        lambda m: (
            '<pre style="background:var(--surface-2);padding:0.75rem 1rem;'
            'border-radius:6px;font-family:\'JetBrains Mono\',monospace;'
            f'font-size:0.8rem;overflow-x:auto;margin:0.5rem 0;">'
            f'{m.group(1).strip()}</pre>'
        ),
        t, flags=re.DOTALL,
    )
    t = re.sub(r"`([^`]+)`",      r'<code>\1</code>', t)
    t = re.sub(r"\*\*(.+?)\*\*",  r'<strong>\1</strong>', t)
    t = re.sub(r"\*(.+?)\*",      r'<em>\1</em>', t)

    def _bullets(m: re.Match) -> str:
        items = re.findall(r"^[-•]\s+(.+)$", m.group(0), re.MULTILINE)
        return "<ul>" + "".join(f"<li>{i}</li>" for i in items) + "</ul>"

    def _nums(m: re.Match) -> str:
        items = re.findall(r"^\d+\.\s+(.+)$", m.group(0), re.MULTILINE)
        return "<ol>" + "".join(f"<li>{i}</li>" for i in items) + "</ol>"

    t = re.sub(r"(^[-•]\s+.+$\n?)+", _bullets, t, flags=re.MULTILINE)
    t = re.sub(r"(^\d+\.\s+.+$\n?)+", _nums,    t, flags=re.MULTILINE)

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
        st.markdown("""
        <div style="padding:1.25rem 0 1.5rem;">
            <div style="font-family:'Lora',serif;font-size:1.3rem;
                        font-weight:600;color:#ede8e0;letter-spacing:-0.01em;">
                📄 PDF Analyzer
            </div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:0.62rem;
                        color:#504b43;letter-spacing:0.12em;text-transform:uppercase;
                        margin-top:0.25rem;">
                Research Assistant
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Single-PDF upload (only shown in Single PDF mode)
        _sidebar_label("Upload PDF")
        uploaded = st.file_uploader(
            "Drop PDF here", type=["pdf"], label_visibility="collapsed",
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
                '<div style="font-size:0.78rem;color:#3a3630;'
                'font-style:italic;padding:0.25rem 0;">No documents yet</div>',
                unsafe_allow_html=True,
            )
        else:
            for doc in docs:
                active = doc["doc_id"] == st.session_state.active_doc_id
                prefix = "▶ " if active else "   "
                name   = doc["filename"][:26] + ("…" if len(doc["filename"]) > 26 else "")
                status_icon = "✓" if doc.get("status") == "ready" else "⏳"
                if st.button(
                    f"{prefix}{status_icon} {name}",
                    key=f"doc_{doc['doc_id']}",
                ):
                    st.session_state.active_doc_id = doc["doc_id"]
                    st.session_state.chat_history  = []
                    st.session_state.app_mode      = "📄 Single PDF"
                    st.rerun()

        _sidebar_divider()

        # LLM provider status
        _sidebar_label("LLM Providers")
        try:
            providers = analysis_service.get_provider_status()
            for name, info in providers.items():
                dot   = "🟢" if info.get("configured") else "🔴"
                model = info.get("model", "—").split("/")[-1][:24]
                st.markdown(
                    f'<div style="font-family:\'JetBrains Mono\',monospace;'
                    f'font-size:0.68rem;color:#6b6560;margin:0.2rem 0;'
                    f'display:flex;justify-content:space-between;">'
                    f'<span>{dot} {name.upper()}</span>'
                    f'<span style="color:#3a3630;">{model}</span></div>',
                    unsafe_allow_html=True,
                )
        except Exception:
            st.markdown(
                '<div style="font-size:0.72rem;color:#3a3630;">Status unavailable</div>',
                unsafe_allow_html=True,
            )

        _sidebar_divider()
        st.markdown(
            '<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.62rem;'
            'color:#3a3630;line-height:1.7;">'
            '<span style="color:#504b43;">Tip:</span> Use Batch to upload 50 PDFs at once</div>',
            unsafe_allow_html=True,
        )


def _sidebar_label(text: str) -> None:
    st.markdown(
        f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.65rem;'
        f'color:#504b43;text-transform:uppercase;letter-spacing:0.1em;'
        f'margin-bottom:0.5rem;">{text}</div>',
        unsafe_allow_html=True,
    )


def _sidebar_divider() -> None:
    st.markdown("<hr style='border-color:#1f1d18;margin:1rem 0;'>", unsafe_allow_html=True)


# ── Upload / Process ──────────────────────────────────────────────────────────
def _handle_upload(f) -> None:
    with st.spinner("Uploading …"):
        doc, err = analysis_service.save_upload(file_bytes=f.read(), filename=f.name)
    if err or not doc:
        st.error(f"Upload failed: {getattr(err, 'detail', str(err))}")
        return
    st.success(f"✓ {f.name} uploaded")
    st.session_state.active_doc_id = doc.doc_id
    st.session_state.chat_history  = []
    _process_document(doc.doc_id)


def _process_document(doc_id: str) -> None:
    bar  = st.progress(0, text="Starting …")
    slot = st.empty()

    def on_progress(step: str, pct: int) -> None:
        bar.progress(max(0.0, min(pct / 100, 1.0)), text=step)
        slot.markdown(
            f'<div style="font-family:\'JetBrains Mono\',monospace;'
            f'font-size:0.72rem;color:var(--muted);">{step}</div>',
            unsafe_allow_html=True,
        )

    result = analysis_service.process_document(doc_id=doc_id, on_progress=on_progress)
    bar.empty()
    slot.empty()

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


# ── Main ──────────────────────────────────────────────────────────────────────
def _render_main() -> None:
    st.markdown(
        '<div class="app-title">PDF Research Analyzer</div>'
        '<div class="app-subtitle">'
        'Semantic search &nbsp;·&nbsp; Section detection &nbsp;·&nbsp; Chat with your paper'
        '</div>',
        unsafe_allow_html=True,
    )

    # Mode selector — persisted in session state to survive reruns
    mode = st.radio(
        "mode",
        ["📄 Single PDF", "📚 Batch Upload", "📤 Export"],
        index    = ["📄 Single PDF", "📚 Batch Upload", "📤 Export"].index(
            st.session_state.get("app_mode", "📄 Single PDF")
        ),
        horizontal       = True,
        label_visibility = "collapsed",
        key              = "mode_radio",
    )
    # Sync to session state so sidebar document clicks restore correct mode
    if mode != st.session_state.get("app_mode"):
        st.session_state.app_mode    = mode
        st.session_state.export_data = {}   # clear stale export data on mode switch

    st.markdown("<hr>", unsafe_allow_html=True)

    if mode == "📚 Batch Upload":
        _render_batch_tab()
        return
    if mode == "📤 Export":
        _render_export_tab()
        return

    # ── Single PDF ────────────────────────────────────────────────────────────
    doc_id = st.session_state.active_doc_id
    if not doc_id:
        _render_empty_state()
        return

    info = analysis_service.get_document_info(doc_id)
    if "error" in info:
        st.error(info["error"])
        st.session_state.active_doc_id = None
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


# ── Empty state ───────────────────────────────────────────────────────────────
def _render_empty_state() -> None:
    st.markdown("""
    <div style="display:flex;flex-direction:column;align-items:center;
                justify-content:center;padding:5rem 2rem;text-align:center;
                animation:fadeIn 0.4s ease;">
        <div style="font-size:3.5rem;margin-bottom:1.25rem;">📄</div>
        <div style="font-family:'Lora',serif;font-size:1.6rem;font-weight:600;
                    letter-spacing:-0.02em;margin-bottom:0.6rem;">
            Upload a research paper to begin
        </div>
        <div style="font-size:0.88rem;color:#857f76;max-width:420px;
                    line-height:1.7;margin-bottom:2rem;">
            Drop a PDF in the sidebar to analyse a single paper,
            or switch to <strong>Batch Upload</strong> to process up to 50 at once.
        </div>
        <div style="display:flex;gap:1.5rem;flex-wrap:wrap;justify-content:center;">
            <div style="background:#eee9e0;border-radius:10px;padding:0.75rem 1.25rem;
                        font-size:0.8rem;color:#504b43;min-width:130px;text-align:center;">
                <div style="font-size:1.3rem;margin-bottom:0.3rem;">🔍</div>
                <div style="font-weight:500;">Search</div>
            </div>
            <div style="background:#eee9e0;border-radius:10px;padding:0.75rem 1.25rem;
                        font-size:0.8rem;color:#504b43;min-width:130px;text-align:center;">
                <div style="font-size:1.3rem;margin-bottom:0.3rem;">💬</div>
                <div style="font-weight:500;">Chat</div>
            </div>
            <div style="background:#eee9e0;border-radius:10px;padding:0.75rem 1.25rem;
                        font-size:0.8rem;color:#504b43;min-width:130px;text-align:center;">
                <div style="font-size:1.3rem;margin-bottom:0.3rem;">📤</div>
                <div style="font-weight:500;">Export</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


# ── Doc header ────────────────────────────────────────────────────────────────
def _render_doc_header(info: dict) -> None:
    meta   = info.get("metadata", {})
    chunks = info.get("chunks", {})
    c1, c2, c3, c4 = st.columns([3, 1, 1, 1])
    with c1:
        title   = html.escape(meta.get("title") or info.get("filename", ""))
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
            f"Document status is **{status}**. Processing must complete before chatting.",
            icon="⏳",
        )
        if st.button("▶ Process Now", type="primary", key="process_now"):
            _process_document(doc_id)
        return

    history: list[ChatMessage] = st.session_state.chat_history

    if not history:
        st.markdown(
            '<div style="text-align:center;padding:2.5rem 1rem;color:#a09890;'
            'font-size:0.85rem;font-style:italic;">'
            'Ask a question about this paper to begin.</div>',
            unsafe_allow_html=True,
        )
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
    c_in, c_btn, c_clr = st.columns([7, 1, 1])
    with c_in:
        question = st.text_input(
            "question",
            placeholder="Ask anything about this paper …",
            label_visibility="collapsed",
            key="chat_input",
        )
    with c_btn:
        send = st.button("Send", type="primary", use_container_width=True, key="send_btn")
    with c_clr:
        if st.button("Clear", use_container_width=True, disabled=not history, key="clear_btn"):
            st.session_state.chat_history = []
            st.rerun()

    # Suggested questions — only when no history
    if not history:
        st.markdown(
            '<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.65rem;'
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
                if st.button(s, key=f"sugg_{hash(s)}", use_container_width=True):
                    _handle_chat(doc_id, s)
                    st.rerun()

    if send and question and question.strip():
        _handle_chat(doc_id, question.strip())


def _handle_chat(doc_id: str, question: str) -> None:
    try:
        st.session_state.chat_history.append(
            ChatMessage(role=MessageRole.USER, content=question)
        )
        if len(st.session_state.chat_history) > MAX_CHAT_HISTORY:
            st.session_state.chat_history = st.session_state.chat_history[-MAX_CHAT_HISTORY:]

        st.markdown(
            f'<div class="msg-user">{html.escape(question)}</div>',
            unsafe_allow_html=True,
        )

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
            doc_id  = doc_id,
            question= question,
            history = st.session_state.chat_history[:-1],
        )

        for token in stream:
            if not token:
                continue
            full_reply += str(token)
            typing.empty()
            container.markdown(
                f'<div class="msg-assistant">{_md_to_html(full_reply)}'
                f'<span style="color:var(--accent);">▌</span></div>',
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
        # Remove the user message we appended since chat failed
        if st.session_state.chat_history and \
                st.session_state.chat_history[-1].role == MessageRole.USER:
            st.session_state.chat_history.pop()


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
    if not available:
        st.info("No sections detected.")
        return

    labels = [s.value.capitalize() for s in available]
    idx    = st.selectbox(
        "Section", range(len(labels)),
        format_func=lambda i: labels[i],
        label_visibility="collapsed",
    )
    if idx is None:
        return

    sel  = available[idx]
    meta = s_map[sel.value]

    c1, c2, c3 = st.columns(3)
    c1.metric("Section", meta["type"].capitalize())
    c2.metric("Words",   _fmt(meta.get("word_count", 0)))
    c3.metric("Page",    meta.get("page_start", 0) + 1)
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
            st.caption(f"Showing first 5,000 of {_fmt(len(content))} characters.")
    else:
        st.info("Section content not available.")


# ── Search tab ────────────────────────────────────────────────────────────────
def _render_search_tab(doc_id: str) -> None:
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
                f'<div style="font-family:\'JetBrains Mono\',monospace;'
                f'font-size:0.7rem;color:var(--muted);margin-bottom:0.75rem;">'
                f'{results.total_found} result{"s" if results.total_found != 1 else ""}'
                f' &nbsp;·&nbsp; {results.search_time_ms:.0f}ms</div>',
                unsafe_allow_html=True,
            )

            for r in results.results:
                pct  = int(r.score * 100)
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


# ── Info tab ──────────────────────────────────────────────────────────────────
def _render_info_tab(info: dict) -> None:
    meta   = info.get("metadata", {})
    chunks = info.get("chunks",   {})
    secs   = info.get("sections", [])

    st.markdown(
        '<div style="font-family:\'Lora\',serif;font-size:1.1rem;'
        'font-weight:600;margin-bottom:1rem;">Document Metadata</div>',
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns(2)
    with c1:
        _meta_block("Title",    meta.get("title") or "—")
        _meta_block("Authors",  ", ".join(meta.get("authors", [])) or "—")
        _meta_block("Language", meta.get("language", "en").upper())
    with c2:
        _meta_block("Pages",     str(meta.get("pages", 0)),  large=True)
        _meta_block("Words",     _fmt(meta.get("words", 0)), large=True)
        _meta_block("File Size", meta.get("file_size", "—"))

    st.markdown(
        '<div style="font-family:\'Lora\',serif;font-size:1.1rem;'
        'font-weight:600;margin:1.5rem 0 0.75rem;">Sections Detected</div>',
        unsafe_allow_html=True,
    )
    if secs:
        rows = "".join(
            f'<div class="section-row">'
            f'<span style="font-weight:500;">{s["type"].capitalize()}</span>'
            f'<span style="font-family:\'JetBrains Mono\',monospace;'
            f'font-size:0.72rem;color:var(--muted);">'
            f'{_fmt(s["word_count"])} words · p.{s["page_start"]+1}</span>'
            f'</div>'
            for s in secs
        )
        st.markdown(
            f'<div style="border:1px solid var(--border);border-radius:10px;'
            f'overflow:hidden;">{rows}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("No sections detected.")

    st.markdown(
        '<div style="font-family:\'Lora\',serif;font-size:1.1rem;'
        'font-weight:600;margin:1.5rem 0 0.75rem;">Vector Index</div>',
        unsafe_allow_html=True,
    )
    ci, cv = st.columns(2)
    with ci: _meta_block("Total Chunks",    str(chunks.get("total",   0)), large=True)
    with cv: _meta_block("Indexed Vectors", str(chunks.get("indexed", 0)), large=True)

    created = info.get("created_at", "")[:19].replace("T", " ")
    if created:
        st.markdown(
            f'<div style="margin-top:1rem;font-family:\'JetBrains Mono\',monospace;'
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


# ── Batch tab ─────────────────────────────────────────────────────────────────
def _render_batch_tab() -> None:
    st.markdown(
        '<div style="font-family:\'Lora\',serif;font-size:1.3rem;'
        'font-weight:600;margin-bottom:0.25rem;">Batch Upload</div>'
        '<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.7rem;'
        'color:var(--muted);margin-bottom:1.5rem;">Upload 1–50 PDFs and process them all at once</div>',
        unsafe_allow_html=True,
    )

    uploaded_files = st.file_uploader(
        "Drop PDFs here",
        type                  = ["pdf"],
        accept_multiple_files = True,
        label_visibility      = "collapsed",
        key                   = "batch_uploader",
    )

    if not uploaded_files:
        st.markdown("""
        <div style="text-align:center;padding:3rem 1rem;color:#a09890;font-size:0.85rem;">
            <div style="font-size:2.5rem;margin-bottom:0.75rem;">📚</div>
            Drop multiple PDFs above — up to 50 at a time.<br>
            Each will be extracted, embedded, and indexed automatically.
        </div>
        """, unsafe_allow_html=True)
        return

    count = len(uploaded_files)
    if count > 50:
        st.error(f"Maximum 50 PDFs per batch. You selected {count}. Please remove some files.")
        return

    st.markdown(
        f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.75rem;'
        f'color:var(--muted);margin-bottom:1rem;">'
        f'{count} file{"s" if count != 1 else ""} selected</div>',
        unsafe_allow_html=True,
    )

    # Preview list (first 15)
    preview_rows = "".join(
        f'<div class="batch-row">'
        f'<span>📄</span>'
        f'<span style="flex:1;">{html.escape(f.name[:50])}</span>'
        f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.7rem;'
        f'color:var(--muted);">{round(len(f.getvalue())/1024,1)} KB</span>'
        f'</div>'
        for f in uploaded_files[:15]
    )
    if count > 15:
        preview_rows += (
            f'<div class="batch-row" style="color:var(--muted);font-style:italic;">'
            f'… and {count - 15} more</div>'
        )
    st.markdown(
        f'<div style="border:1px solid var(--border);border-radius:10px;'
        f'overflow:hidden;margin-bottom:1rem;">{preview_rows}</div>',
        unsafe_allow_html=True,
    )

    if st.button(f"▶ Process All {count} PDFs", type="primary", key="batch_run"):
        _run_batch(uploaded_files)


def _run_batch(uploaded_files) -> None:
    total      = len(uploaded_files)
    bar        = st.progress(0, text="Starting batch …")
    status_el  = st.empty()
    results_el = st.empty()
    rows: list[dict] = []

    def on_start(current: int, total: int, filename: str) -> None:
        pct = max(0.0, min((current - 1) / total, 1.0))
        bar.progress(pct, text=f"[{current}/{total}] {filename[:40]} …")
        status_el.markdown(
            f'<div style="font-family:\'JetBrains Mono\',monospace;'
            f'font-size:0.72rem;color:var(--muted);">Processing: {html.escape(filename)}</div>',
            unsafe_allow_html=True,
        )

    def on_done(item) -> None:
        icon  = "✓" if item.status == "ready" else "✗"
        color = "var(--success)" if item.status == "ready" else "var(--accent)"
        rows.append({
            "icon": icon, "color": color,
            "filename": item.filename, "status": item.status,
            "pages": item.pages, "words": item.words,
            "chunks": item.chunks, "error": item.error,
        })
        html_rows = "".join(
            f'<div class="batch-row">'
            f'<span style="color:{r["color"]};font-weight:700;min-width:1rem;">{r["icon"]}</span>'
            f'<span style="flex:1;">{html.escape(r["filename"][:40])}</span>'
            f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.7rem;color:var(--muted);">'
            + (f'{r["pages"]}p · {r["words"]:,}w · {r["chunks"]} chunks'
               if r["status"] == "ready"
               else f'<span style="color:var(--accent);">{html.escape(r["error"][:40])}</span>')
            + '</span></div>'
            for r in rows
        )
        results_el.markdown(
            f'<div style="border:1px solid var(--border);border-radius:10px;'
            f'overflow:hidden;margin-top:0.5rem;">{html_rows}</div>',
            unsafe_allow_html=True,
        )

    # Read all file bytes before processing (files can't be re-read after)
    files = []
    for f in uploaded_files:
        try:
            files.append((f.getvalue(), f.name))
        except Exception:
            files.append((f.read(), f.name))

    result = batch_service.process_batch(
        files, on_item_start=on_start, on_item_done=on_done,
    )

    bar.progress(1.0, text="Batch complete ✓")
    status_el.empty()

    st.success(
        f"✓ Batch complete — {result.succeeded}/{result.total} succeeded "
        f"in {result.duration_s:.1f}s"
    )
    if result.failed > 0:
        failed_names = [i.filename for i in result.items if i.status == "failed"]
        st.warning(
            f"{result.failed} file(s) failed: {', '.join(failed_names[:5])}"
            + (" …" if len(failed_names) > 5 else "")
        )

    # Auto-select first successful doc
    for item in result.items:
        if item.status == "ready" and item.doc_id:
            st.session_state.active_doc_id = item.doc_id
            break

    st.rerun()


# ── Export tab ────────────────────────────────────────────────────────────────
def _render_export_tab() -> None:
    st.markdown(
        '<div style="font-family:\'Lora\',serif;font-size:1.3rem;'
        'font-weight:600;margin-bottom:0.25rem;">Export</div>'
        '<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.7rem;'
        'color:var(--muted);margin-bottom:1.5rem;">'
        'Download extracted metadata — XLSX · DOCX · CSV · JSON</div>',
        unsafe_allow_html=True,
    )

    docs       = analysis_service.list_documents()
    ready_docs = [d for d in docs if d.get("status") == "ready"]

    if not ready_docs:
        st.info("No processed documents found. Upload and process PDFs first.")
        return

    all_names = [d["filename"] for d in ready_docs]
    selected  = st.multiselect(
        "Select documents to export",
        options  = all_names,
        default  = all_names,
        key      = "export_select",
    )

    selected_ids = [
        d["doc_id"] for d in ready_docs if d["filename"] in selected
    ]

    if not selected_ids:
        st.warning("Select at least one document to export.", icon="⚠️")
        return

    st.caption(f"{len(selected_ids)} document(s) selected")
    st.markdown("<br>", unsafe_allow_html=True)

    # ── Export format cards ───────────────────────────────────────────────────
    # BUG FIX: Generate + Download in one click using session_state cache.
    # Previously, "Generate" triggered a rerun which lost the bytes before
    # download_button could render. Now we store bytes in session_state.

    c1, c2, c3, c4 = st.columns(4)
    export_cache = st.session_state.export_data

    with c1:
        st.markdown("**📊 Excel (XLSX)**")
        st.caption("Matches your metadata template")
        if st.button("Generate XLSX", type="primary", use_container_width=True, key="gen_xlsx"):
            with st.spinner("Building …"):
                try:
                    export_cache["xlsx"] = export_service.export_xlsx(selected_ids)
                except Exception as e:
                    st.error(f"XLSX export failed: {e}")
        if "xlsx" in export_cache:
            data, fname = export_cache["xlsx"]
            st.download_button(
                "⬇ Download XLSX", data=data, file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, key="dl_xlsx",
            )

    with c2:
        st.markdown("**📝 Word (DOCX)**")
        st.caption("Formatted report per document")
        if st.button("Generate DOCX", type="primary", use_container_width=True, key="gen_docx"):
            with st.spinner("Building …"):
                try:
                    export_cache["docx"] = export_service.export_docx(selected_ids)
                except Exception as e:
                    st.error(f"DOCX export failed: {e}")
        if "docx" in export_cache:
            data, fname = export_cache["docx"]
            st.download_button(
                "⬇ Download DOCX", data=data, file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                use_container_width=True, key="dl_docx",
            )

    with c3:
        st.markdown("**📋 CSV**")
        st.caption("Plain text, importable anywhere")
        if st.button("Generate CSV", type="primary", use_container_width=True, key="gen_csv"):
            with st.spinner("Building …"):
                try:
                    export_cache["csv"] = export_service.export_csv(selected_ids)
                except Exception as e:
                    st.error(f"CSV export failed: {e}")
        if "csv" in export_cache:
            data, fname = export_cache["csv"]
            st.download_button(
                "⬇ Download CSV", data=data, file_name=fname,
                mime="text/csv", use_container_width=True, key="dl_csv",
            )

    with c4:
        st.markdown("**🔗 JSON**")
        st.caption("For API / integration use")
        if st.button("Generate JSON", type="primary", use_container_width=True, key="gen_json"):
            with st.spinner("Building …"):
                try:
                    export_cache["json"] = export_service.export_json(selected_ids)
                except Exception as e:
                    st.error(f"JSON export failed: {e}")
        if "json" in export_cache:
            data, fname = export_cache["json"]
            st.download_button(
                "⬇ Download JSON", data=data, file_name=fname,
                mime="application/json", use_container_width=True, key="dl_json",
            )

    # Preview table
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '<div style="font-family:\'Lora\',serif;font-size:1rem;'
        'font-weight:600;margin-bottom:0.5rem;">Selected Documents</div>',
        unsafe_allow_html=True,
    )
    rows_html = "".join(
        f'<div class="batch-row">'
        f'<span style="flex:2;">{html.escape(d["filename"][:45])}</span>'
        f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:0.7rem;'
        f'color:var(--success);">● ready</span>'
        f'</div>'
        for d in ready_docs if d["filename"] in selected
    )
    if rows_html:
        st.markdown(
            f'<div style="border:1px solid var(--border);border-radius:10px;'
            f'overflow:hidden;">{rows_html}</div>',
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