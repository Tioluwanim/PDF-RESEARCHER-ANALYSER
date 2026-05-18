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
from app.db.repository import init_db
from app.utils.logger import get_logger, log_startup

from app.ui import (
    render_sidebar,
    render_doc_header,
    render_empty_state,
    render_chat_tab,
    render_sections_tab,
    render_search_tab,
    render_info_tab,
    render_library_search_tab,
    render_batch_tab,
    render_export_tab,
)

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
@import url('https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,300;0,9..144,600;0,9..144,700;1,9..144,400&family=Plus+Jakarta+Sans:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

/* ═══════════════════════════════════════════════════════════════
   DESIGN TOKENS — Warm Scholarly · Human · Refined
═══════════════════════════════════════════════════════════════ */
:root {
    /* Surface palette */
    --canvas:       #f6f2eb;
    --paper:        #faf8f3;
    --card:         #ffffff;
    --card-hover:   #fefcf9;
    --surface:      #f0ebe0;
    --surface-2:    #e8e0d0;
    --surface-3:    #ddd3be;

    /* Ink */
    --ink:          #1c1814;
    --ink-2:        #3b342b;
    --ink-3:        #5a5248;
    --muted:        #8c8278;
    --muted-2:      #b8b0a6;
    --ghost:        #cec6bc;

    /* Accent — terracotta */
    --accent:       #c03b15;
    --accent-deep:  #9e2f0f;
    --accent-warm:  #e85c28;
    --accent-soft:  rgba(192,59,21,0.12);
    --accent-glow:  rgba(192,59,21,0.20);

    /* Gold */
    --gold:         #a8762a;
    --gold-light:   #f5e6c0;
    --gold-border:  #dfc87a;

    /* Semantic */
    --success:      #2d6b4a;
    --success-bg:   #e6f4ed;
    --success-bd:   #a8d9be;
    --warn:         #b56b1a;
    --warn-bg:      #fef4e2;
    --warn-bd:      #f0c97a;
    --info:         #1a5fa8;
    --info-bg:      #e6f0fa;

    /* Borders */
    --border:       #ddd3c0;
    --border-2:     #cfc4ae;
    --border-3:     #c0b49a;

    /* Sidebar — deep charcoal-brown */
    --sb-bg:        #111009;
    --sb-bg-2:      #1a1812;
    --sb-bg-3:      #222018;
    --sb-bg-4:      #2c291f;
    --sb-txt:       #d0c9bd;
    --sb-muted:     #7a7268;
    --sb-dim:       #3a3730;

    /* Radii */
    --r-xs:  4px;
    --r-sm:  8px;
    --r-md:  12px;
    --r-lg:  18px;
    --r-xl:  24px;
    --r-2xl: 32px;

    /* Shadows */
    --sh-xs:     0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
    --sh-sm:     0 2px 8px rgba(0,0,0,0.08), 0 1px 3px rgba(0,0,0,0.05);
    --sh-md:     0 4px 20px rgba(0,0,0,0.10), 0 2px 8px rgba(0,0,0,0.06);
    --sh-lg:     0 8px 40px rgba(0,0,0,0.13), 0 3px 12px rgba(0,0,0,0.07);
    --sh-accent: 0 4px 20px rgba(192,59,21,0.30), 0 1px 6px rgba(192,59,21,0.15);
    --sh-inset:  inset 0 1px 3px rgba(0,0,0,0.08);

    /* Typography */
    --f-display: 'Fraunces', Georgia, serif;
    --f-body:    'Plus Jakarta Sans', system-ui, sans-serif;
    --f-mono:    'JetBrains Mono', 'Courier New', monospace;
}

/* ═══════════════════════════════════════════════════════════════
   BASE RESET
═══════════════════════════════════════════════════════════════ */
html, body, [class*="css"] {
    font-family: var(--f-body) !important;
    background:  var(--canvas) !important;
    color:       var(--ink) !important;
    -webkit-font-smoothing: antialiased !important;
    text-rendering: optimizeLegibility !important;
}

/* Subtle canvas texture */
body::before {
    content: '';
    position: fixed;
    inset: 0;
    background-image:
        radial-gradient(ellipse 80% 50% at 20% 0%, rgba(192,59,21,0.04) 0%, transparent 60%),
        radial-gradient(ellipse 60% 40% at 80% 100%, rgba(168,118,42,0.04) 0%, transparent 60%);
    pointer-events: none;
    z-index: 0;
}

/* ═══════════════════════════════════════════════════════════════
   CHROME REMOVAL
═══════════════════════════════════════════════════════════════ */
#MainMenu, footer, .stDeployButton,
[data-testid="stToolbar"],
[data-testid="stDecoration"],
[data-testid="stStatusWidget"] { display: none !important; }
header[data-testid="stHeader"]  { background: transparent !important; height: 0 !important; }

/* Sidebar toggle */
[data-testid="collapsedControl"] {
    visibility:     visible !important;
    display:        flex    !important;
    opacity:        1       !important;
    z-index:        99999   !important;
    background:     var(--accent) !important;
    border-radius:  0 var(--r-sm) var(--r-sm) 0 !important;
    width:          28px    !important;
    color:          white   !important;
    box-shadow:     var(--sh-sm) !important;
    transition:     all 0.2s ease !important;
}
[data-testid="collapsedControl"]:hover {
    background: var(--accent-warm) !important;
    width: 32px !important;
}
[data-testid="collapsedControl"] * {
    visibility:     visible !important;
    pointer-events: auto    !important;
    color:          white   !important;
}

/* ═══════════════════════════════════════════════════════════════
   SIDEBAR
═══════════════════════════════════════════════════════════════ */
section[data-testid="stSidebar"] {
    background:   var(--sb-bg) !important;
    border-right: 1px solid rgba(255,255,255,0.04) !important;
    box-shadow:   4px 0 24px rgba(0,0,0,0.25) !important;
}
section[data-testid="stSidebar"] > div {
    padding-top: 0 !important;
}
section[data-testid="stSidebar"] * {
    color: var(--sb-txt) !important;
}

/* Sidebar doc buttons */
section[data-testid="stSidebar"] .stButton > button {
    background:     transparent;
    border:         1px solid var(--sb-dim);
    color:          var(--sb-txt) !important;
    border-radius:  var(--r-md);
    font-family:    var(--f-body);
    font-size:      0.77rem;
    font-weight:    400;
    padding:        0.48rem 0.9rem;
    transition:     all 0.22s cubic-bezier(0.4,0,0.2,1);
    width:          100%;
    text-align:     left;
    letter-spacing: 0.01em;
    margin-bottom:  2px;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background:    var(--sb-bg-3);
    border-color:  var(--accent);
    color:         #f5f0e8 !important;
    transform:     translateX(3px);
    box-shadow:    0 0 12px rgba(192,59,21,0.15);
}
section[data-testid="stSidebar"] .stButton > button[kind="primary"] {
    background:    linear-gradient(135deg, var(--accent), var(--accent-warm)) !important;
    border-color:  transparent !important;
    color:         white !important;
    font-weight:   600 !important;
    box-shadow:    var(--sh-accent) !important;
}
section[data-testid="stSidebar"] .stButton > button[kind="primary"]:hover {
    filter: brightness(1.1);
    transform: translateX(2px);
}

/* ── Sidebar Logo ─────────────────────────────────────────────────────────── */
.sidebar-logo {
    padding: 1.5rem 1rem 1.25rem;
    border-bottom: 1px solid var(--sb-dim);
    margin-bottom: 1.5rem;
    background: linear-gradient(180deg, var(--sb-bg-2) 0%, var(--sb-bg) 100%);
}
.sidebar-logo-mark {
    width: 42px; height: 42px;
    background: linear-gradient(135deg, var(--accent) 0%, var(--accent-warm) 100%);
    border-radius: var(--r-md);
    display: flex; align-items: center; justify-content: center;
    font-size: 1.2rem;
    margin-bottom: 0.85rem;
    box-shadow: var(--sh-accent);
    position: relative;
    overflow: hidden;
}
.sidebar-logo-mark::after {
    content: '';
    position: absolute;
    top: -50%; left: -50%;
    width: 200%; height: 200%;
    background: linear-gradient(45deg, transparent 30%, rgba(255,255,255,0.15) 50%, transparent 70%);
    animation: shimmer 3s ease-in-out infinite;
}
.sidebar-app-name {
    font-family:    var(--f-display) !important;
    font-size:      1.15rem !important;
    font-weight:    600 !important;
    color:          #f0ece4 !important;
    letter-spacing: -0.02em !important;
    line-height:    1.2 !important;
}
.sidebar-app-sub {
    font-family:    var(--f-mono) !important;
    font-size:      0.58rem !important;
    color:          var(--sb-muted) !important;
    letter-spacing: 0.15em !important;
    text-transform: uppercase !important;
    margin-top:     0.25rem !important;
}

/* ── Sidebar Section Labels ───────────────────────────────────────────────── */
.sb-label {
    font-family:    var(--f-mono) !important;
    font-size:      0.6rem !important;
    color:          var(--sb-muted) !important;
    text-transform: uppercase !important;
    letter-spacing: 0.12em !important;
    padding:        0 0.1rem 0.45rem !important;
    border-bottom:  1px solid var(--sb-dim) !important;
    margin-bottom:  0.5rem !important;
    display:        block !important;
}

/* ── Sidebar Provider Status ──────────────────────────────────────────────── */
.provider-row {
    display:         flex;
    justify-content: space-between;
    align-items:     center;
    padding:         0.35rem 0.5rem;
    border-radius:   var(--r-sm);
    margin-bottom:   3px;
    transition:      background 0.15s;
}
.provider-row:hover { background: var(--sb-bg-2); }
.provider-name {
    font-family: var(--f-mono);
    font-size:   0.67rem;
    color:       var(--sb-muted) !important;
    letter-spacing: 0.06em;
}
.provider-model {
    font-family:  var(--f-mono);
    font-size:    0.63rem;
    color:        var(--sb-bg-4) !important;
    background:   var(--sb-bg-2);
    padding:      0.1rem 0.4rem;
    border-radius: 4px;
    max-width:    120px;
    overflow:     hidden;
    text-overflow: ellipsis;
    white-space:  nowrap;
}
.provider-dot-on  { color: #4ade80 !important; }
.provider-dot-off { color: #f87171 !important; }

/* ═══════════════════════════════════════════════════════════════
   APP HEADER
═══════════════════════════════════════════════════════════════ */
.app-header {
    display:         flex;
    align-items:     flex-end;
    justify-content: space-between;
    padding:         1rem 0 1.5rem;
    border-bottom:   2px solid var(--surface-2);
    margin-bottom:   1.75rem;
    animation:       fadeUp 0.5s ease both;
}
.app-title {
    font-family:    var(--f-display);
    font-size:      2.4rem;
    font-weight:    700;
    letter-spacing: -0.04em;
    line-height:    1;
    color:          var(--ink);
}
.app-title span { color: var(--accent); font-style: italic; }
.app-subtitle {
    font-family:    var(--f-mono);
    font-size:      0.63rem;
    color:          var(--muted);
    letter-spacing: 0.14em;
    text-transform: uppercase;
    margin-top:     0.5rem;
}
.app-badge {
    font-family:    var(--f-mono);
    font-size:      0.6rem;
    background:     var(--gold-light);
    color:          var(--gold);
    border:         1px solid var(--gold-border);
    padding:        0.22rem 0.75rem;
    border-radius:  20px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    font-weight:    600;
    box-shadow:     0 1px 4px rgba(168,118,42,0.15);
}

/* ═══════════════════════════════════════════════════════════════
   MODE SELECTOR (radio pills)
═══════════════════════════════════════════════════════════════ */
.stRadio [data-baseweb="radio-group"] {
    display:     flex !important;
    gap:         0.5rem !important;
    flex-wrap:   wrap;
    padding:     0.35rem !important;
    background:  var(--surface) !important;
    border:      1.5px solid var(--border) !important;
    border-radius: var(--r-xl) !important;
    width:       fit-content !important;
}
.stRadio label {
    background:    transparent !important;
    border:        none !important;
    border-radius: var(--r-lg) !important;
    padding:       0.45rem 1.2rem !important;
    font-family:   var(--f-body) !important;
    font-size:     0.83rem !important;
    font-weight:   500 !important;
    cursor:        pointer !important;
    transition:    all 0.22s cubic-bezier(0.4,0,0.2,1) !important;
    color:         var(--ink-3) !important;
    white-space:   nowrap !important;
}
.stRadio label:hover {
    background: var(--surface-2) !important;
    color:      var(--ink) !important;
}
.stRadio label[data-baseweb] > div:first-child { display: none !important; }
[data-testid="stRadio"] label:has(input:checked),
.stRadio label:has(input:checked) {
    background: var(--card) !important;
    color:      var(--accent) !important;
    font-weight: 700 !important;
    box-shadow: var(--sh-sm) !important;
}

/* ═══════════════════════════════════════════════════════════════
   STAT CARDS
═══════════════════════════════════════════════════════════════ */
.stat-card {
    background:    var(--card);
    border:        1.5px solid var(--border);
    border-radius: var(--r-lg);
    padding:       1rem 1.1rem;
    text-align:    center;
    transition:    all 0.25s cubic-bezier(0.4,0,0.2,1);
    position:      relative;
    overflow:      hidden;
    cursor:        default;
}
.stat-card::before {
    content: '';
    position: absolute; top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(90deg, var(--accent), var(--accent-warm));
    transform: scaleX(0);
    transform-origin: left;
    transition: transform 0.3s cubic-bezier(0.4,0,0.2,1);
    border-radius: var(--r-lg) var(--r-lg) 0 0;
}
.stat-card::after {
    content: '';
    position: absolute;
    bottom: -30px; right: -30px;
    width: 80px; height: 80px;
    background: radial-gradient(circle, var(--accent-soft) 0%, transparent 70%);
    pointer-events: none;
    transition: all 0.3s ease;
}
.stat-card:hover {
    border-color:  var(--border-2);
    box-shadow:    var(--sh-md);
    transform:     translateY(-3px);
    background:    var(--card-hover);
}
.stat-card:hover::before { transform: scaleX(1); }
.stat-card:hover::after  { bottom: -10px; right: -10px; }
.stat-label {
    font-family:    var(--f-mono);
    font-size:      0.59rem;
    color:          var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.11em;
    margin-bottom:  0.4rem;
}
.stat-value {
    font-family: var(--f-display);
    font-size:   1.4rem;
    font-weight: 700;
    color:       var(--ink);
    line-height: 1;
    letter-spacing: -0.02em;
}

/* ═══════════════════════════════════════════════════════════════
   DOCUMENT HEADER CARD
═══════════════════════════════════════════════════════════════ */
.doc-header-card {
    background:    var(--card);
    border:        1.5px solid var(--border);
    border-radius: var(--r-xl);
    padding:       1.5rem 1.75rem;
    margin-bottom: 1.25rem;
    position:      relative;
    overflow:      hidden;
    animation:     fadeUp 0.4s ease both;
    box-shadow:    var(--sh-sm);
}
.doc-header-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 4px;
    background: linear-gradient(90deg, var(--accent) 0%, var(--accent-warm) 50%, var(--gold) 100%);
    border-radius: var(--r-xl) var(--r-xl) 0 0;
}
.doc-header-card::after {
    content: '';
    position: absolute;
    top: -40px; right: -40px;
    width: 160px; height: 160px;
    background: radial-gradient(circle, var(--accent-soft) 0%, transparent 65%);
    pointer-events: none;
}
.doc-title {
    font-family:    var(--f-display);
    font-size:      1.3rem;
    font-weight:    600;
    line-height:    1.4;
    color:          var(--ink);
    margin-bottom:  0.4rem;
    letter-spacing: -0.02em;
}
.doc-authors {
    font-size:  0.82rem;
    color:      var(--muted);
    font-style: italic;
    font-family: var(--f-body);
    line-height: 1.5;
}
.doc-journal-pill {
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 20px;
    padding: 0.2rem 0.75rem;
    font-size: 0.68rem;
    color: var(--muted);
    font-family: var(--f-mono);
    margin-top: 0.5rem;
    letter-spacing: 0.04em;
}
.ocr-badge {
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
    background: var(--warn-bg);
    border: 1px solid var(--warn-bd);
    color: var(--warn);
    font-size: 0.65rem;
    font-family: var(--f-mono);
    padding: 0.18rem 0.6rem;
    border-radius: 20px;
    margin-left: 0.5rem;
    letter-spacing: 0.05em;
    font-weight: 500;
}

/* ═══════════════════════════════════════════════════════════════
   TABS
═══════════════════════════════════════════════════════════════ */
.stTabs [data-baseweb="tab-list"] {
    background:    var(--surface) !important;
    border-radius: var(--r-xl)   !important;
    padding:       0.3rem        !important;
    gap:           0.2rem        !important;
    border:        1.5px solid var(--border) !important;
    box-shadow:    var(--sh-inset) !important;
}
.stTabs [data-baseweb="tab"] {
    font-family:    var(--f-body)   !important;
    font-size:      0.81rem         !important;
    font-weight:    500             !important;
    letter-spacing: 0.005em         !important;
    border-radius:  var(--r-lg)     !important;
    padding:        0.45rem 1.1rem  !important;
    color:          var(--ink-3)    !important;
    transition:     all 0.2s cubic-bezier(0.4,0,0.2,1) !important;
    border:         none            !important;
}
.stTabs [data-baseweb="tab"]:hover {
    color:      var(--ink) !important;
    background: var(--surface-2) !important;
}
.stTabs [aria-selected="true"] {
    background:  var(--card)   !important;
    color:       var(--accent) !important;
    box-shadow:  var(--sh-sm)  !important;
    font-weight: 700           !important;
}
.stTabs [data-baseweb="tab-highlight"] { display: none !important; }
.stTabs [data-baseweb="tab-border"]    { display: none !important; }

/* ═══════════════════════════════════════════════════════════════
   CHAT
═══════════════════════════════════════════════════════════════ */
.chat-container {
    max-height:     560px;
    overflow-y:     auto;
    padding:        1.25rem 0.25rem;
    scroll-behavior: smooth;
    scrollbar-width: thin;
    scrollbar-color: var(--border) transparent;
}
.msg-wrap {
    display:       flex;
    align-items:   flex-end;
    margin-bottom: 1.1rem;
    animation:     msgIn 0.28s cubic-bezier(0.4,0,0.2,1);
    gap:           0.65rem;
}
.msg-wrap.user { flex-direction: row-reverse; }
.msg-wrap.asst { flex-direction: row; }

.msg-avatar {
    width: 32px; height: 32px;
    border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 0.78rem;
    flex-shrink: 0;
    font-weight: 700;
    box-shadow: var(--sh-xs);
    transition: transform 0.2s ease;
}
.msg-avatar:hover { transform: scale(1.08); }
.msg-avatar.user {
    background: var(--ink-2);
    color: #f0ece4;
    font-size: 0.65rem;
    letter-spacing: 0.02em;
}
.msg-avatar.asst {
    background: linear-gradient(135deg, var(--accent), var(--accent-warm));
    color: white;
    font-size: 0.9rem;
}

.msg-bubble {
    max-width:   78%;
    padding:     0.9rem 1.15rem;
    font-size:   0.875rem;
    line-height: 1.75;
    position:    relative;
    animation:   fadeUp 0.2s ease;
}
.msg-bubble.user {
    background:    var(--ink-2);
    color:         #ece8e0;
    border-radius: var(--r-xl) var(--r-xl) var(--r-xs) var(--r-xl);
    box-shadow:    0 2px 12px rgba(0,0,0,0.18);
}
.msg-bubble.user::after {
    content: '';
    position: absolute;
    bottom: 0; right: -6px;
    width: 12px; height: 12px;
    background: var(--ink-2);
    clip-path: polygon(0 0, 0 100%, 100% 100%);
}
.msg-bubble.asst {
    background:    var(--card);
    color:         var(--ink);
    border:        1.5px solid var(--border);
    border-radius: var(--r-xl) var(--r-xl) var(--r-xl) var(--r-xs);
    box-shadow:    var(--sh-sm);
    border-left:   3px solid var(--accent) !important;
}
.msg-bubble.asst::after {
    content: '';
    position: absolute;
    bottom: -1px; left: -7px;
    width: 12px; height: 12px;
    background: var(--card);
    border-bottom: 1.5px solid var(--border);
    border-left: 1.5px solid var(--border);
    clip-path: polygon(0 0, 100% 100%, 100% 0);
}
.msg-bubble.asst p  { margin: 0 0 0.6rem; }
.msg-bubble.asst p:last-child { margin-bottom: 0; }
.msg-bubble.asst code {
    background:   var(--surface);
    padding:      0.1em 0.35em;
    border-radius: var(--r-xs);
    font-family:  var(--f-mono);
    font-size:    0.82em;
    color:        var(--accent);
    border:       1px solid var(--border);
}
.msg-bubble.asst ul, .msg-bubble.asst ol {
    margin: 0.4rem 0 0.6rem 1.2rem;
}
.msg-bubble.asst li { margin-bottom: 0.3rem; }
.msg-bubble.asst strong { font-weight: 700; color: var(--ink); }

/* Message timestamp */
.msg-ts {
    font-family: var(--f-mono);
    font-size:   0.58rem;
    color:       var(--muted-2);
    margin-top:  0.25rem;
    opacity:     0;
    transition:  opacity 0.2s ease;
    text-align:  center;
}
.msg-wrap:hover .msg-ts { opacity: 1; }

/* ── Typing indicator ─────────────────────────────────────────────────────── */
.typing-indicator {
    display:       flex;
    gap:           5px;
    align-items:   center;
    padding:       0.85rem 1.15rem;
    background:    var(--card);
    border:        1.5px solid var(--border);
    border-left:   3px solid var(--accent);
    border-radius: var(--r-xl) var(--r-xl) var(--r-xl) var(--r-xs);
    width:         fit-content;
    box-shadow:    var(--sh-sm);
    animation:     fadeUp 0.2s ease;
}
.typing-dot {
    width: 7px; height: 7px;
    background: var(--muted-2);
    border-radius: 50%;
    animation: bounce 1.5s ease-in-out infinite;
}
.typing-dot:nth-child(2) { animation-delay: 0.2s; }
.typing-dot:nth-child(3) { animation-delay: 0.4s; }

/* ── Chat input ───────────────────────────────────────────────────────────── */
.chat-input-bar {
    display:       flex;
    align-items:   center;
    gap:           0.5rem;
    background:    var(--card);
    border:        1.5px solid var(--border);
    border-radius: var(--r-2xl);
    padding:       0.45rem 0.5rem 0.45rem 1.1rem;
    box-shadow:    var(--sh-sm);
    margin-top:    0.75rem;
    transition:    border-color 0.2s, box-shadow 0.25s cubic-bezier(0.4,0,0.2,1);
}
.chat-input-bar:focus-within {
    border-color: var(--accent);
    box-shadow:   0 0 0 4px var(--accent-glow), var(--sh-sm);
}

/* ═══════════════════════════════════════════════════════════════
   INPUTS & FORMS
═══════════════════════════════════════════════════════════════ */
.stTextInput > div > div > input,
.stTextArea  > div > div > textarea {
    border:        1.5px solid var(--border) !important;
    border-radius: var(--r-md) !important;
    font-family:   var(--f-body) !important;
    background:    var(--card) !important;
    font-size:     0.9rem !important;
    color:         var(--ink) !important;
    transition:    border-color 0.2s, box-shadow 0.25s ease !important;
    box-shadow:    var(--sh-inset) !important;
}
.stTextInput > div > div > input:focus,
.stTextArea  > div > div > textarea:focus {
    border-color: var(--accent)  !important;
    box-shadow:   0 0 0 4px var(--accent-glow) !important;
    outline:      none !important;
}

/* ═══════════════════════════════════════════════════════════════
   BUTTONS
═══════════════════════════════════════════════════════════════ */
.stButton > button {
    font-family:    var(--f-body);
    font-weight:    500;
    border-radius:  var(--r-md);
    transition:     all 0.22s cubic-bezier(0.4,0,0.2,1);
    letter-spacing: 0.01em;
    border:         1.5px solid var(--border) !important;
}
.stButton > button[kind="primary"] {
    background:  linear-gradient(135deg, var(--accent) 0%, var(--accent-warm) 100%) !important;
    border:      none !important;
    color:       white !important;
    font-weight: 600  !important;
    box-shadow:  var(--sh-accent) !important;
    position:    relative !important;
    overflow:    hidden !important;
}
.stButton > button[kind="primary"]::after {
    content:    '';
    position:   absolute;
    top: -50%; left: -60%;
    width:      60%;
    height:     200%;
    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.18), transparent);
    transform:  skewX(-20deg);
    transition: left 0.5s ease;
}
.stButton > button[kind="primary"]:hover::after { left: 110%; }
.stButton > button[kind="primary"]:hover {
    transform:  translateY(-2px);
    box-shadow: 0 6px 28px rgba(192,59,21,0.40) !important;
    filter:     brightness(1.06);
}
.stButton > button[kind="primary"]:active {
    transform:  translateY(0);
    box-shadow: var(--sh-xs) !important;
}
.stButton > button:not([kind="primary"]):hover {
    border-color: var(--accent) !important;
    color:        var(--accent) !important;
    background:   var(--accent-soft) !important;
    transform:    translateY(-1px);
}

/* ═══════════════════════════════════════════════════════════════
   SECTION CONTENT BLOCK
═══════════════════════════════════════════════════════════════ */
.section-block {
    background:    var(--card);
    border:        1.5px solid var(--border);
    border-radius: var(--r-md);
    border-left:   4px solid var(--accent) !important;
    padding:       1.35rem 1.6rem;
    font-size:     0.875rem;
    line-height:   1.9;
    white-space:   pre-wrap;
    color:         var(--ink-2);
    box-shadow:    var(--sh-xs);
    animation:     fadeUp 0.3s ease both;
}

/* ═══════════════════════════════════════════════════════════════
   RESULT CARDS (semantic search)
═══════════════════════════════════════════════════════════════ */
.result-card {
    background:    var(--card);
    border:        1.5px solid var(--border);
    border-radius: var(--r-lg);
    padding:       1.1rem 1.35rem;
    margin:        0.55rem 0;
    transition:    all 0.25s cubic-bezier(0.4,0,0.2,1);
    position:      relative;
    overflow:      hidden;
    animation:     fadeUp 0.3s ease both;
}
.result-card::before {
    content:    '';
    position:   absolute;
    left: 0; top: 0; bottom: 0;
    width:      4px;
    background: linear-gradient(180deg, var(--accent), var(--accent-warm));
    opacity:    0;
    transition: opacity 0.2s ease;
    border-radius: var(--r-lg) 0 0 var(--r-lg);
}
.result-card:hover {
    border-color: var(--border-2);
    box-shadow:   var(--sh-md);
    transform:    translateY(-2px) translateX(2px);
    background:   var(--card-hover);
}
.result-card:hover::before { opacity: 1; }
.result-section-tag {
    font-family:    var(--f-mono);
    font-size:      0.62rem;
    color:          var(--accent);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    background:     var(--accent-soft);
    padding:        0.15rem 0.55rem;
    border-radius:  20px;
    display:        inline-block;
    font-weight:    600;
}
.result-score {
    font-family:  var(--f-mono);
    font-size:    0.63rem;
    color:        var(--success);
    background:   var(--success-bg);
    border:       1px solid var(--success-bd);
    padding:      0.12rem 0.55rem;
    border-radius: 20px;
    display:      inline-block;
    font-weight:  500;
}
.result-text {
    font-size:   0.86rem;
    line-height: 1.75;
    color:       var(--ink-2);
    margin-top:  0.6rem;
}

/* ═══════════════════════════════════════════════════════════════
   META BLOCKS (info tab)
═══════════════════════════════════════════════════════════════ */
.meta-block {
    background:    var(--card);
    border:        1.5px solid var(--border);
    border-radius: var(--r-md);
    padding:       0.95rem 1.15rem;
    margin-bottom: 0.65rem;
    transition:    all 0.2s ease;
    animation:     fadeUp 0.3s ease both;
}
.meta-block:hover {
    border-color: var(--border-2);
    box-shadow:   var(--sh-xs);
    transform:    translateX(2px);
}
.meta-label {
    font-family:    var(--f-mono);
    font-size:      0.59rem;
    color:          var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.11em;
    margin-bottom:  0.3rem;
    display:        flex;
    align-items:    center;
    gap:            0.35rem;
}
.meta-label::before {
    content: '';
    display: inline-block;
    width: 3px; height: 3px;
    background: var(--accent);
    border-radius: 50%;
    flex-shrink: 0;
}
.meta-value    { font-size: 0.92rem; font-weight: 500; color: var(--ink); line-height: 1.45; }
.meta-value-lg { font-family: var(--f-display); font-size: 1.3rem; font-weight: 700; color: var(--ink); letter-spacing: -0.02em; }

/* ═══════════════════════════════════════════════════════════════
   SECTION / BATCH ROWS
═══════════════════════════════════════════════════════════════ */
.section-row {
    display:         flex;
    justify-content: space-between;
    align-items:     center;
    padding:         0.65rem 0.9rem;
    border-bottom:   1px solid var(--surface-2);
    font-size:       0.84rem;
    transition:      all 0.15s ease;
    cursor:          default;
}
.section-row:hover { background: var(--surface); border-radius: var(--r-sm); }
.section-row:last-child { border-bottom: none; }

.batch-row {
    display:     flex;
    gap:         1rem;
    align-items: center;
    padding:     0.55rem 0.9rem;
    border-bottom: 1px solid var(--surface-2);
    font-size:   0.83rem;
    transition:  all 0.15s ease;
}
.batch-row:hover      { background: var(--surface); border-radius: var(--r-sm); }
.batch-row:last-child { border-bottom: none; }

/* ═══════════════════════════════════════════════════════════════
   EMPTY STATE
═══════════════════════════════════════════════════════════════ */
.empty-state {
    display:         flex;
    flex-direction:  column;
    align-items:     center;
    justify-content: center;
    padding:         4.5rem 2rem;
    text-align:      center;
    animation:       fadeUp 0.6s cubic-bezier(0.4,0,0.2,1) both;
}
.empty-icon-ring {
    width: 90px; height: 90px;
    border-radius: 50%;
    background: linear-gradient(135deg, var(--surface-2), var(--surface));
    border: 2px solid var(--border);
    display: flex; align-items: center; justify-content: center;
    font-size: 2.2rem;
    margin-bottom: 1.75rem;
    position: relative;
    box-shadow: var(--sh-md);
}
.empty-icon-ring::before {
    content: '';
    position: absolute;
    inset: -8px;
    border-radius: 50%;
    border: 1.5px dashed var(--border-2);
    animation: spin 18s linear infinite;
}
.empty-icon-ring::after {
    content: '';
    position: absolute;
    inset: -20px;
    border-radius: 50%;
    border: 1px dashed var(--ghost);
    animation: spin 30s linear infinite reverse;
}
.empty-title {
    font-family:    var(--f-display);
    font-size:      1.9rem;
    font-weight:    700;
    letter-spacing: -0.03em;
    margin-bottom:  0.65rem;
    color:          var(--ink);
    line-height:    1.15;
}
.empty-sub {
    font-size:     0.88rem;
    color:         var(--muted);
    max-width:     420px;
    line-height:   1.75;
    margin-bottom: 2.75rem;
}
.feature-chip {
    background:    var(--card);
    border:        1.5px solid var(--border);
    border-radius: var(--r-lg);
    padding:       1rem 1.4rem;
    font-size:     0.79rem;
    color:         var(--ink-2);
    min-width:     130px;
    text-align:    center;
    transition:    all 0.25s cubic-bezier(0.4,0,0.2,1);
    box-shadow:    var(--sh-xs);
    cursor:        default;
}
.feature-chip:hover {
    border-color: var(--accent);
    transform:    translateY(-4px);
    box-shadow:   var(--sh-md);
    color:        var(--accent);
    background:   var(--card-hover);
}
.feature-chip-icon  { font-size: 1.7rem; margin-bottom: 0.5rem; }
.feature-chip-label { font-weight: 600; letter-spacing: 0.01em; }

/* ═══════════════════════════════════════════════════════════════
   EXPORT CARDS
═══════════════════════════════════════════════════════════════ */
.export-card {
    background:    var(--card);
    border:        1.5px solid var(--border);
    border-radius: var(--r-xl);
    padding:       1.35rem 1.25rem;
    text-align:    center;
    transition:    all 0.25s cubic-bezier(0.4,0,0.2,1);
    height:        100%;
    position:      relative;
    overflow:      hidden;
    box-shadow:    var(--sh-xs);
}
.export-card::before {
    content:    '';
    position:   absolute;
    bottom: -40px; right: -40px;
    width:      120px; height: 120px;
    border-radius: 50%;
    background: var(--accent-soft);
    transition: all 0.3s ease;
}
.export-card:hover {
    border-color: var(--accent);
    box-shadow:   var(--sh-lg);
    transform:    translateY(-4px);
}
.export-card:hover::before {
    bottom: -20px; right: -20px;
    background: rgba(192,59,21,0.08);
}
.export-card-icon  {
    font-size:     2.2rem;
    margin-bottom: 0.6rem;
    display:       block;
    transition:    transform 0.3s cubic-bezier(0.4,0,0.2,1);
}
.export-card:hover .export-card-icon { transform: scale(1.15) rotate(-5deg); }
.export-card-title {
    font-family:  var(--f-display);
    font-size:    1.05rem;
    font-weight:  700;
    color:        var(--ink);
    margin-bottom: 0.3rem;
    letter-spacing: -0.01em;
}
.export-card-sub {
    font-size:   0.75rem;
    color:       var(--muted);
    margin-bottom: 1.1rem;
    line-height: 1.5;
}

/* ═══════════════════════════════════════════════════════════════
   PROGRESS BAR
═══════════════════════════════════════════════════════════════ */
.stProgress > div > div {
    background: linear-gradient(90deg,
        var(--accent) 0%,
        var(--accent-warm) 50%,
        var(--gold) 100%) !important;
    border-radius: 4px !important;
    box-shadow:    0 1px 6px rgba(192,59,21,0.35) !important;
    transition:    width 0.4s cubic-bezier(0.4,0,0.2,1) !important;
}
.stProgress > div {
    background:    var(--surface-2) !important;
    border-radius: 4px !important;
    overflow:      hidden !important;
}

/* ═══════════════════════════════════════════════════════════════
   ALERTS
═══════════════════════════════════════════════════════════════ */
.stAlert {
    border-radius: var(--r-lg) !important;
    border:        1.5px solid transparent !important;
    animation:     fadeUp 0.3s ease !important;
}

/* ═══════════════════════════════════════════════════════════════
   SELECT / MULTISELECT
═══════════════════════════════════════════════════════════════ */
.stSelectbox > div > div,
.stMultiSelect > div {
    border:        1.5px solid var(--border) !important;
    border-radius: var(--r-md) !important;
    background:    var(--card) !important;
    transition:    border-color 0.2s, box-shadow 0.2s !important;
}
.stSelectbox > div > div:focus-within,
.stMultiSelect > div:focus-within {
    border-color: var(--accent) !important;
    box-shadow:   0 0 0 4px var(--accent-glow) !important;
}

/* ═══════════════════════════════════════════════════════════════
   FILE UPLOADER
═══════════════════════════════════════════════════════════════ */
[data-testid="stFileUploader"] > div {
    border:        2px dashed var(--border-2) !important;
    border-radius: var(--r-xl) !important;
    background:    var(--surface) !important;
    transition:    all 0.25s cubic-bezier(0.4,0,0.2,1) !important;
    position:      relative !important;
}
[data-testid="stFileUploader"] > div:hover {
    border-color: var(--accent) !important;
    background:   var(--surface-2) !important;
    box-shadow:   0 0 0 4px var(--accent-glow) !important;
    transform:    scale(1.005) !important;
}
[data-testid="stFileUploader"] label {
    font-family: var(--f-body) !important;
    color:       var(--ink-3) !important;
}

/* ═══════════════════════════════════════════════════════════════
   SCROLLBAR
═══════════════════════════════════════════════════════════════ */
::-webkit-scrollbar       { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb {
    background:    var(--border);
    border-radius: 6px;
    transition:    background 0.2s;
}
::-webkit-scrollbar-thumb:hover { background: var(--border-2); }

hr {
    border: none;
    border-top: 1.5px solid var(--surface-2);
    margin: 1.35rem 0;
}

/* ═══════════════════════════════════════════════════════════════
   KEYWORD PILLS
═══════════════════════════════════════════════════════════════ */
.kw-pill {
    display:       inline-block;
    background:    var(--surface);
    border:        1px solid var(--border);
    border-radius: 20px;
    padding:       0.22rem 0.75rem;
    font-family:   var(--f-mono);
    font-size:     0.66rem;
    margin:        0.2rem 0.2rem 0 0;
    color:         var(--ink-3);
    transition:    all 0.18s ease;
    cursor:        default;
}
.kw-pill:hover {
    background:   var(--accent-soft);
    border-color: var(--accent);
    color:        var(--accent);
    transform:    translateY(-1px);
}

/* ═══════════════════════════════════════════════════════════════
   DIVIDER WITH LABEL
═══════════════════════════════════════════════════════════════ */
.divider-label {
    display:     flex;
    align-items: center;
    gap:         0.75rem;
    margin:      1.35rem 0;
    color:       var(--muted);
    font-family: var(--f-mono);
    font-size:   0.61rem;
    text-transform: uppercase;
    letter-spacing: 0.11em;
}
.divider-label::before,
.divider-label::after {
    content: ''; flex: 1;
    height: 1px;
    background: linear-gradient(90deg, transparent, var(--surface-2), transparent);
}

/* ═══════════════════════════════════════════════════════════════
   TOAST / SUCCESS PULSE
═══════════════════════════════════════════════════════════════ */
.success-pulse {
    background:    var(--success-bg);
    border:        1.5px solid var(--success-bd);
    border-radius: var(--r-lg);
    padding:       0.75rem 1.25rem;
    font-size:     0.875rem;
    color:         var(--success);
    display:       flex;
    align-items:   center;
    gap:           0.6rem;
    animation:     successPop 0.4s cubic-bezier(0.34,1.56,0.64,1) both;
}
.success-pulse .sp-icon {
    font-size: 1.1rem;
    animation: spin 0.5s ease both;
}

/* ═══════════════════════════════════════════════════════════════
   PROCESSING STATE
═══════════════════════════════════════════════════════════════ */
.processing-banner {
    background:    linear-gradient(135deg, var(--surface) 0%, var(--surface-2) 100%);
    border:        1.5px solid var(--border);
    border-left:   4px solid var(--accent) !important;
    border-radius: var(--r-lg);
    padding:       1rem 1.5rem;
    display:       flex;
    align-items:   center;
    gap:           1rem;
    animation:     fadeUp 0.3s ease;
}
.processing-spinner {
    width: 20px; height: 20px;
    border:        2.5px solid var(--border-2);
    border-top:    2.5px solid var(--accent);
    border-radius: 50%;
    animation:     spin 0.8s linear infinite;
    flex-shrink:   0;
}

/* ═══════════════════════════════════════════════════════════════
   TEMPLATE SELECTOR (Export)
═══════════════════════════════════════════════════════════════ */
.template-card {
    background:    var(--card);
    border:        2px solid var(--border);
    border-radius: var(--r-xl);
    padding:       1.25rem 1.5rem;
    cursor:        pointer;
    transition:    all 0.25s cubic-bezier(0.4,0,0.2,1);
    position:      relative;
    overflow:      hidden;
}
.template-card:hover {
    border-color:  var(--accent);
    box-shadow:    var(--sh-md);
    transform:     translateY(-2px);
}
.template-card.active {
    border-color:  var(--accent);
    background:    linear-gradient(135deg, #fff8f6 0%, #fff 100%);
    box-shadow:    var(--sh-accent);
}
.template-card-icon   { font-size: 1.8rem; margin-bottom: 0.5rem; }
.template-card-title  { font-family: var(--f-display); font-size: 1rem; font-weight: 700; color: var(--ink); }
.template-card-desc   { font-size: 0.74rem; color: var(--muted); margin-top: 0.25rem; line-height: 1.5; }

/* ═══════════════════════════════════════════════════════════════
   WATERMARK
═══════════════════════════════════════════════════════════════ */
.page-watermark {
    position:    fixed;
    bottom:      1.5rem;
    right:       1.5rem;
    font-family: var(--f-mono);
    font-size:   0.58rem;
    color:       var(--ghost);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    pointer-events: none;
    z-index:     0;
    user-select: none;
}

/* ═══════════════════════════════════════════════════════════════
   ANIMATIONS
═══════════════════════════════════════════════════════════════ */
@keyframes fadeUp {
    from { opacity: 0; transform: translateY(14px); }
    to   { opacity: 1; transform: translateY(0);    }
}
@keyframes msgIn {
    from { opacity: 0; transform: translateY(8px) scale(0.97); }
    to   { opacity: 1; transform: translateY(0)   scale(1);    }
}
@keyframes bounce {
    0%, 60%, 100% { transform: translateY(0);    opacity: 0.4; }
    30%            { transform: translateY(-6px); opacity: 1;   }
}
@keyframes spin {
    from { transform: rotate(0deg); }
    to   { transform: rotate(360deg); }
}
@keyframes shimmer {
    0%   { left: -60%; }
    100% { left: 120%; }
}
@keyframes successPop {
    from { opacity: 0; transform: scale(0.92); }
    to   { opacity: 1; transform: scale(1);    }
}
@keyframes pulse {
    0%, 100% { opacity: 1; }
    50%       { opacity: 0.5; }
}

/* ── Staggered animations for lists ──────────────────────────────────────── */
.stagger-1 { animation-delay: 0.05s; }
.stagger-2 { animation-delay: 0.10s; }
.stagger-3 { animation-delay: 0.15s; }
.stagger-4 { animation-delay: 0.20s; }

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
        try:
            init_db()
        except Exception as exc:
            warnings.append(f"Database startup failed: {exc}")
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
            'border-radius:6px;font-family:var(--f-mono);'
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
    render_sidebar()


# ── Main ──────────────────────────────────────────────────────────────────────
def _render_main() -> None:
    st.markdown(
        '<div class="app-header">'
        '  <div class="app-title-block">'
        '    <div class="app-title">PDF <span>Research</span> Analyzer</div>'
        '    <div class="app-subtitle">'
        '      Semantic Search · Section Detection · AI Chat · OCR · Multi-format Export'
        '    </div>'
        '  </div>'
        '  <div class="app-badge">✦ AI Powered</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    mode = st.radio(
        "mode",
        ["📄 Single PDF", "🔎 Library Search", "📚 Batch Upload", "📤 Export"],
        index=["📄 Single PDF", "🔎 Library Search", "📚 Batch Upload", "📤 Export"].index(
            st.session_state.get("app_mode", "📄 Single PDF")
        ),
        horizontal=True,
        label_visibility="collapsed",
        key="mode_radio",
    )
    if mode != st.session_state.get("app_mode"):
        st.session_state.app_mode = mode
        st.session_state.export_data = {}

    st.markdown("<hr>", unsafe_allow_html=True)

    if mode == "🔎 Library Search":
        render_library_search_tab()
        return
    if mode == "📚 Batch Upload":
        render_batch_tab()
        return
    if mode == "📤 Export":
        render_export_tab()
        return

    doc_id = st.session_state.active_doc_id
    if not doc_id:
        render_empty_state()
        return

    info = analysis_service.get_document_info(doc_id)
    if "error" in info:
        st.error(info["error"])
        st.session_state.active_doc_id = None
        return

    render_doc_header(info, info.get("chunks", {}))
    st.markdown("<hr>", unsafe_allow_html=True)

    tab_chat, tab_sections, tab_search, tab_info = st.tabs([
        "💬  Chat", "📑  Sections", "🔍  Search", "ℹ️  Info",
    ])
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
    _render_sidebar()
    _render_main()


if __name__ == "__main__":
    main()
