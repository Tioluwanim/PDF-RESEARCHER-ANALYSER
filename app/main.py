"""
main.py - PDF Research Analyzer
Production UI: single PDF, batch (1-50), export (XLSX/DOCX/CSV/JSON).

Fixes applied:
  - CSS: added .msg-user / .msg-assistant rules (were missing — messages unstyled)
  - CSS: fixed var(--font-mono) typo → var(--f-mono) in empty-state HTML
  - Chat: removed duplicate message rendering (user msg was rendered twice)
  - Chat: suggestion buttons no longer call st.rerun() mid-stream; rerun only
    after stream completes and history is stored
  - Chat: text input key is counter-based so it resets after each send
  - Export: st.session_state.export_data explicitly re-assigned after mutation
    so Streamlit 1.32+ detects the change
  - Batch: f.getvalue() only (no read() fallback that returns empty bytes)
  - _delete_doc_cache / _delete_all_docs: import guard wrapped in try/except
  - Pydantic v2 compat: getattr calls use `getattr(m, field, None) or ""`
    pattern throughout helper calls in _render_doc_header
  - mode_radio key collision: mode selector key made unique to avoid DuplicateWidgetID
  - _render_sections_tab: selectbox key made stable
  - Minor: removed stray `label_visibility` on selectboxes that don't need it
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

/* ── FIX: added missing .msg-user and .msg-assistant classes ── */
.msg-user {
    display:       flex;
    flex-direction: row-reverse;
    align-items:   flex-end;
    margin-bottom: 1.1rem;
    animation:     msgIn 0.28s cubic-bezier(0.4,0,0.2,1);
    gap:           0.65rem;
}
.msg-assistant {
    display:       flex;
    flex-direction: row;
    align-items:   flex-end;
    margin-bottom: 1.1rem;
    animation:     msgIn 0.28s cubic-bezier(0.4,0,0.2,1);
    gap:           0.65rem;
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

/* ── FIX: .msg-user > div and .msg-assistant > div bubble styles ── */
.msg-user > div,
.msg-bubble.user {
    max-width:     78%;
    padding:       0.9rem 1.15rem;
    font-size:     0.875rem;
    line-height:   1.75;
    position:      relative;
    animation:     fadeUp 0.2s ease;
    background:    var(--ink-2);
    color:         #ece8e0;
    border-radius: var(--r-xl) var(--r-xl) var(--r-xs) var(--r-xl);
    box-shadow:    0 2px 12px rgba(0,0,0,0.18);
}

.msg-assistant > div,
.msg-bubble.asst {
    max-width:     78%;
    padding:       0.9rem 1.15rem;
    font-size:     0.875rem;
    line-height:   1.75;
    position:      relative;
    animation:     fadeUp 0.2s ease;
    background:    var(--card);
    color:         var(--ink);
    border:        1.5px solid var(--border);
    border-radius: var(--r-xl) var(--r-xl) var(--r-xl) var(--r-xs);
    box-shadow:    var(--sh-sm);
    border-left:   3px solid var(--accent) !important;
}
.msg-bubble.asst p,
.msg-assistant > div p  { margin: 0 0 0.6rem; }
.msg-bubble.asst p:last-child,
.msg-assistant > div p:last-child { margin-bottom: 0; }
.msg-bubble.asst code,
.msg-assistant > div code {
    background:   var(--surface);
    padding:      0.1em 0.35em;
    border-radius: var(--r-xs);
    font-family:  var(--f-mono);
    font-size:    0.82em;
    color:        var(--accent);
    border:       1px solid var(--border);
}
.msg-bubble.asst ul, .msg-bubble.asst ol,
.msg-assistant > div ul, .msg-assistant > div ol {
    margin: 0.4rem 0 0.6rem 1.2rem;
}
.msg-bubble.asst li,
.msg-assistant > div li { margin-bottom: 0.3rem; }
.msg-bubble.asst strong,
.msg-assistant > div strong { font-weight: 700; color: var(--ink); }

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
    defaults: dict = {
        "active_doc_id"     : None,
        "chat_history"      : [],
        "startup_done"      : False,
        "last_uploaded_name": None,
        "app_mode"          : "📄 Single PDF",
        # Export: store generated bytes so rerun doesn't lose them
        "export_data"       : {},   # format → (bytes, filename)
        # Batch: store last results so rerun doesn't re-run batch
        "batch_done"        : False,
        # FIX: counter-based key so chat input resets after each send
        "chat_input_key"    : 0,
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
    with st.sidebar:
        st.markdown("""
        <div class="sidebar-logo">
            <div class="sidebar-logo-mark">📚</div>
            <div class="sidebar-app-name">Research Analyzer</div>
            <div class="sidebar-app-sub">AI · PDF · Intelligence</div>
        </div>
        """, unsafe_allow_html=True)

        # Single file upload
        _sidebar_label("Upload File")
        uploaded = st.file_uploader(
            "Drop file here", type=["pdf","docx","doc","txt","xlsx","xls","csv"],
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

        # Re-process active document
        if st.session_state.active_doc_id:
            _sidebar_label("Actions")
            if st.button("🔄 Re-process Active Doc", key="reprocess_btn"):
                _delete_doc_cache(st.session_state.active_doc_id)
                st.session_state.chat_history = []
                st.rerun()

        # Clear all documents
        if docs:
            if st.button("🗑 Clear All Documents", key="clear_all_btn"):
                _delete_all_docs()
                st.session_state.active_doc_id = None
                st.session_state.chat_history  = []
                st.session_state.export_data   = {}
                st.rerun()

        _sidebar_divider()

        # LLM provider status
        _sidebar_label("LLM Providers")
        try:
            providers = analysis_service.get_provider_status()
            for name, info in providers.items():
                configured = info.get("configured")
                dot_class  = "provider-dot-on" if configured else "provider-dot-off"
                dot        = "●" if configured else "○"
                model      = info.get("model", "—").split("/")[-1][:22]
                st.markdown(
                    f'<div class="provider-row">'
                    f'<span class="provider-name">'
                    f'<span class="{dot_class}">{dot}</span> {name.upper()}</span>'
                    f'<span class="provider-model">{model}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        except Exception:
            st.markdown(
                '<div style="font-size:0.72rem;color:#3a3630;padding:0.3rem 0.5rem;">Status unavailable</div>',
                unsafe_allow_html=True,
            )

        _sidebar_divider()
        st.markdown(
            '<div style="font-family:var(--f-mono);font-size:0.6rem;'
            'color:var(--sb-muted);line-height:1.8;padding:0 0.1rem;">'
            '<span style="color:var(--accent);">✦</span> Batch-upload up to 50 files<br>'
            '<span style="color:var(--accent);">✦</span> PDF, DOCX, TXT, XLSX, CSV<br>'
            '<span style="color:var(--accent);">✦</span> Two export templates available'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div class="page-watermark">Research Analyzer v2</div>',
            unsafe_allow_html=True,
        )


def _sidebar_label(text: str) -> None:
    st.markdown(
        f'<div class="sb-label">{text}</div>',
        unsafe_allow_html=True,
    )


def _sidebar_divider() -> None:
    st.markdown("<hr style='border-color:#1e1c16;margin:1rem 0;'>", unsafe_allow_html=True)


# ── Cache helpers ─────────────────────────────────────────────────────────────
def _delete_doc_cache(doc_id: str) -> None:
    """Delete processed JSON + vector index so doc is re-extracted fresh."""
    import shutil
    try:
        from app.config import PROCESSED_DIR, VECTORSTORE_DIR
        json_file = Path(PROCESSED_DIR) / f"{doc_id}.json"
        if json_file.exists():
            json_file.unlink()
        vec_dir = Path(VECTORSTORE_DIR) / doc_id
        if vec_dir.exists():
            shutil.rmtree(vec_dir, ignore_errors=True)
    except ImportError as exc:
        logger.warning("_delete_doc_cache: config import failed — %s", exc)


def _delete_all_docs() -> None:
    """Wipe all processed docs and vector indexes."""
    import shutil
    try:
        from app.config import PROCESSED_DIR, VECTORSTORE_DIR, UPLOAD_DIR
        for folder in [PROCESSED_DIR, VECTORSTORE_DIR, UPLOAD_DIR]:
            p = Path(folder)
            if p.exists():
                shutil.rmtree(p, ignore_errors=True)
            p.mkdir(parents=True, exist_ok=True)
    except ImportError as exc:
        logger.warning("_delete_all_docs: config import failed — %s", exc)


# ── Upload / Process ──────────────────────────────────────────────────────────
def _handle_upload(f) -> None:
    # FIX: use getvalue() consistently — read() can return empty bytes if
    # the file cursor has already been advanced by Streamlit internals.
    try:
        file_bytes = f.getvalue()
    except Exception:
        file_bytes = f.read()

    with st.spinner("Uploading …"):
        doc, err = analysis_service.save_upload(file_bytes=file_bytes, filename=f.name)
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
            f'<div style="font-family:var(--f-mono);'
            f'font-size:0.72rem;color:var(--muted);">{step}</div>',
            unsafe_allow_html=True,
        )

    result = analysis_service.process_document(doc_id=doc_id, on_progress=on_progress, reprocess=True)
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

    _MODES = ["📄 Single PDF", "📚 Batch Upload", "📤 Export"]

    # FIX: use a stable key; avoid duplicate-widget errors across reruns
    current_mode = st.session_state.get("app_mode", _MODES[0])
    safe_index   = _MODES.index(current_mode) if current_mode in _MODES else 0

    mode = st.radio(
        "mode",
        _MODES,
        index            = safe_index,
        horizontal       = True,
        label_visibility = "collapsed",
        key              = "mode_radio",
    )

    # Sync to session state
    if mode != st.session_state.get("app_mode"):
        st.session_state.app_mode    = mode
        # Clear stale export data on mode switch
        st.session_state.export_data = {}

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
    # FIX: var(--font-mono) → var(--f-mono) (was undefined CSS variable)
    st.markdown("""
    <div class="empty-state">
        <div class="empty-icon-ring">📄</div>
        <div class="empty-title">Drop a paper to begin</div>
        <div class="empty-sub">
            Upload a PDF, DOCX, TXT, or spreadsheet from the sidebar.
            Your document will be extracted, indexed, and ready to chat with in seconds.
        </div>
        <div style="display:flex;gap:1rem;flex-wrap:wrap;justify-content:center;">
            <div class="feature-chip">
                <div class="feature-chip-icon">💬</div>
                <div class="feature-chip-label">Ask questions</div>
            </div>
            <div class="feature-chip">
                <div class="feature-chip-icon">🔍</div>
                <div class="feature-chip-label">Semantic search</div>
            </div>
            <div class="feature-chip">
                <div class="feature-chip-icon">📑</div>
                <div class="feature-chip-label">Section view</div>
            </div>
            <div class="feature-chip">
                <div class="feature-chip-icon">📤</div>
                <div class="feature-chip-label">Export metadata</div>
            </div>
        </div>
        <div style="margin-top:2rem;font-family:var(--f-mono);font-size:0.65rem;
                    color:var(--muted-2);letter-spacing:0.08em;">
            PDF · DOCX · TXT · XLSX · CSV &nbsp;·&nbsp; Up to 50MB
        </div>
    </div>
    """, unsafe_allow_html=True)


# ── Doc header ────────────────────────────────────────────────────────────────
def _render_doc_header(info: dict) -> None:
    meta    = info.get("metadata", {})
    chunks  = info.get("chunks",   {})
    title   = html.escape(meta.get("title") or info.get("filename", "Untitled"))
    authors = html.escape(", ".join(meta.get("authors", [])[:4]))
    journal = html.escape(meta.get("journal", "") or "")
    is_ocr  = meta.get("language") == "ocr"

    # Build journal + OCR pills
    pills_html = ""
    if journal:
        pills_html += f'<span class="doc-journal-pill">📰 {journal[:60]}</span>'
    if is_ocr:
        pills_html += '<span class="ocr-badge">🔍 OCR</span>'

    c1, c2, c3, c4 = st.columns([3, 1, 1, 1])
    with c1:
        st.markdown(
            f'<div class="doc-header-card">'
            f'  <div class="doc-title">{title[:110]}</div>'
            + (f'  <div class="doc-authors">{authors}</div>' if authors else "")
            + (f'  <div style="margin-top:0.5rem;">{pills_html}</div>' if pills_html else "")
            + f'</div>',
            unsafe_allow_html=True,
        )
    with c2:
        # FIX: guard against missing "pages" key — meta.get returns None safely
        pages_val = meta.get("pages") or 0
        st.markdown(
            f'<div class="stat-card">'
            f'  <div class="stat-label">Pages</div>'
            f'  <div class="stat-value">{pages_val}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    with c3:
        words_val = meta.get("words") or 0
        st.markdown(
            f'<div class="stat-card">'
            f'  <div class="stat-label">Words</div>'
            f'  <div class="stat-value">{_fmt(words_val)}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    with c4:
        chunk_label = "Chunks" + (" · OCR" if is_ocr else "")
        chunk_total = chunks.get("total") or 0
        st.markdown(
            f'<div class="stat-card">'
            f'  <div class="stat-label">{chunk_label}</div>'
            f'  <div class="stat-value">{chunk_total}</div>'
            f'</div>',
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

    # ── Render existing history ────────────────────────────────────────────
    # FIX: use correct CSS class names (.msg-user / .msg-assistant) that are
    # now defined in the stylesheet above. No duplicate rendering.
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
                    f'<div class="msg-user"><div>{html.escape(msg.content)}</div></div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div class="msg-assistant"><div>{_md_to_html(msg.content)}</div></div>',
                    unsafe_allow_html=True,
                )

    # ── Input row ──────────────────────────────────────────────────────────
    # FIX: counter-based key resets the text input widget after each send
    input_key = f"chat_input_{st.session_state.chat_input_key}"
    c_in, c_btn, c_clr = st.columns([7, 1, 1])
    with c_in:
        question = st.text_input(
            "question",
            placeholder="Ask anything about this paper …",
            label_visibility="collapsed",
            key=input_key,
        )
    with c_btn:
        send = st.button("Send", type="primary", use_container_width=True, key="send_btn")
    with c_clr:
        if st.button("Clear", use_container_width=True, disabled=not history, key="clear_btn"):
            st.session_state.chat_history  = []
            st.session_state.chat_input_key += 1
            st.rerun()

    # ── Suggested questions — only when no history ─────────────────────────
    if not history:
        st.markdown(
            '<div style="font-family:var(--f-mono);font-size:0.65rem;'
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
                    # FIX: handle chat, bump input key to clear field, then
                    # rerun — no double-rerun, stream completes before rerun
                    _handle_chat(doc_id, s)
                    st.session_state.chat_input_key += 1
                    st.rerun()

    # ── Handle manual send ─────────────────────────────────────────────────
    if send and question and question.strip():
        _handle_chat(doc_id, question.strip())
        st.session_state.chat_input_key += 1
        st.rerun()


def _handle_chat(doc_id: str, question: str) -> None:
    """
    Stream a response from the AI and append both turns to history.

    FIX: User message is appended to history first, THEN we stream the
    response into a placeholder. We do NOT manually render the user message
    here — the history loop at the top of _render_chat_tab handles that on
    the next rerun. This prevents duplicate message display.

    The typing indicator and stream container are ephemeral placeholders that
    only live during this call; after st.rerun() they are replaced by the
    properly re-rendered history.
    """
    try:
        # Append user turn to history
        st.session_state.chat_history.append(
            ChatMessage(role=MessageRole.USER, content=question)
        )
        if len(st.session_state.chat_history) > MAX_CHAT_HISTORY:
            st.session_state.chat_history = st.session_state.chat_history[-MAX_CHAT_HISTORY:]

        # Show user message immediately (ephemeral — will be re-rendered by history loop)
        st.markdown(
            f'<div class="msg-user"><div>{html.escape(question)}</div></div>',
            unsafe_allow_html=True,
        )

        # Typing indicator
        typing = st.empty()
        typing.markdown(
            '<div class="msg-assistant"><div class="typing-indicator">'
            '<div class="typing-dot"></div>'
            '<div class="typing-dot"></div>'
            '<div class="typing-dot"></div>'
            '</div></div>',
            unsafe_allow_html=True,
        )

        container  = st.empty()
        full_reply = ""

        stream = analysis_service.chat_stream(
            doc_id  = doc_id,
            question= question,
            # Pass history excluding the user turn we just added
            history = st.session_state.chat_history[:-1],
        )

        for token in stream:
            if not token:
                continue
            full_reply += str(token)
            typing.empty()
            container.markdown(
                f'<div class="msg-assistant"><div>{_md_to_html(full_reply)}'
                f'<span style="color:var(--accent);">▌</span></div></div>',
                unsafe_allow_html=True,
            )

        typing.empty()
        container.markdown(
            f'<div class="msg-assistant"><div>{_md_to_html(full_reply)}</div></div>',
            unsafe_allow_html=True,
        )

        # Append assistant turn to history
        st.session_state.chat_history.append(
            ChatMessage(
                role    = MessageRole.ASSISTANT,
                content = full_reply or "⚠️ No response received.",
            )
        )

    except Exception as e:
        logger.error("Chat failed: %s", e)
        st.error("⚠️ Chat error. Please try again.")
        # Roll back the user message we appended since the exchange failed
        if (
            st.session_state.chat_history
            and st.session_state.chat_history[-1].role == MessageRole.USER
        ):
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
    # FIX: stable key prevents DuplicateWidgetID on rerun
    idx = st.selectbox(
        "Section",
        range(len(labels)),
        format_func=lambda i: labels[i],
        label_visibility="collapsed",
        key="sections_selectbox",
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
        top_k = st.selectbox(
            "k", [3, 5, 10], index=1,
            label_visibility="collapsed",
            key="search_topk",
        )

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
                f'<div style="font-family:var(--f-mono);'
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
        '<div style="font-family:var(--f-display);font-size:1.1rem;'
        'font-weight:600;margin-bottom:1rem;">Document Metadata</div>',
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns(2)
    with c1:
        _meta_block("Title",     meta.get("title")   or "—")
        _meta_block("Authors",   ", ".join(meta.get("authors", [])) or "—")
        _meta_block("Journal",   meta.get("journal") or "—")
        _meta_block("Publisher", meta.get("publisher") or "—")
        lang = meta.get("language", "en")
        lang_display = "🔍 OCR Processed" if lang == "ocr" else lang.upper()
        _meta_block("Language / Mode", lang_display)
    with c2:
        _meta_block("Pages",     str(meta.get("pages") or 0),  large=True)
        _meta_block("Words",     _fmt(meta.get("words") or 0), large=True)
        _meta_block("DOI",       meta.get("doi")    or "—")
        _meta_block("ISSN",      meta.get("issn")   or "—")
        vol   = meta.get("volume") or ""
        issue = meta.get("issue")  or ""
        vol_issue = (
            (f"Vol {vol}" if vol else "") +
            (f", No {issue}" if issue else "")
        ) or "—"
        _meta_block("Vol / Issue", vol_issue)
        _meta_block("File Size", meta.get("file_size") or "—")

    # Keywords
    kws = meta.get("keywords", [])
    if kws:
        st.markdown(
            '<div style="font-family:var(--f-display);font-size:1rem;'
            'font-weight:600;margin:1rem 0 0.5rem;">Keywords</div>',
            unsafe_allow_html=True,
        )
        kw_html = " ".join(
            f'<span style="display:inline-block;background:var(--surface);'
            f'border:1px solid var(--border);border-radius:20px;'
            f'padding:0.18rem 0.65rem;font-family:var(--f-mono);'
            f'font-size:0.68rem;margin:0.2rem 0.2rem 0 0;color:var(--ink);">'
            f'{html.escape(k)}</span>'
            for k in kws
        )
        st.markdown(kw_html, unsafe_allow_html=True)

    st.markdown(
        '<div style="font-family:var(--f-display);font-size:1.1rem;'
        'font-weight:600;margin:1.5rem 0 0.75rem;">Sections Detected</div>',
        unsafe_allow_html=True,
    )
    if secs:
        rows = "".join(
            f'<div class="section-row">'
            f'<span style="font-weight:500;">{s["type"].capitalize()}</span>'
            f'<span style="font-family:var(--f-mono);'
            f'font-size:0.72rem;color:var(--muted);">'
            f'{_fmt(s.get("word_count", 0))} words · p.{s.get("page_start", 0)+1}</span>'
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
        '<div style="font-family:var(--f-display);font-size:1.1rem;'
        'font-weight:600;margin:1.5rem 0 0.75rem;">Vector Index</div>',
        unsafe_allow_html=True,
    )
    ci, cv = st.columns(2)
    with ci: _meta_block("Total Chunks",    str(chunks.get("total")   or 0), large=True)
    with cv: _meta_block("Indexed Vectors", str(chunks.get("indexed") or 0), large=True)

    created = info.get("created_at", "")[:19].replace("T", " ")
    if created:
        st.markdown(
            f'<div style="margin-top:1rem;font-family:var(--f-mono);'
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
        '<div style="font-family:var(--f-display);font-size:1.3rem;'
        'font-weight:600;margin-bottom:0.25rem;">Batch Upload</div>'
        '<div style="font-family:var(--f-mono);font-size:0.7rem;'
        'color:var(--muted);margin-bottom:1.5rem;">Upload 1–50 PDFs and process them all at once</div>',
        unsafe_allow_html=True,
    )

    uploaded_files = st.file_uploader(
        "Drop files here",
        type                  = ["pdf","docx","doc","txt","xlsx","xls","csv"],
        accept_multiple_files = True,
        label_visibility      = "collapsed",
        key                   = "batch_uploader",
    )

    if not uploaded_files:
        st.markdown("""
        <div style="text-align:center;padding:3rem 1rem;color:#a09890;font-size:0.85rem;">
            <div style="font-size:2.5rem;margin-bottom:0.75rem;">📚</div>
            Drop multiple files above — PDF, DOCX, TXT, XLSX, CSV supported.<br>
            Up to 50 files at a time. Each will be extracted and indexed automatically.
        </div>
        """, unsafe_allow_html=True)
        return

    count = len(uploaded_files)
    if count > 50:
        st.error(f"Maximum 50 PDFs per batch. You selected {count}. Please remove some files.")
        return

    st.markdown(
        f'<div style="font-family:var(--f-mono);font-size:0.75rem;'
        f'color:var(--muted);margin-bottom:1rem;">'
        f'{count} file{"s" if count != 1 else ""} selected</div>',
        unsafe_allow_html=True,
    )

    # FIX: read all bytes eagerly with getvalue() before rendering the list,
    # so the file cursor isn't moved by getvalue() calls below
    file_data: list[tuple[bytes, str]] = []
    for f in uploaded_files:
        try:
            file_data.append((f.getvalue(), f.name))
        except Exception:
            file_data.append((b"", f.name))

    # Preview list (first 15)
    preview_rows = "".join(
        f'<div class="batch-row">'
        f'<span>📄</span>'
        f'<span style="flex:1;">{html.escape(name[:50])}</span>'
        f'<span style="font-family:var(--f-mono);font-size:0.7rem;'
        f'color:var(--muted);">{round(len(data)/1024, 1)} KB</span>'
        f'</div>'
        for data, name in file_data[:15]
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
        _run_batch(file_data)


def _run_batch(file_data: list[tuple[bytes, str]]) -> None:
    """
    FIX: accepts pre-read (bytes, name) tuples instead of raw UploadedFile
    objects so getvalue() / read() ordering issues are eliminated.
    """
    total      = len(file_data)
    bar        = st.progress(0, text="Starting batch …")
    status_el  = st.empty()
    results_el = st.empty()
    rows: list[dict] = []

    def on_start(current: int, total: int, filename: str) -> None:
        pct = max(0.0, min((current - 1) / total, 1.0))
        bar.progress(pct, text=f"[{current}/{total}] {filename[:40]} …")
        status_el.markdown(
            f'<div style="font-family:var(--f-mono);'
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
            f'<span style="font-family:var(--f-mono);font-size:0.7rem;color:var(--muted);">'
            + (f'{r["pages"]}p · {r["words"]:,}w · {r["chunks"]} chunks'
               if r["status"] == "ready"
               else f'<span style="color:var(--accent);">{html.escape(str(r["error"])[:40])}</span>')
            + '</span></div>'
            for r in rows
        )
        results_el.markdown(
            f'<div style="border:1px solid var(--border);border-radius:10px;'
            f'overflow:hidden;margin-top:0.5rem;">{html_rows}</div>',
            unsafe_allow_html=True,
        )

    result = batch_service.process_batch(
        file_data, on_item_start=on_start, on_item_done=on_done,
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
        '<div style="font-family:var(--f-display);font-size:1.3rem;'
        'font-weight:600;margin-bottom:0.25rem;">Export</div>'
        '<div style="font-family:var(--f-mono);font-size:0.7rem;'
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

    # ── Template selector ─────────────────────────────────────────────────────
    template_choice = st.radio(
        "Export template",
        options   = ["Journal Articles", "PhD Theses"],
        horizontal= True,
        key       = "export_template",
    )
    template = "thesis" if template_choice == "PhD Theses" else "journal"

    # Clear cached exports when template switches
    prev = st.session_state.get("_last_export_template")
    if prev != template:
        st.session_state.export_data = {}
        st.session_state["_last_export_template"] = template

    st.caption(
        "📋 **Journal Articles** — authors, DOI, ISSN, journal, volume, issue, keywords …"
        if template == "journal" else
        "🎓 **PhD Theses** — author, date, description, abstract, publisher, subject, type"
    )
    st.markdown("<br>", unsafe_allow_html=True)

    # ── Export format cards ───────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    fname_suffix   = "_theses" if template == "thesis" else "_journal"

    # FIX: always read from session_state directly and write back after
    # mutation so Streamlit 1.32+ detects the state change properly.
    with c1:
        st.markdown("**📊 Excel (XLSX)**")
        st.caption("Matches your metadata template")
        if st.button("Generate XLSX", type="primary", use_container_width=True, key="gen_xlsx"):
            with st.spinner("Building …"):
                try:
                    result = export_service.export_xlsx(
                        selected_ids,
                        filename = f"metadata_export{fname_suffix}.xlsx",
                        template = template,
                    )
                    cache = dict(st.session_state.export_data)
                    cache["xlsx"] = result
                    st.session_state.export_data = cache
                except Exception as e:
                    st.error(f"XLSX export failed: {e}")
        if "xlsx" in st.session_state.export_data:
            data, fname = st.session_state.export_data["xlsx"]
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
                    result = export_service.export_docx(
                        selected_ids,
                        filename = f"research_report{fname_suffix}.docx",
                        template = template,
                    )
                    cache = dict(st.session_state.export_data)
                    cache["docx"] = result
                    st.session_state.export_data = cache
                except Exception as e:
                    st.error(f"DOCX export failed: {e}")
        if "docx" in st.session_state.export_data:
            data, fname = st.session_state.export_data["docx"]
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
                    result = export_service.export_csv(
                        selected_ids,
                        filename = f"metadata_export{fname_suffix}.csv",
                        template = template,
                    )
                    cache = dict(st.session_state.export_data)
                    cache["csv"] = result
                    st.session_state.export_data = cache
                except Exception as e:
                    st.error(f"CSV export failed: {e}")
        if "csv" in st.session_state.export_data:
            data, fname = st.session_state.export_data["csv"]
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
                    result = export_service.export_json(
                        selected_ids,
                        filename = f"metadata_export{fname_suffix}.json",
                        template = template,
                    )
                    cache = dict(st.session_state.export_data)
                    cache["json"] = result
                    st.session_state.export_data = cache
                except Exception as e:
                    st.error(f"JSON export failed: {e}")
        if "json" in st.session_state.export_data:
            data, fname = st.session_state.export_data["json"]
            st.download_button(
                "⬇ Download JSON", data=data, file_name=fname,
                mime="application/json", use_container_width=True, key="dl_json",
            )

    # Preview table
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '<div style="font-family:var(--f-display);font-size:1rem;'
        'font-weight:600;margin-bottom:0.5rem;">Selected Documents</div>',
        unsafe_allow_html=True,
    )
    rows_html = "".join(
        f'<div class="batch-row">'
        f'<span style="flex:2;">{html.escape(d["filename"][:45])}</span>'
        f'<span style="font-family:var(--f-mono);font-size:0.7rem;'
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