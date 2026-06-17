"""
drive_tab.py — Multi-account Google Drive sync UI.

Renders a full Drive management panel inside the sidebar and as a
dedicated tab.  Supports:
  - Adding / removing multiple Drive accounts (each with its own folder URL,
    OAuth client JSON path, and optional service-account fallback)
  - Per-account auth flow (OAuth consent → code exchange inline)
  - Individual account sync or sync-all-accounts-at-once
  - Real-time progress display during parallel download + ingestion
  - Account status badges (ready / login required / missing credentials)
  - Inline error reporting per account and per file
"""

from __future__ import annotations

import html
import os
import streamlit as st

from app.services.multi_drive_service import multi_drive_service, DriveAccount
from app.services.ingestion_service   import ingestion_service
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Sidebar mini-widget (compact version shown in the sidebar)
# ══════════════════════════════════════════════════════════════════════════════

def render_drive_sidebar() -> None:
    """Compact Drive widget for the app sidebar."""
    st.sidebar.markdown("<hr>", unsafe_allow_html=True)
    st.sidebar.markdown(
        '<span class="sb-label">Google Drive Sync</span>',
        unsafe_allow_html=True,
    )

    accounts = multi_drive_service.list_accounts()

    if not accounts:
        st.sidebar.caption("No Drive accounts configured.")
        if st.sidebar.button("＋ Add Drive Account", key="sb_drive_add", use_container_width=True):
            st.session_state.app_mode = "☁️ Drive Sync"
            st.rerun()
        return

    # Show each account with a status dot and quick-sync button
    for acc in accounts:
        status = multi_drive_service.get_account_status(acc.account_id)
        is_ready = status["status"] == "ready"
        dot_color = "var(--success)" if is_ready else "var(--accent)"
        dot = "●"

        st.sidebar.markdown(
            f'<div style="display:flex;justify-content:space-between;'
            f'align-items:center;margin:0.2rem 0;">'
            f'<span style="font-family:var(--f-mono);font-size:0.68rem;'
            f'color:var(--sb-muted);overflow:hidden;text-overflow:ellipsis;'
            f'white-space:nowrap;max-width:140px;" title="{html.escape(acc.label)}">'
            f'<span style="color:{dot_color};">{dot}</span> {html.escape(acc.label[:22])}'
            f'</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # Sync-all button
    if st.sidebar.button(
        f"⟳ Sync All ({len(accounts)})",
        type="primary",
        key="sb_drive_sync_all",
        use_container_width=True,
    ):
        _run_sync_all_sidebar()

    if st.sidebar.button(
        "⚙ Manage Drives",
        key="sb_drive_manage",
        use_container_width=True,
    ):
        st.session_state.app_mode = "☁️ Drive Sync"
        st.rerun()


def _run_sync_all_sidebar() -> None:
    """Sync-all with a compact sidebar status widget."""
    slot = st.sidebar.empty()
    with st.sidebar.status("Syncing all Drive accounts…", expanded=False) as sync_status:
        try:
            file_log: list[str] = []

            def on_prog(label: str, fname: str, cur: int, total: int) -> None:
                slot.markdown(
                    f'<div style="font-family:var(--f-mono);font-size:0.62rem;'
                    f'color:var(--sb-muted);">[{cur}/{total}] {html.escape(fname[:35])}</div>',
                    unsafe_allow_html=True,
                )

            def on_done(label: str, fname: str, err: str | None) -> None:
                if err:
                    file_log.append(f"✗ [{label}] {fname}: {err}")
                else:
                    file_log.append(f"✓ [{label}] {fname}")

            result = multi_drive_service.sync_all(
                on_progress  = on_prog,
                on_file_done = on_done,
            )
            jobs = ingestion_service.process_pending_jobs(limit=100)
            sync_status.update(label="Sync complete ✓", state="complete")
            slot.empty()
            st.sidebar.success(
                f"↑ {result.total_new} new · ↻ {result.total_updated} updated · "
                f"→ {result.total_skipped} skipped · "
                f"✓ {jobs.get('succeeded', 0)} ingested"
            )
            if result.total_failed:
                st.sidebar.warning(f"{result.total_failed} file(s) failed during sync.")
            st.rerun()
        except Exception as e:
            sync_status.update(label="Sync failed", state="error")
            st.sidebar.error(f"Sync error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Full Drive management tab
# ══════════════════════════════════════════════════════════════════════════════

def render_drive_tab() -> None:
    """Full Drive management tab rendered in the main content area."""
    st.markdown(
        '<div style="font-family:\'Lora\',serif;font-size:1.3rem;font-weight:600;'
        'margin-bottom:0.25rem;">Google Drive Sync</div>'
        '<div style="font-family:var(--f-mono);font-size:0.7rem;color:var(--muted);'
        'margin-bottom:1.5rem;">'
        'Connect multiple Google Drive folders · parallel download · live progress'
        '</div>',
        unsafe_allow_html=True,
    )

    accounts = multi_drive_service.list_accounts()

    # ── Sync-all banner ───────────────────────────────────────────────────────
    if accounts:
        ready_count = sum(
            1 for a in accounts
            if multi_drive_service.get_account_status(a.account_id)["status"] == "ready"
        )
        c_info, c_btn = st.columns([4, 1])
        with c_info:
            st.markdown(
                f'<div style="background:var(--surface);border:1px solid var(--border);'
                f'border-radius:10px;padding:0.75rem 1rem;font-size:0.85rem;">'
                f'<strong>{len(accounts)}</strong> account(s) configured · '
                f'<strong style="color:var(--success);">{ready_count}</strong> ready'
                f'</div>',
                unsafe_allow_html=True,
            )
        with c_btn:
            if st.button(
                "⟳ Sync All",
                type="primary",
                use_container_width=True,
                key="tab_sync_all",
                disabled=ready_count == 0,
            ):
                _run_sync_all_tab()

        st.markdown("<br>", unsafe_allow_html=True)

    # ── Account cards ─────────────────────────────────────────────────────────
    for acc in accounts:
        _render_account_card(acc)

    # ── Add account form ──────────────────────────────────────────────────────
    st.markdown("<hr>", unsafe_allow_html=True)
    _render_add_account_form()

    # ── OAuth code exchange (one slot per account needing it) ─────────────────
    for acc in accounts:
        status = multi_drive_service.get_account_status(acc.account_id)
        if status["status"] == "oauth_login_required":
            _render_oauth_callback(acc, status)


def _render_account_card(acc: DriveAccount) -> None:
    """Renders a single account card with status, controls, and per-account sync."""
    status = multi_drive_service.get_account_status(acc.account_id)
    s_code = status["status"]

    badge_color = {
        "ready"               : "var(--success)",
        "oauth_login_required": "var(--warn)",
        "missing_credentials" : "var(--accent)",
        "missing_folder"      : "var(--muted)",
    }.get(s_code, "var(--muted)")

    badge_text = {
        "ready"               : "✓ Ready",
        "oauth_login_required": "⚠ Login required",
        "missing_credentials" : "✗ No credentials",
        "missing_folder"      : "✗ No folder",
    }.get(s_code, s_code)

    with st.expander(
        f"📁 {acc.label}  —  {badge_text}",
        expanded=(s_code != "ready"),
    ):
        col_info, col_ctrl = st.columns([3, 1])
        with col_info:
            st.markdown(
                f'<div style="font-family:var(--f-mono);font-size:0.72rem;'
                f'color:var(--muted);line-height:1.9;">'
                f'<div><strong>Folder:</strong> {html.escape(acc.folder_id or "—")}</div>'
                f'<div><strong>Auth:</strong> {html.escape(status.get("auth_mode", "—"))}</div>'
                f'<div><strong>Token:</strong> {html.escape(acc.token_path)}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

        with col_ctrl:
            # Enable/disable toggle
            enabled = acc.enabled
            if st.checkbox(
                "Enabled", value=enabled,
                key=f"acc_enabled_{acc.account_id}",
            ):
                if not enabled:
                    multi_drive_service.set_account_enabled(acc.account_id, True)
            else:
                if enabled:
                    multi_drive_service.set_account_enabled(acc.account_id, False)

        # OAuth sign-in button
        if s_code == "oauth_login_required":
            auth_url = status.get("authorization_url") or \
                       multi_drive_service.get_auth_url(acc.account_id)
            if auth_url:
                st.info(
                    "This account needs a one-time Google sign-in. "
                    "Click the link below, authorise, then paste the code back here."
                )
                st.markdown(
                    f'<a href="{html.escape(auth_url)}" target="_blank" '
                    f'style="font-family:var(--f-mono);font-size:0.8rem;'
                    f'color:var(--accent);">→ Sign in with Google</a>',
                    unsafe_allow_html=True,
                )

        st.markdown("<br>", unsafe_allow_html=True)

        # Inline folder ID update
        new_folder = st.text_input(
            "Update folder URL / ID",
            value=acc.folder_id,
            key=f"acc_folder_{acc.account_id}",
            label_visibility="visible",
            placeholder="https://drive.google.com/drive/folders/…",
        )

        btn_col1, btn_col2, btn_col3 = st.columns(3)
        with btn_col1:
            if st.button(
                "💾 Save",
                key=f"acc_save_{acc.account_id}",
                use_container_width=True,
            ):
                if new_folder.strip():
                    multi_drive_service.update_account(
                        acc.account_id, folder_id=new_folder.strip(),
                    )
                    st.success("Folder updated.")
                    st.rerun()

        with btn_col2:
            if st.button(
                "⟳ Sync",
                key=f"acc_sync_{acc.account_id}",
                use_container_width=True,
                disabled=(s_code != "ready"),
                type="primary",
            ):
                _run_account_sync(acc)

        with btn_col3:
            if st.button(
                "🗑 Remove",
                key=f"acc_remove_{acc.account_id}",
                use_container_width=True,
            ):
                multi_drive_service.remove_account(acc.account_id)
                st.success(f"Removed '{acc.label}'.")
                st.rerun()


def _render_add_account_form() -> None:
    """Form to add a new Drive account."""
    with st.expander("＋ Add New Drive Account", expanded=len(multi_drive_service.list_accounts()) == 0):
        st.caption(
            "Each account has its own OAuth client JSON and token. "
            "You can add as many Drive folders as you need."
        )

        label = st.text_input(
            "Account label",
            placeholder="e.g. Research Papers, Team Shared Drive",
            key="new_acc_label",
        )
        folder = st.text_input(
            "Drive folder URL or ID",
            placeholder="https://drive.google.com/drive/folders/1Bxi…",
            key="new_acc_folder",
        )

        use_custom_client = st.checkbox(
            "Use a custom OAuth client JSON (leave unchecked to reuse the global one)",
            key="new_acc_custom_client",
        )
        client_path = ""
        if use_custom_client:
            client_path = st.text_input(
                "Path to google_oauth_client.json",
                placeholder="/path/to/client.json",
                key="new_acc_client_path",
            )

        use_sa = st.checkbox(
            "Use service-account JSON instead of OAuth",
            key="new_acc_use_sa",
        )
        sa_path = ""
        if use_sa:
            sa_path = st.text_input(
                "Path to service-account credentials.json",
                placeholder="/path/to/credentials.json",
                key="new_acc_sa_path",
            )

        if st.button("Add Account", type="primary", key="new_acc_submit"):
            if not label.strip():
                st.warning("Please enter a label for this account.")
            elif not folder.strip():
                st.warning("Please enter a folder URL or ID.")
            else:
                acc = multi_drive_service.add_account(
                    label       = label.strip(),
                    folder_id   = folder.strip(),
                    client_path = client_path.strip() if client_path else "",
                    sa_path     = sa_path.strip() if sa_path else "",
                )
                st.success(f"Account '{acc.label}' added.")
                st.rerun()


def _render_oauth_callback(acc: DriveAccount, status: dict) -> None:
    """Inline OAuth code exchange for a specific account."""
    st.markdown(
        f'<div style="background:var(--surface);border:1px solid var(--warn);'
        f'border-radius:8px;padding:0.75rem 1rem;margin:0.5rem 0;">'
        f'<strong>{html.escape(acc.label)}</strong> — paste the OAuth code below:</div>',
        unsafe_allow_html=True,
    )
    code_val = st.text_input(
        "OAuth code",
        key=f"oauth_code_{acc.account_id}",
        placeholder="Paste the code from the redirect URL here",
        label_visibility="collapsed",
    )
    if st.button(
        "Exchange code",
        key=f"oauth_exchange_{acc.account_id}",
        type="primary",
    ):
        if code_val.strip():
            ok = multi_drive_service.exchange_code(acc.account_id, code_val.strip())
            if ok:
                st.success(f"✓ '{acc.label}' authorised successfully.")
                st.rerun()
            else:
                st.error("Code exchange failed. Try signing in again.")
        else:
            st.warning("Please paste the OAuth code first.")


def _run_sync_all_tab() -> None:
    """Full sync-all with detailed progress table in the main area."""
    progress_slot  = st.empty()
    result_slot    = st.empty()
    rows: list[dict] = []

    with st.status("Syncing all Drive accounts…", expanded=True) as sync_status:
        try:
            def on_prog(label: str, fname: str, cur: int, total: int) -> None:
                progress_slot.markdown(
                    f'<div style="font-family:var(--f-mono);font-size:0.72rem;'
                    f'color:var(--muted);">'
                    f'[{label}] {cur}/{total}: {html.escape(fname[:50])}</div>',
                    unsafe_allow_html=True,
                )

            def on_done(label: str, fname: str, err: str | None) -> None:
                rows.append({
                    "icon"  : "✗" if err else "✓",
                    "color" : "var(--accent)" if err else "var(--success)",
                    "label" : label,
                    "name"  : fname,
                    "error" : err or "",
                })
                html_rows = "".join(
                    f'<div style="display:flex;gap:0.75rem;align-items:center;'
                    f'padding:0.35rem 0.75rem;border-bottom:1px solid var(--border);'
                    f'font-size:0.8rem;">'
                    f'<span style="color:{r["color"]};font-weight:700;">{r["icon"]}</span>'
                    f'<span style="min-width:90px;font-family:var(--f-mono);'
                    f'font-size:0.68rem;color:var(--muted);">{html.escape(r["label"][:16])}</span>'
                    f'<span style="flex:1;">{html.escape(r["name"][:50])}</span>'
                    + (f'<span style="color:var(--accent);font-size:0.7rem;">'
                       f'{html.escape(r["error"][:40])}</span>' if r["error"] else "")
                    + '</div>'
                    for r in rows[-30:]   # show last 30
                )
                result_slot.markdown(
                    f'<div style="border:1px solid var(--border);border-radius:8px;'
                    f'overflow:hidden;max-height:400px;overflow-y:auto;">'
                    f'{html_rows}</div>',
                    unsafe_allow_html=True,
                )

            drive_result = multi_drive_service.sync_all(
                on_progress  = on_prog,
                on_file_done = on_done,
            )

            progress_slot.empty()
            st.info(
                f"⬇ {drive_result.total_new} new · "
                f"↻ {drive_result.total_updated} updated · "
                f"→ {drive_result.total_skipped} skipped · "
                f"✗ {drive_result.total_failed} failed"
            )

            # Process ingestion jobs
            st.write("Processing ingestion queue…")
            jobs_result = ingestion_service.process_pending_jobs(
                limit       = 100,
                on_progress = lambda step, pct: progress_slot.markdown(
                    f'<div style="font-family:var(--f-mono);font-size:0.72rem;'
                    f'color:var(--muted);">[{pct}%] {html.escape(step)}</div>',
                    unsafe_allow_html=True,
                ),
            )
            progress_slot.empty()

            sync_status.update(label="Sync & ingestion complete ✓", state="complete")
            st.success(
                f"✓ {jobs_result.get('succeeded', 0)}/{jobs_result.get('total', 0)} "
                f"documents ingested in {jobs_result.get('duration_s', 0):.1f}s"
            )
            if drive_result.total_failed:
                st.warning(
                    f"{drive_result.total_failed} file(s) failed during download. "
                    "Check the table above for details."
                )

            # Per-account breakdown
            if len(drive_result.accounts) > 1:
                st.markdown("**Per-account breakdown:**")
                for ar in drive_result.accounts:
                    st.markdown(
                        f"- **{ar.label}**: {ar.new} new · {ar.updated} updated · "
                        f"{ar.skipped} skipped · {ar.failed} failed "
                        f"({ar.duration_s:.1f}s)"
                        + (f" — *{ar.error}*" if ar.error else ""),
                    )

            st.rerun()

        except Exception as e:
            sync_status.update(label="Sync failed", state="error")
            st.error(f"Sync error: {e}")
            logger.error("Drive sync-all failed: %s", e, exc_info=True)


def _run_account_sync(acc: DriveAccount) -> None:
    """Sync a single account with live progress."""
    progress_slot = st.empty()
    result_slot   = st.empty()
    rows: list[dict] = []

    with st.status(f"Syncing '{acc.label}'…", expanded=True) as sync_status:
        try:
            def on_prog(fname: str, cur: int, total: int) -> None:
                progress_slot.markdown(
                    f'<div style="font-family:var(--f-mono);font-size:0.72rem;'
                    f'color:var(--muted);">{cur}/{total}: {html.escape(fname[:60])}</div>',
                    unsafe_allow_html=True,
                )

            def on_done(fname: str, err: str | None) -> None:
                rows.append({
                    "icon"  : "✗" if err else "✓",
                    "color" : "var(--accent)" if err else "var(--success)",
                    "name"  : fname,
                    "error" : err or "",
                })
                html_rows = "".join(
                    f'<div style="display:flex;gap:0.75rem;align-items:center;'
                    f'padding:0.35rem 0.75rem;border-bottom:1px solid var(--border);'
                    f'font-size:0.8rem;">'
                    f'<span style="color:{r["color"]};font-weight:700;">{r["icon"]}</span>'
                    f'<span style="flex:1;">{html.escape(r["name"][:55])}</span>'
                    + (f'<span style="color:var(--accent);font-size:0.7rem;">'
                       f'{html.escape(r["error"][:40])}</span>' if r["error"] else "")
                    + '</div>'
                    for r in rows[-25:]
                )
                result_slot.markdown(
                    f'<div style="border:1px solid var(--border);border-radius:8px;'
                    f'overflow:hidden;">{html_rows}</div>',
                    unsafe_allow_html=True,
                )

            sync_r = multi_drive_service.sync_account(
                account_id   = acc.account_id,
                on_progress  = on_prog,
                on_file_done = on_done,
            )
            progress_slot.empty()

            jobs_r = ingestion_service.process_pending_jobs(limit=100)
            sync_status.update(label="Done ✓", state="complete")
            st.success(
                f"↑ {sync_r.new} new · ↻ {sync_r.updated} updated · "
                f"→ {sync_r.skipped} skipped · "
                f"✓ {jobs_r.get('succeeded', 0)} ingested · "
                f"✗ {sync_r.failed} failed "
                f"({sync_r.duration_s:.1f}s)"
            )
            st.rerun()

        except Exception as e:
            sync_status.update(label="Failed", state="error")
            st.error(f"Sync error: {e}")
