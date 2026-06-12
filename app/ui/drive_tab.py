"""
drive_tab.py — Multi-account Google Drive sync + Processing Queue UI.

Two surfaces:
  render_drive_sidebar()  — compact sidebar widget
  render_drive_tab()      — full management tab with:
      • Account cards (add / remove / save / per-account sync)
      • OAuth flow inline
      • Processing Queue section:
          - Live table of all ingestion jobs (queued / running / completed / failed)
          - "Process Now" button that runs all queued jobs with live progress
          - Per-job "Retry" button for failed jobs
          - "Retry All Failed" bulk action
"""

from __future__ import annotations

import html
import time
import streamlit as st

from app.db.repository import repository
from app.services.ingestion_service import ingestion_service
from app.services.multi_drive_service import multi_drive_service, DriveAccount
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Sidebar mini-widget
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
        if st.sidebar.button(
            "＋ Add Drive Account", key="sb_drive_add", width="stretch"
        ):
            st.session_state.app_mode = "☁️ Drive Sync"
            st.rerun()
        return

    for acc in accounts:
        status = multi_drive_service.get_account_status(acc.account_id)
        is_ready = status["status"] == "ready"
        dot_color = "var(--success)" if is_ready else "var(--accent)"
        st.sidebar.markdown(
            f'<div style="font-family:var(--f-mono);font-size:0.68rem;'
            f'color:var(--sb-muted);overflow:hidden;text-overflow:ellipsis;'
            f'white-space:nowrap;margin:0.15rem 0;">'
            f'<span style="color:{dot_color};">●</span> '
            f'{html.escape(acc.label[:24])}'
            f'</div>',
            unsafe_allow_html=True,
        )

    # Show pending jobs count if any
    try:
        pending = repository.list_ingestion_jobs(statuses=["queued", "running"])
        failed  = repository.list_ingestion_jobs(statuses=["failed"])
        if pending:
            st.sidebar.caption(f"⏳ {len(pending)} job(s) queued")
        if failed:
            st.sidebar.warning(f"✗ {len(failed)} job(s) failed", icon="⚠️")
    except Exception:
        pass

    if st.sidebar.button(
        f"⟳ Sync All ({len(accounts)})",
        type="primary",
        key="sb_drive_sync_all",
        width="stretch",
    ):
        _run_sync_all_sidebar()

    if st.sidebar.button(
        "⚙ Manage Drives", key="sb_drive_manage", width="stretch",
    ):
        st.session_state.app_mode = "☁️ Drive Sync"
        st.rerun()


def _run_sync_all_sidebar() -> None:
    slot = st.sidebar.empty()
    with st.sidebar.status("Syncing all Drive accounts…", expanded=False) as s:
        try:
            def on_prog(label, fname, cur, total):
                slot.markdown(
                    f'<div style="font-family:var(--f-mono);font-size:0.62rem;'
                    f'color:var(--sb-muted);">[{cur}/{total}] {html.escape(fname[:35])}</div>',
                    unsafe_allow_html=True,
                )

            result = multi_drive_service.sync_all(on_progress=on_prog)
            jobs   = ingestion_service.process_pending_jobs(limit=100)
            s.update(label="Sync complete ✓", state="complete")
            slot.empty()
            st.sidebar.success(
                f"↑ {result.total_new} new · ↻ {result.total_updated} updated · "
                f"✓ {jobs.get('succeeded', 0)} ingested"
            )
            if result.total_failed:
                st.sidebar.warning(f"{result.total_failed} file(s) failed.")
            st.rerun()
        except Exception as e:
            s.update(label="Sync failed", state="error")
            st.sidebar.error(f"Sync error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Full Drive management tab
# ══════════════════════════════════════════════════════════════════════════════

def render_drive_tab() -> None:
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
                width="stretch",
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

    # ── OAuth callback slots ──────────────────────────────────────────────────
    for acc in accounts:
        status = multi_drive_service.get_account_status(acc.account_id)
        if status["status"] == "oauth_login_required":
            _render_oauth_callback(acc, status)

    # ── Processing Queue ──────────────────────────────────────────────────────
    st.markdown("<hr>", unsafe_allow_html=True)
    _render_processing_queue()


# ══════════════════════════════════════════════════════════════════════════════
# Processing Queue section
# ══════════════════════════════════════════════════════════════════════════════

def _render_processing_queue() -> None:
    """
    Full ingestion job queue panel.

    Shows every job with its status, lets the user:
      - Process all queued jobs with live progress
      - Retry individual failed jobs
      - Retry all failed jobs at once
    """
    st.markdown(
        '<div style="font-family:\'Lora\',serif;font-size:1.1rem;font-weight:600;'
        'margin-bottom:0.5rem;">📋 Processing Queue</div>',
        unsafe_allow_html=True,
    )

    # Load all jobs (most recent first)
    try:
        all_jobs = repository.list_ingestion_jobs(limit=200)
    except Exception as e:
        st.error(f"Could not load processing queue: {e}")
        return

    queued   = [j for j in all_jobs if j.status == "queued"]
    running  = [j for j in all_jobs if j.status == "running"]
    done     = [j for j in all_jobs if j.status == "completed"]
    failed   = [j for j in all_jobs if j.status == "failed"]

    # ── Summary bar ───────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(
            f'<div class="stat-card"><div class="stat-label">Queued</div>'
            f'<div class="stat-value" style="color:var(--warn);">{len(queued)}</div></div>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f'<div class="stat-card"><div class="stat-label">Running</div>'
            f'<div class="stat-value" style="color:var(--info);">{len(running)}</div></div>',
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            f'<div class="stat-card"><div class="stat-label">Completed</div>'
            f'<div class="stat-value" style="color:var(--success);">{len(done)}</div></div>',
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            f'<div class="stat-card"><div class="stat-label">Failed</div>'
            f'<div class="stat-value" style="color:var(--accent);">{len(failed)}</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Action buttons ────────────────────────────────────────────────────────
    btn_cols = st.columns([2, 2, 2, 3])
    with btn_cols[0]:
        process_clicked = st.button(
            f"▶ Process Now ({len(queued)} queued)",
            type="primary",
            width="stretch",
            key="pq_process_now",
            disabled=len(queued) == 0,
            help="Extract text, chunk, and embed all queued documents",
        )
    with btn_cols[1]:
        retry_all_clicked = st.button(
            f"↺ Retry All Failed ({len(failed)})",
            width="stretch",
            key="pq_retry_all",
            disabled=len(failed) == 0,
            help="Re-queue all failed jobs",
        )
    with btn_cols[2]:
        if st.button(
            "↺ Refresh",
            width="stretch",
            key="pq_refresh",
            help="Reload the queue table",
        ):
            st.rerun()

    # ── Handle Retry All ─────────────────────────────────────────────────────
    if retry_all_clicked:
        try:
            n = repository.requeue_all_failed_jobs()
            st.success(f"Re-queued {n} failed job(s). Click **Process Now** to run them.")
            st.rerun()
        except Exception as e:
            st.error(f"Retry failed: {e}")

    # ── Handle Process Now ────────────────────────────────────────────────────
    if process_clicked:
        _run_process_queue()
        return   # rerun happens inside

    # ── Job table ─────────────────────────────────────────────────────────────
    if not all_jobs:
        st.info(
            "No ingestion jobs yet. Sync a Drive folder to populate the queue.",
            icon="📭",
        )
        return

    # Show jobs grouped: failed first (need attention), then queued, running, completed
    ordered = failed + queued + running + done

    STATUS_COLOR = {
        "queued"    : "var(--warn)",
        "running"   : "var(--info, #1a5fa8)",
        "completed" : "var(--success)",
        "failed"    : "var(--accent)",
    }
    STATUS_ICON = {
        "queued"    : "⏳",
        "running"   : "⚙",
        "completed" : "✓",
        "failed"    : "✗",
    }

    st.markdown(
        '<div style="background:var(--card);border:1.5px solid var(--border);'
        'border-radius:var(--r-lg);overflow:hidden;">',
        unsafe_allow_html=True,
    )

    # Header row
    st.markdown(
        '<div style="display:grid;grid-template-columns:2fr 1fr 1fr 1fr 2fr;'
        'gap:0.5rem;padding:0.55rem 1rem;background:var(--surface);'
        'font-family:var(--f-mono);font-size:0.62rem;color:var(--muted);'
        'text-transform:uppercase;letter-spacing:0.08em;border-bottom:1.5px solid var(--border);">'
        '<span>Document</span><span>Status</span><span>Source</span>'
        '<span>Queued</span><span>Error / Action</span>'
        '</div>',
        unsafe_allow_html=True,
    )

    for job in ordered[:100]:   # cap at 100 rows to keep UI fast
        fname  = ""
        if job.document:
            fname = job.document.filename or ""
        fname = fname[:48] or f"job-{job.id}"

        status_color = STATUS_COLOR.get(job.status, "var(--muted)")
        status_icon  = STATUS_ICON.get(job.status, "?")
        queued_at    = ""
        if job.queued_at:
            queued_at = job.queued_at.strftime("%m/%d %H:%M")

        error_snippet = ""
        if job.error_message:
            error_snippet = html.escape((job.error_message or "")[:60])

        # Main row HTML
        st.markdown(
            f'<div style="display:grid;grid-template-columns:2fr 1fr 1fr 1fr 2fr;'
            f'gap:0.5rem;padding:0.5rem 1rem;border-bottom:1px solid var(--surface-2);'
            f'font-size:0.83rem;align-items:center;">'
            f'<span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'
            f'color:var(--ink-2);">{html.escape(fname)}</span>'
            f'<span style="color:{status_color};font-family:var(--f-mono);font-size:0.72rem;">'
            f'{status_icon} {job.status}</span>'
            f'<span style="font-family:var(--f-mono);font-size:0.68rem;color:var(--muted);">'
            f'{html.escape(job.source or "")}</span>'
            f'<span style="font-family:var(--f-mono);font-size:0.68rem;color:var(--muted);">'
            f'{queued_at}</span>'
            f'<span style="font-size:0.72rem;color:var(--accent);overflow:hidden;'
            f'text-overflow:ellipsis;white-space:nowrap;">{error_snippet}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # Retry button for failed jobs — rendered below the row
        if job.status == "failed":
            _, _, _, _, retry_col = st.columns([2, 1, 1, 1, 2])
            with retry_col:
                if st.button(
                    "↺ Retry",
                    key=f"pq_retry_{job.id}",
                    help=f"Re-queue job {job.id}",
                ):
                    try:
                        repository.requeue_ingestion_job(job.id)
                        st.success(f"Job {job.id} re-queued.")
                        time.sleep(0.3)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Retry failed: {e}")

    st.markdown("</div>", unsafe_allow_html=True)

    if len(ordered) > 100:
        st.caption(f"Showing 100 of {len(ordered)} jobs.")


def _run_process_queue() -> None:
    """Run all queued ingestion jobs with a live progress display."""
    progress_slot = st.empty()
    log_slot      = st.empty()
    log_lines: list[str] = []

    with st.status("Processing ingestion queue…", expanded=True) as proc_status:
        try:
            def on_progress(step: str, pct: int) -> None:
                progress_slot.progress(
                    pct / 100,
                    text=f"{pct}% — {html.escape(step[:70])}",
                )
                log_lines.append(f"[{pct:3d}%] {step}")
                log_slot.markdown(
                    '<div style="font-family:var(--f-mono);font-size:0.7rem;'
                    'color:var(--muted);max-height:180px;overflow-y:auto;">'
                    + "".join(
                        f'<div>{html.escape(l)}</div>'
                        for l in log_lines[-20:]
                    )
                    + "</div>",
                    unsafe_allow_html=True,
                )

            t0 = time.monotonic()
            result = ingestion_service.process_pending_jobs(
                limit=200,
                on_progress=on_progress,
            )
            elapsed = round(time.monotonic() - t0, 1)

            progress_slot.empty()
            log_slot.empty()
            proc_status.update(label="Processing complete ✓", state="complete")

            succeeded = result.get("succeeded", 0)
            total     = result.get("total", 0)
            failed_n  = result.get("failed", 0)

            if succeeded == total:
                st.success(
                    f"✓ All {total} document(s) ingested in {elapsed}s — "
                    f"ready for search and chat."
                )
            else:
                st.warning(
                    f"✓ {succeeded}/{total} succeeded · ✗ {failed_n} failed "
                    f"({elapsed}s). Use **Retry All Failed** above."
                )

            st.rerun()

        except Exception as e:
            proc_status.update(label="Processing failed", state="error")
            st.error(f"Processing error: {e}")
            logger.error("process_pending_jobs failed: %s", e, exc_info=True)


# ══════════════════════════════════════════════════════════════════════════════
# Account card
# ══════════════════════════════════════════════════════════════════════════════

def _render_account_card(acc: DriveAccount) -> None:
    status = multi_drive_service.get_account_status(acc.account_id)
    s_code = status["status"]

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
            enabled = acc.enabled
            new_enabled = st.checkbox(
                "Enabled",
                value=enabled,
                key=f"acc_enabled_{acc.account_id}",
            )
            if new_enabled != enabled:
                multi_drive_service.set_account_enabled(acc.account_id, new_enabled)

        if s_code == "oauth_login_required":
            auth_url = (
                status.get("authorization_url")
                or multi_drive_service.get_auth_url(acc.account_id)
            )
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

        new_folder = st.text_input(
            "Update folder URL / ID",
            value=acc.folder_id,
            key=f"acc_folder_{acc.account_id}",
            placeholder="https://drive.google.com/drive/folders/…",
        )

        btn_col1, btn_col2, btn_col3 = st.columns(3)
        with btn_col1:
            if st.button(
                "💾 Save",
                key=f"acc_save_{acc.account_id}",
                width="stretch",
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
                width="stretch",
                disabled=(s_code != "ready"),
                type="primary",
            ):
                _run_account_sync(acc)

        with btn_col3:
            if st.button(
                "🗑 Remove",
                key=f"acc_remove_{acc.account_id}",
                width="stretch",
            ):
                multi_drive_service.remove_account(acc.account_id)
                st.success(f"Removed '{acc.label}'.")
                st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# Add account form
# ══════════════════════════════════════════════════════════════════════════════

def _render_add_account_form() -> None:
    with st.expander(
        "＋ Add New Drive Account",
        expanded=(len(multi_drive_service.list_accounts()) == 0),
    ):
        st.caption(
            "Each account has its own OAuth client JSON and token. "
            "You can add as many Drive folders as you need."
        )

        label  = st.text_input(
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
            "Use a custom OAuth client JSON",
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
                    label=label.strip(),
                    folder_id=folder.strip(),
                    client_path=client_path.strip() if client_path else "",
                    sa_path=sa_path.strip() if sa_path else "",
                )
                st.success(f"Account '{acc.label}' added.")
                st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# OAuth callback
# ══════════════════════════════════════════════════════════════════════════════

def _render_oauth_callback(acc: DriveAccount, status: dict) -> None:
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


# ══════════════════════════════════════════════════════════════════════════════
# Sync runners
# ══════════════════════════════════════════════════════════════════════════════

def _run_sync_all_tab() -> None:
    progress_slot = st.empty()
    result_slot   = st.empty()
    rows: list[dict] = []

    with st.status("Syncing all Drive accounts…", expanded=True) as sync_status:
        try:
            def on_prog(label, fname, cur, total):
                progress_slot.markdown(
                    f'<div style="font-family:var(--f-mono);font-size:0.72rem;'
                    f'color:var(--muted);">'
                    f'[{label}] {cur}/{total}: {html.escape(fname[:50])}</div>',
                    unsafe_allow_html=True,
                )

            def on_done(label, fname, err):
                rows.append({
                    "icon" : "✗" if err else "✓",
                    "color": "var(--accent)" if err else "var(--success)",
                    "label": label,
                    "name" : fname,
                    "error": err or "",
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
                    for r in rows[-30:]
                )
                result_slot.markdown(
                    f'<div style="border:1px solid var(--border);border-radius:8px;'
                    f'overflow:hidden;max-height:400px;overflow-y:auto;">'
                    f'{html_rows}</div>',
                    unsafe_allow_html=True,
                )

            drive_result = multi_drive_service.sync_all(
                on_progress=on_prog,
                on_file_done=on_done,
            )
            progress_slot.empty()

            st.info(
                f"⬇ {drive_result.total_new} new · "
                f"↻ {drive_result.total_updated} updated · "
                f"→ {drive_result.total_skipped} skipped · "
                f"✗ {drive_result.total_failed} failed"
            )

            # ── Auto-process queued jobs ───────────────────────────────────
            if drive_result.total_new + drive_result.total_updated > 0:
                st.write("Processing ingestion queue…")
                jobs_result = ingestion_service.process_pending_jobs(
                    limit=200,
                    on_progress=lambda step, pct: progress_slot.markdown(
                        f'<div style="font-family:var(--f-mono);font-size:0.72rem;'
                        f'color:var(--muted);">[{pct}%] {html.escape(step)}</div>',
                        unsafe_allow_html=True,
                    ),
                )
                progress_slot.empty()

                sync_status.update(
                    label="Sync & ingestion complete ✓", state="complete"
                )
                succeeded = jobs_result.get("succeeded", 0)
                total_j   = jobs_result.get("total", 0)
                dur       = jobs_result.get("duration_s", 0)
                st.success(
                    f"✓ {succeeded}/{total_j} documents ingested in {dur:.1f}s"
                )
                if jobs_result.get("failed", 0):
                    st.warning(
                        f"{jobs_result['failed']} job(s) failed. "
                        "Scroll down to the **Processing Queue** to retry them."
                    )
            else:
                sync_status.update(label="Sync complete ✓", state="complete")
                st.info("No new or changed files — nothing to process.")

            if drive_result.total_failed:
                st.warning(
                    f"{drive_result.total_failed} file(s) failed during download."
                )

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
    progress_slot = st.empty()
    result_slot   = st.empty()
    rows: list[dict] = []

    with st.status(f"Syncing '{acc.label}'…", expanded=True) as sync_status:
        try:
            def on_prog(fname, cur, total):
                progress_slot.markdown(
                    f'<div style="font-family:var(--f-mono);font-size:0.72rem;'
                    f'color:var(--muted);">{cur}/{total}: {html.escape(fname[:60])}</div>',
                    unsafe_allow_html=True,
                )

            def on_done(fname, err):
                rows.append({
                    "icon" : "✗" if err else "✓",
                    "color": "var(--accent)" if err else "var(--success)",
                    "name" : fname, "error": err or "",
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
                account_id=acc.account_id,
                on_progress=on_prog,
                on_file_done=on_done,
            )
            progress_slot.empty()

            # Auto-process after single-account sync
            if sync_r.new + sync_r.updated > 0:
                st.write("Processing ingestion queue…")
                jobs_r = ingestion_service.process_pending_jobs(limit=200)
                sync_status.update(label="Done ✓", state="complete")
                st.success(
                    f"↑ {sync_r.new} new · ↻ {sync_r.updated} updated · "
                    f"→ {sync_r.skipped} skipped · "
                    f"✓ {jobs_r.get('succeeded', 0)} ingested · "
                    f"✗ {sync_r.failed} failed "
                    f"({sync_r.duration_s:.1f}s)"
                )
                if jobs_r.get("failed", 0):
                    st.warning(
                        f"{jobs_r['failed']} job(s) failed. "
                        "Scroll down to **Processing Queue** to retry."
                    )
            else:
                sync_status.update(label="Done ✓", state="complete")
                st.info("No new or changed files.")

            st.rerun()

        except Exception as e:
            sync_status.update(label="Failed", state="error")
            st.error(f"Sync error: {e}")
