from __future__ import annotations

import html
import streamlit as st
from app.services.batch_service import batch_service


def render_batch_tab() -> None:
    st.markdown(
        '<div style="font-family:var(--f-display);font-size:1.3rem;'
        'font-weight:600;margin-bottom:0.25rem;">Batch Upload</div>'
        '<div style="font-family:var(--f-mono);font-size:0.7rem;'
        'color:var(--muted);margin-bottom:1.5rem;">Upload 1–50 PDFs and process them all at once</div>',
        unsafe_allow_html=True,
    )

    uploaded_files = st.file_uploader(
        "Drop files here",
        type=["pdf","docx","doc","txt","xlsx","xls","csv"],
        accept_multiple_files=True,
        label_visibility="collapsed",
        key="batch_uploader",
    )

    if not uploaded_files:
        st.markdown(
            """
            <div style="text-align:center;padding:3rem 1rem;color:#a09890;font-size:0.85rem;">
                <div style="font-size:2.5rem;margin-bottom:0.75rem;">📚</div>
                Drop multiple files above — PDF, DOCX, TXT, XLSX, CSV supported.<br>
                Up to 50 files at a time. Each will be extracted and indexed automatically.
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    count = len(uploaded_files)
    if count > 50:
        st.error(f"Maximum 50 PDFs per batch. You selected {count}. Please remove some files.")
        return

    st.markdown(
        f'<div style="font-family:var(--f-mono);font-size:0.75rem;color:var(--muted);margin-bottom:1rem;">'
        f'{count} file{"s" if count != 1 else ""} selected</div>',
        unsafe_allow_html=True,
    )

    preview_rows = "".join(
        f'<div class="batch-row">'
        f'<span>📄</span>'
        f'<span style="flex:1;">{html.escape(f.name[:55])}</span>'
        f'<span style="font-family:var(--f-mono);font-size:0.7rem;color:var(--muted);">{_file_size_kb(f)}</span>'
        '</div>'
        for f in uploaded_files[:15]
    )
    if count > 15:
        preview_rows += (
            f'<div class="batch-row" style="color:var(--muted);font-style:italic;">'
            f'… and {count - 15} more files</div>'
        )
    st.markdown(
        f'<div style="border:1px solid var(--border);border-radius:10px;overflow:hidden;margin-bottom:1rem;">{preview_rows}</div>',
        unsafe_allow_html=True,
    )

    if st.button(f"▶ Process All {count} PDFs", type="primary", key="batch_run"):
        _run_batch(uploaded_files)


def _file_size_kb(uploaded_file) -> str:
    try:
        data = uploaded_file.read()
        uploaded_file.seek(0)
        return f"{len(data)/1024:.1f} KB"
    except Exception:
        return "?"


def _run_batch(uploaded_files) -> None:
    total = len(uploaded_files)
    bar = st.progress(0, text="Starting batch …")
    status_el = st.empty()
    results_el = st.empty()
    rows: list[dict] = []

    def on_start(current: int, total: int, filename: str) -> None:
        pct = max(0.0, min((current - 1) / total, 1.0))
        bar.progress(pct, text=f"[{current}/{total}] {filename[:40]} …")
        status_el.markdown(
            f'<div style="font-family:var(--f-mono);font-size:0.72rem;color:var(--muted);">Processing: {html.escape(filename)}</div>',
            unsafe_allow_html=True,
        )

    def on_done(item) -> None:
        icon = "✓" if item.status == "ready" else "✗"
        color = "var(--success)" if item.status == "ready" else "var(--accent)"
        rows.append({
            "icon": icon,
            "color": color,
            "filename": item.filename,
            "status": item.status,
            "pages": item.pages,
            "words": item.words,
            "chunks": item.chunks,
            "error": item.error,
        })
        html_rows = "".join(
            f'<div class="batch-row">'
            f'<span style="color:{r["color"]};font-weight:700;min-width:1rem;">{r["icon"]}</span>'
            f'<span style="flex:1;">{html.escape(r["filename"][:40])}</span>'
            f'<span style="font-family:var(--f-mono);font-size:0.7rem;color:var(--muted);">'
            + (f'{r["pages"]}p · {r["words"]:,}w · {r["chunks"]} chunks' if r["status"] == "ready" else f'<span style="color:var(--accent);">{html.escape(r["error"][:40])}</span>')
            + '</span></div>'
            for r in rows
        )
        results_el.markdown(
            f'<div style="border:1px solid var(--border);border-radius:10px;overflow:hidden;margin-top:0.5rem;">{html_rows}</div>',
            unsafe_allow_html=True,
        )

    files = []
    for f in uploaded_files:
        try:
            files.append((f.getvalue(), f.name))
        except Exception:
            files.append((f.read(), f.name))

    result = batch_service.process_batch(files, on_item_start=on_start, on_item_done=on_done)
    bar.progress(1.0, text="Batch complete ✓")
    status_el.empty()

    st.success(
        f"✓ Batch complete — {result.succeeded}/{result.total} succeeded in {result.duration_s:.1f}s"
    )
    if result.failed > 0:
        failed_names = [i.filename for i in result.items if i.status == "failed"]
        st.warning(
            f"{result.failed} file(s) failed: {', '.join(failed_names[:5])}" + (" …" if len(failed_names) > 5 else "")
        )

    for item in result.items:
        if item.status == "ready" and item.doc_id:
            st.session_state.active_doc_id = item.doc_id
            break

    st.rerun()
