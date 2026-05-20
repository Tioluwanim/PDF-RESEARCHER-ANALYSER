from __future__ import annotations

import html
from typing import Iterable

import streamlit as st

from app.services.analysis_service import analysis_service
from app.services.export_service import export_service


def render_export_tab() -> None:
    st.markdown(
        """
        <div style="font-family:var(--f-display);font-size:1.3rem;
        font-weight:600;margin-bottom:0.25rem;">Export</div>
        <div style="font-family:var(--f-mono);font-size:0.7rem;
        color:var(--muted);margin-bottom:1.25rem;">
        Download extracted metadata — XLSX · DOCX · CSV · JSON</div>
        """,
        unsafe_allow_html=True,
    )

    docs = analysis_service.list_documents()
    ready_docs = [
        d for d in docs
        if str(d.get("status", "")).lower() == "ready"
    ]

    if not docs:
        st.info("No documents found in the database. Upload and process PDFs first.")
        return

    if not ready_docs:
        st.info(
            f"{len(docs)} document(s) found, but none are ready for export. "
            "Process documents first, then return to Export."
        )
        return

    template_choice = st.radio(
        "Export template",
        options=["Journal Articles", "PhD Theses"],
        horizontal=True,
        key="export_template",
    )
    template = "thesis" if template_choice == "PhD Theses" else "journal"

    selection_signature = _make_signature(
        ready_docs=[d["doc_id"] for d in ready_docs],
        selected_ids=st.session_state.get("export_selected_ids", []),
        template=template,
    )

    if st.session_state.get("_export_signature") != selection_signature:
        st.session_state.export_data = {}
        st.session_state["_export_signature"] = selection_signature

    st.caption(
        "📋 Journal Articles — authors, DOI, ISSN, journal, volume, issue, keywords"
        if template == "journal"
        else
        "🎓 PhD Theses — author, date, description, abstract, publisher, subject, title, type"
    )

    option_map = {
        f'{d["filename"]} — {d["doc_id"][:8]}': d["doc_id"]
        for d in ready_docs
    }

    selected_labels = st.multiselect(
        "Select documents to export",
        options=list(option_map.keys()),
        default=list(option_map.keys()),
        key="export_select",
    )

    selected_ids = [option_map[label] for label in selected_labels if label in option_map]
    st.session_state.export_selected_ids = selected_ids

    if not selected_ids:
        st.warning("Select at least one document to export.", icon="⚠️")
        return

    st.caption(f"{len(selected_ids)} document(s) selected")

    fname_suffix = "_theses" if template == "thesis" else "_journal"
    export_cache = st.session_state.setdefault("export_data", {})
    export_key_prefix = f"{template}_{'_'.join(selected_ids)}"

    st.markdown("<hr style='margin:1rem 0;'>", unsafe_allow_html=True)

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button(
            "Generate all exports",
            type="primary",
            use_container_width=True,
            key="gen_all_exports",
        ):
            _generate_all_exports(
                selected_ids=selected_ids,
                template=template,
                fname_suffix=fname_suffix,
                export_cache=export_cache,
                export_key_prefix=export_key_prefix,
            )
            st.success("Exports are ready below.")
            st.rerun()

    with col2:
        if st.button(
            "Clear generated files",
            use_container_width=True,
            key="clear_export_cache",
        ):
            export_cache.clear()
            st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)

    cards = st.columns(4)

    _render_export_card(
        col=cards[0],
        title="📊 Excel (XLSX)",
        description="Structured spreadsheet for analysis.",
        button_label="Generate XLSX",
        download_label="⬇ Download XLSX",
        cache_key=f"{export_key_prefix}_xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"metadata_export{fname_suffix}.xlsx",
        generate_fn=lambda: export_service.export_xlsx(
            selected_ids,
            filename=f"metadata_export{fname_suffix}.xlsx",
            template=template,
        ),
        export_cache=export_cache,
    )

    _render_export_card(
        col=cards[1],
        title="📝 Word (DOCX)",
        description="Formatted report per document.",
        button_label="Generate DOCX",
        download_label="⬇ Download DOCX",
        cache_key=f"{export_key_prefix}_docx",
        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename=f"research_report{fname_suffix}.docx",
        generate_fn=lambda: export_service.export_docx(
            selected_ids,
            filename=f"research_report{fname_suffix}.docx",
            template=template,
        ),
        export_cache=export_cache,
    )

    _render_export_card(
        col=cards[2],
        title="📋 CSV",
        description="Plain text, importable anywhere.",
        button_label="Generate CSV",
        download_label="⬇ Download CSV",
        cache_key=f"{export_key_prefix}_csv",
        mime="text/csv",
        filename=f"metadata_export{fname_suffix}.csv",
        generate_fn=lambda: export_service.export_csv(
            selected_ids,
            filename=f"metadata_export{fname_suffix}.csv",
            template=template,
        ),
        export_cache=export_cache,
    )

    _render_export_card(
        col=cards[3],
        title="🔗 JSON",
        description="For API and integration use.",
        button_label="Generate JSON",
        download_label="⬇ Download JSON",
        cache_key=f"{export_key_prefix}_json",
        mime="application/json",
        filename=f"metadata_export{fname_suffix}.json",
        generate_fn=lambda: export_service.export_json(
            selected_ids,
            filename=f"metadata_export{fname_suffix}.json",
            template=template,
        ),
        export_cache=export_cache,
    )

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        """
        <div style="font-family:var(--f-display);font-size:1rem;
        font-weight:600;margin-bottom:0.5rem;">Selected Documents</div>
        """,
        unsafe_allow_html=True,
    )

    rows_html = "".join(
        f'<div class="batch-row">'
        f'<span style="flex:2;">{html.escape(d["filename"][:45])}</span>'
        f'<span style="font-family:var(--f-mono);font-size:0.7rem;color:var(--success);">● ready</span>'
        "</div>"
        for d in ready_docs
        if d["doc_id"] in selected_ids
    )

    if rows_html:
        st.markdown(
            f'<div style="border:1px solid var(--border);border-radius:10px;overflow:hidden;">{rows_html}</div>',
            unsafe_allow_html=True,
        )


def _render_export_card(
    *,
    col,
    title: str,
    description: str,
    button_label: str,
    download_label: str,
    cache_key: str,
    mime: str,
    filename: str,
    generate_fn,
    export_cache: dict,
) -> None:
    with col:
        st.markdown(f"**{title}**")
        st.caption(description)

        if st.button(
            button_label,
            type="primary",
            use_container_width=True,
            key=f"btn_{cache_key}",
        ):
            with st.spinner("Building export…"):
                try:
                    export_cache[cache_key] = generate_fn()
                except Exception as exc:
                    st.error(f"{title} failed: {exc}")

        if cache_key in export_cache:
            data, fname = export_cache[cache_key]
            st.download_button(
                download_label,
                data=data,
                file_name=fname or filename,
                mime=mime,
                use_container_width=True,
                key=f"dl_{cache_key}",
            )


def _generate_all_exports(
    *,
    selected_ids: list[str],
    template: str,
    fname_suffix: str,
    export_cache: dict,
    export_key_prefix: str,
) -> None:
    export_cache[f"{export_key_prefix}_xlsx"] = export_service.export_xlsx(
        selected_ids,
        filename=f"metadata_export{fname_suffix}.xlsx",
        template=template,
    )
    export_cache[f"{export_key_prefix}_docx"] = export_service.export_docx(
        selected_ids,
        filename=f"research_report{fname_suffix}.docx",
        template=template,
    )
    export_cache[f"{export_key_prefix}_csv"] = export_service.export_csv(
        selected_ids,
        filename=f"metadata_export{fname_suffix}.csv",
        template=template,
    )
    export_cache[f"{export_key_prefix}_json"] = export_service.export_json(
        selected_ids,
        filename=f"metadata_export{fname_suffix}.json",
        template=template,
    )


def _make_signature(
    *,
    ready_docs: Iterable[str],
    selected_ids: Iterable[str],
    template: str,
) -> str:
    return "|".join([
        template,
        ",".join(sorted(map(str, ready_docs))),
        ",".join(sorted(map(str, selected_ids))),
    ])