from __future__ import annotations

import html
import streamlit as st
from app.services.analysis_service import analysis_service
from app.services.export_service import export_service


def render_export_tab() -> None:
    st.markdown(
        '<div style="font-family:var(--f-display);font-size:1.3rem;'
        'font-weight:600;margin-bottom:0.25rem;">Export</div>'
        '<div style="font-family:var(--f-mono);font-size:0.7rem;'
        'color:var(--muted);margin-bottom:1.5rem;">'
        'Download extracted metadata — XLSX · DOCX · CSV · JSON</div>',
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

    all_options = [
        f'{d["filename"]} — {d["doc_id"][:8]}'
        for d in ready_docs
    ]
    option_to_id = {
        option: d["doc_id"]
        for option, d in zip(all_options, ready_docs)
    }

    selected = st.multiselect(
        "Select documents to export",
        options=all_options,
        default=all_options,
        key="export_select",
    )

    selected_ids = [option_to_id[name] for name in selected if name in option_to_id]
    if not selected_ids:
        st.warning("Select at least one document to export.", icon="⚠️")
        return

    st.caption(f"{len(selected_ids)} document(s) selected")
    st.markdown("<br>", unsafe_allow_html=True)

    template_choice = st.radio(
        "Export template",
        options=["Journal Articles", "PhD Theses"],
        horizontal=True,
        key="export_template",
    )
    template = "thesis" if template_choice == "PhD Theses" else "journal"
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

    c1, c2, c3, c4 = st.columns(4)
    if "export_data" not in st.session_state:
        st.session_state.export_data = {}
    export_cache = st.session_state.export_data
    fname_suffix = "_theses" if template == "thesis" else "_journal"

    with c1:
        st.markdown("**📊 Excel (XLSX)**")
        st.caption("Matches your metadata template")
        if st.button("Generate XLSX", type="primary", width="stretch", key="gen_xlsx"):
            with st.spinner("Building …"):
                try:
                    export_cache["xlsx"] = export_service.export_xlsx(
                        selected_ids,
                        filename=f"metadata_export{fname_suffix}.xlsx",
                        template=template,
                    )
                except Exception as exc:
                    st.error(f"XLSX export failed: {exc}")
        if "xlsx" in export_cache:
            data, fname = export_cache["xlsx"]
            st.download_button(
                "⬇ Download XLSX", data=data, file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width="stretch", key="dl_xlsx",
            )

    with c2:
        st.markdown("**📝 Word (DOCX)**")
        st.caption("Formatted report per document")
        if st.button("Generate DOCX", type="primary", width="stretch", key="gen_docx"):
            with st.spinner("Building …"):
                try:
                    export_cache["docx"] = export_service.export_docx(
                        selected_ids,
                        filename=f"research_report{fname_suffix}.docx",
                        template=template,
                    )
                except Exception as exc:
                    st.error(f"DOCX export failed: {exc}")
        if "docx" in export_cache:
            data, fname = export_cache["docx"]
            st.download_button(
                "⬇ Download DOCX", data=data, file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                width="stretch", key="dl_docx",
            )

    with c3:
        st.markdown("**📋 CSV**")
        st.caption("Plain text, importable anywhere")
        if st.button("Generate CSV", type="primary", width="stretch", key="gen_csv"):
            with st.spinner("Building …"):
                try:
                    export_cache["csv"] = export_service.export_csv(
                        selected_ids,
                        filename=f"metadata_export{fname_suffix}.csv",
                        template=template,
                    )
                except Exception as exc:
                    st.error(f"CSV export failed: {exc}")
        if "csv" in export_cache:
            data, fname = export_cache["csv"]
            st.download_button(
                "⬇ Download CSV", data=data, file_name=fname,
                mime="text/csv", width="stretch", key="dl_csv",
            )

    with c4:
        st.markdown("**🔗 JSON**")
        st.caption("For API / integration use")
        if st.button("Generate JSON", type="primary", width="stretch", key="gen_json"):
            with st.spinner("Building …"):
                try:
                    export_cache["json"] = export_service.export_json(
                        selected_ids,
                        filename=f"metadata_export{fname_suffix}.json",
                        template=template,
                    )
                except Exception as exc:
                    st.error(f"JSON export failed: {exc}")
        if "json" in export_cache:
            data, fname = export_cache["json"]
            st.download_button(
                "⬇ Download JSON", data=data, file_name=fname,
                mime="application/json", width="stretch", key="dl_json",
            )

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '<div style="font-family:var(--f-display);font-size:1rem;font-weight:600;margin-bottom:0.5rem;">Selected Documents</div>',
        unsafe_allow_html=True,
    )
    rows_html = "".join(
        f'<div class="batch-row">'
        f'<span style="flex:2;">{html.escape(d["filename"][:45])}</span>'
        f'<span style="font-family:var(--f-mono);font-size:0.7rem;color:var(--success);">● ready</span>'
        '</div>'
        for d in ready_docs
        if option_to_id.get(f'{d["filename"]} — {d["doc_id"][:8]}') in selected_ids
    )
    if rows_html:
        st.markdown(
            f'<div style="border:1px solid var(--border);border-radius:10px;overflow:hidden;">{rows_html}</div>',
            unsafe_allow_html=True,
        )