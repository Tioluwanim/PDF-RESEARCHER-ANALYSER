"""
export_service.py - Export extracted PDF metadata and content to xlsx / docx / csv.

Supports three export formats:
  1. XLSX — one row per document, columns match the For_Metadata.xlsx template
  2. DOCX — formatted report with sections per document
  3. CSV  — plain comma-separated, importable anywhere

The XLSX template columns are:
  authors, editor, date, page no, abstract, sponsor, citation,
  doi, issn, publisher, keywords, title, type, issue, volume
"""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from typing import Optional

from app.services.pdf_service import pdf_service
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ── Column order matching the uploaded template ───────────────────────────────
XLSX_COLUMNS = [
    "title", "authors", "editor", "date", "page no",
    "abstract", "sponsor", "citation", "doi", "issn",
    "publisher", "keywords", "type", "issue", "volume",
]


class ExportService:
    """
    Converts processed document metadata into downloadable export files.
    All methods return (bytes, filename) tuples ready for st.download_button.
    """

    # ── Public API ────────────────────────────────────────────────────────────

    def export_xlsx(
        self,
        doc_ids   : list[str],
        filename  : str = "pdf_metadata_export.xlsx",
    ) -> tuple[bytes, str]:
        """
        Export metadata for multiple documents to XLSX.
        Matches the column structure of For_Metadata.xlsx.
        """
        try:
            from openpyxl import Workbook
            from openpyxl.styles import (
                Font, PatternFill, Alignment, Border, Side
            )
            from openpyxl.utils import get_column_letter
        except ImportError:
            raise ImportError("Run: pip install openpyxl")

        rows = self._collect_rows(doc_ids)

        wb   = Workbook()
        ws   = wb.active
        ws.title = "PDF Metadata"

        # ── Header row styling ────────────────────────────────────────────────
        header_font    = Font(name="Arial", bold=True, color="FFFFFF", size=11)
        header_fill    = PatternFill("solid", start_color="1A1A1A")
        header_align   = Alignment(horizontal="center", vertical="center", wrap_text=True)
        accent_fill    = PatternFill("solid", start_color="BF3A14")

        thin = Side(style="thin", color="DDDDDD")
        border = Border(left=thin, right=thin, top=thin, bottom=thin)

        ws.row_dimensions[1].height = 32

        for col_i, col_name in enumerate(XLSX_COLUMNS, start=1):
            cell              = ws.cell(row=1, column=col_i, value=col_name.title())
            cell.font         = header_font
            cell.fill         = header_fill if col_i > 1 else accent_fill
            cell.alignment    = header_align
            cell.border       = border

        # ── Data rows ─────────────────────────────────────────────────────────
        row_fill_even = PatternFill("solid", start_color="F8F5F0")
        row_fill_odd  = PatternFill("solid", start_color="FFFFFF")
        data_font     = Font(name="Arial", size=10)
        data_align    = Alignment(vertical="top", wrap_text=True)

        for row_i, row in enumerate(rows, start=2):
            fill = row_fill_even if row_i % 2 == 0 else row_fill_odd
            ws.row_dimensions[row_i].height = 60
            for col_i, col_name in enumerate(XLSX_COLUMNS, start=1):
                cell           = ws.cell(row=row_i, column=col_i, value=row.get(col_name, ""))
                cell.font      = data_font
                cell.fill      = fill
                cell.alignment = data_align
                cell.border    = border

        # ── Column widths ─────────────────────────────────────────────────────
        col_widths = {
            "title": 40, "authors": 30, "abstract": 60,
            "keywords": 30, "doi": 28, "citation": 35,
        }
        for col_i, col_name in enumerate(XLSX_COLUMNS, start=1):
            width = col_widths.get(col_name, 18)
            ws.column_dimensions[get_column_letter(col_i)].width = width

        # ── Freeze header ─────────────────────────────────────────────────────
        ws.freeze_panes = "A2"

        # ── Summary sheet ─────────────────────────────────────────────────────
        ws2        = wb.create_sheet("Summary")
        ws2["A1"]  = "Export Summary"
        ws2["A1"].font = Font(name="Arial", bold=True, size=14)
        ws2["A3"]  = "Total Documents"
        ws2["B3"]  = len(rows)
        ws2["A4"]  = "Exported At"
        ws2["B4"]  = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        ws2["A5"]  = "Columns"
        ws2["B5"]  = ", ".join(XLSX_COLUMNS)

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        logger.info("XLSX export — %d documents", len(rows))
        return buf.read(), filename

    def export_csv(
        self,
        doc_ids  : list[str],
        filename : str = "pdf_metadata_export.csv",
    ) -> tuple[bytes, str]:
        """Export metadata as CSV, columns match the XLSX template."""
        rows = self._collect_rows(doc_ids)

        buf    = io.StringIO()
        writer = csv.DictWriter(
            buf,
            fieldnames  = XLSX_COLUMNS,
            extrasaction= "ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)

        logger.info("CSV export — %d documents", len(rows))
        return buf.getvalue().encode("utf-8"), filename

    def export_docx(
        self,
        doc_ids  : list[str],
        filename : str = "pdf_research_report.docx",
    ) -> tuple[bytes, str]:
        """
        Export a formatted Word document with one section per PDF.
        Includes metadata table + abstract + section summary per document.
        """
        try:
            from docx import Document
            from docx.shared import Pt, RGBColor, Inches
            from docx.enum.text import WD_ALIGN_PARAGRAPH
        except ImportError:
            raise ImportError("Run: pip install python-docx")

        doc  = Document()
        rows = self._collect_rows(doc_ids)

        # ── Document title ─────────────────────────────────────────────────
        title_para = doc.add_heading("PDF Research Analysis Report", level=0)
        title_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
        if title_para.runs:
            title_para.runs[0].font.color.rgb = RGBColor(0xBF, 0x3A, 0x14)

        meta_para = doc.add_paragraph(
            f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}  ·  "
            f"Documents: {len(rows)}"
        )
        meta_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
        if meta_para.runs:
            meta_para.runs[0].font.size = Pt(10)
            meta_para.runs[0].font.color.rgb = RGBColor(0x85, 0x7F, 0x76)

        doc.add_page_break()

        # ── Per-document sections ──────────────────────────────────────────
        for i, row in enumerate(rows, start=1):
            title = row.get("title") or row.get("_filename", f"Document {i}")

            h = doc.add_heading(f"{i}. {title[:120]}", level=1)
            if h.runs:
                h.runs[0].font.color.rgb = RGBColor(0xBF, 0x3A, 0x14)

            # Metadata table
            meta_fields = [
                ("Authors",    row.get("authors", "—")),
                ("Date",       row.get("date",    "—")),
                ("Publisher",  row.get("publisher","—")),
                ("DOI",        row.get("doi",     "—")),
                ("ISSN",       row.get("issn",    "—")),
                ("Keywords",   row.get("keywords","—")),
                ("Pages",      row.get("page no", "—")),
                ("Type",       row.get("type",    "—")),
            ]

            table = doc.add_table(rows=len(meta_fields), cols=2)
            table.style = "Table Grid"

            for r_i, (label, value) in enumerate(meta_fields):
                label_cell = table.cell(r_i, 0)
                value_cell = table.cell(r_i, 1)

                label_cell.text = label
                if label_cell.paragraphs[0].runs:
                    label_run = label_cell.paragraphs[0].runs[0]
                    label_run.bold = True
                    label_run.font.size = Pt(9)
                    label_run.font.color.rgb = RGBColor(0x0D, 0x0C, 0x0B)

                value_cell.text = str(value) if value else "—"
                if value_cell.paragraphs[0].runs:
                    value_cell.paragraphs[0].runs[0].font.size = Pt(9)

                label_cell.width = Inches(1.5)

            doc.add_paragraph()

            # Abstract
            abstract = row.get("abstract", "")
            if abstract:
                doc.add_heading("Abstract", level=2)
                abs_para = doc.add_paragraph(abstract[:1500])
                if abs_para.runs:
                    abs_para.runs[0].font.size = Pt(10)
                    abs_para.runs[0].font.italic = True

            # Citation
            citation = row.get("citation", "")
            if citation:
                doc.add_heading("Citation", level=2)
                cit_para = doc.add_paragraph(citation)
                if cit_para.runs:
                    cit_para.runs[0].font.size = Pt(9)

            if i < len(rows):
                doc.add_page_break()

        buf = io.BytesIO()
        doc.save(buf)
        buf.seek(0)
        logger.info("DOCX export — %d documents", len(rows))
        return buf.read(), filename

    def export_json(
        self,
        doc_ids  : list[str],
        filename : str = "pdf_metadata_export.json",
    ) -> tuple[bytes, str]:
        """Export full metadata as JSON — useful for API integration."""
        rows = self._collect_rows(doc_ids)
        data = {
            "exported_at": datetime.utcnow().isoformat(),
            "total": len(rows),
            "documents": rows,
        }
        out = json.dumps(data, ensure_ascii=False, indent=2)
        logger.info("JSON export — %d documents", len(rows))
        return out.encode("utf-8"), filename

    # ── Private helpers ───────────────────────────────────────────────────────

    def _collect_rows(self, doc_ids: list[str]) -> list[dict]:
        """Load metadata for each doc_id and map to export column names."""
        rows: list[dict] = []
        for doc_id in doc_ids:
            try:
                doc = pdf_service.load_document(doc_id)
                if not doc:
                    continue
                m = doc.metadata
                rows.append({
                    "title"    : m.title or "",
                    "authors"  : "; ".join(m.authors) if m.authors else "",
                    "editor"   : "",           # not extracted — blank
                    "date"     : m.created_at or "",
                    "page no"  : str(m.page_count) if m.page_count else "",
                    "abstract" : m.abstract or "",
                    "sponsor"  : "",           # not in PDF metadata
                    "citation" : self._build_citation(m),
                    "doi"      : "",           # future: regex extraction
                    "issn"     : "",           # future: regex extraction
                    "publisher": "",           # future: regex extraction
                    "keywords" : "; ".join(m.keywords) if m.keywords else "",
                    "type"     : "Research Paper",
                    "issue"    : "",
                    "volume"   : "",
                    # Internal fields for DOCX (not exported to XLSX)
                    "_filename": doc.filename,
                    "_doc_id"  : doc.doc_id,
                    "_words"   : m.word_count,
                    "_status"  : doc.status.value,
                })
            except Exception as e:
                logger.error("Export failed for doc_id=%s: %s", doc_id, e)
        return rows

    @staticmethod
    def _build_citation(m) -> str:
        """Build a basic citation string from available metadata."""
        parts = []
        if m.authors:
            parts.append("; ".join(m.authors[:3]))
            if len(m.authors) > 3:
                parts[-1] += " et al."
        if m.title:
            parts.append(f'"{m.title}"')
        if m.created_at:
            year = m.created_at[:4] if len(m.created_at) >= 4 else m.created_at
            parts.append(f"({year})")
        return ". ".join(parts) if parts else ""


# ── Singleton ─────────────────────────────────────────────────────────────────
export_service = ExportService()