"""
export_service.py - Export extracted PDF metadata to xlsx / docx / csv / json.

Fixes applied:
- HTML entity decoding in title/authors
- PDF date parsing (D:20160823... → 2016-08-23)
- DOI/ISSN/Publisher/Keywords extracted via regex from full_text
- Authors extracted from full_text when metadata author field is blank
- Abstract pulled from the ABSTRACT section (not body/references)
- Citation built from clean parsed fields
- Deduplication by doc_id
"""

from __future__ import annotations

import csv
import io
import json
import re
from datetime import datetime
from html import unescape
from typing import Optional

from app.services.pdf_service import pdf_service
from app.utils.logger import get_logger

logger = get_logger(__name__)

XLSX_COLUMNS = [
    "title", "authors", "editor", "date", "page no",
    "abstract", "sponsor", "citation", "doi", "issn",
    "publisher", "keywords", "type", "issue", "volume",
]


class ExportService:

    # ── Public API ────────────────────────────────────────────────────────────

    def export_xlsx(
        self,
        doc_ids  : list[str],
        filename : str = "pdf_metadata_export.xlsx",
    ) -> tuple[bytes, str]:
        try:
            from openpyxl import Workbook
            from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
            from openpyxl.utils import get_column_letter
        except ImportError:
            raise ImportError("Run: pip install openpyxl")

        rows = self._collect_rows(doc_ids)
        wb   = Workbook()
        ws   = wb.active
        ws.title = "PDF Metadata"

        header_font  = Font(name="Arial", bold=True, color="FFFFFF", size=11)
        header_fill  = PatternFill("solid", start_color="1A1A1A")
        accent_fill  = PatternFill("solid", start_color="BF3A14")
        header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
        thin         = Side(style="thin", color="DDDDDD")
        border       = Border(left=thin, right=thin, top=thin, bottom=thin)

        ws.row_dimensions[1].height = 32
        for col_i, col_name in enumerate(XLSX_COLUMNS, start=1):
            cell           = ws.cell(row=1, column=col_i, value=col_name.title())
            cell.font      = header_font
            cell.fill      = accent_fill if col_i == 1 else header_fill
            cell.alignment = header_align
            cell.border    = border

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

        col_widths = {
            "title": 45, "authors": 32, "abstract": 65,
            "keywords": 32, "doi": 30, "citation": 40, "publisher": 25,
        }
        for col_i, col_name in enumerate(XLSX_COLUMNS, start=1):
            ws.column_dimensions[get_column_letter(col_i)].width = col_widths.get(col_name, 18)

        ws.freeze_panes = "A2"

        ws2       = wb.create_sheet("Summary")
        ws2["A1"] = "Export Summary"
        ws2["A1"].font = Font(name="Arial", bold=True, size=14)
        ws2["A3"] = "Total Documents"
        ws2["B3"] = len(rows)
        ws2["A4"] = "Exported At"
        ws2["B4"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        ws2["A5"] = "Columns"
        ws2["B5"] = ", ".join(XLSX_COLUMNS)

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
        rows = self._collect_rows(doc_ids)
        buf  = io.StringIO()
        writer = csv.DictWriter(
            buf, fieldnames=XLSX_COLUMNS,
            extrasaction="ignore", lineterminator="\n",
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
        try:
            from docx import Document
            from docx.shared import Pt, RGBColor, Inches
            from docx.enum.text import WD_ALIGN_PARAGRAPH
        except ImportError:
            raise ImportError("Run: pip install python-docx")

        doc  = Document()
        rows = self._collect_rows(doc_ids)

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

        for i, row in enumerate(rows, start=1):
            title = row.get("title") or row.get("_filename", f"Document {i}")
            h = doc.add_heading(f"{i}. {title[:120]}", level=1)
            if h.runs:
                h.runs[0].font.color.rgb = RGBColor(0xBF, 0x3A, 0x14)

            meta_fields = [
                ("Authors",   row.get("authors",   "—")),
                ("Date",      row.get("date",       "—")),
                ("Publisher", row.get("publisher",  "—")),
                ("DOI",       row.get("doi",        "—")),
                ("ISSN",      row.get("issn",       "—")),
                ("Keywords",  row.get("keywords",   "—")),
                ("Pages",     row.get("page no",    "—")),
                ("Type",      row.get("type",       "—")),
            ]
            table = doc.add_table(rows=len(meta_fields), cols=2)
            table.style = "Table Grid"
            for r_i, (label, value) in enumerate(meta_fields):
                lc = table.cell(r_i, 0)
                vc = table.cell(r_i, 1)
                lc.text = label
                if lc.paragraphs[0].runs:
                    lc.paragraphs[0].runs[0].bold = True
                    lc.paragraphs[0].runs[0].font.size = Pt(9)
                vc.text = str(value) if value else "—"
                if vc.paragraphs[0].runs:
                    vc.paragraphs[0].runs[0].font.size = Pt(9)
                lc.width = Inches(1.5)

            doc.add_paragraph()

            abstract = row.get("abstract", "")
            if abstract:
                doc.add_heading("Abstract", level=2)
                p = doc.add_paragraph(abstract[:2000])
                if p.runs:
                    p.runs[0].font.size    = Pt(10)
                    p.runs[0].font.italic  = True

            citation = row.get("citation", "")
            if citation:
                doc.add_heading("Citation", level=2)
                p = doc.add_paragraph(citation)
                if p.runs:
                    p.runs[0].font.size = Pt(9)

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
        rows = self._collect_rows(doc_ids)
        # Remove internal _fields before JSON export
        clean = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
        data  = {
            "exported_at": datetime.utcnow().isoformat(),
            "total"      : len(clean),
            "documents"  : clean,
        }
        logger.info("JSON export — %d documents", len(clean))
        return json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"), filename

    # ── Core data collection ──────────────────────────────────────────────────

    def _collect_rows(self, doc_ids: list[str]) -> list[dict]:
        """
        Load and enrich metadata for each unique doc_id.
        Reads new schema fields (doi, issn, publisher, volume, issue) directly.
        Falls back to regex extraction from full_text for older documents.
        """
        seen: set[str] = set()
        rows: list[dict] = []

        for doc_id in doc_ids:
            if doc_id in seen:
                continue
            seen.add(doc_id)

            try:
                doc = pdf_service.load_document(doc_id)
                if not doc:
                    continue

                m         = doc.metadata
                full_text = doc.full_text or ""
                first3    = "\n".join((doc.full_text or "").split("\n\n")[:60])

                # ── Title ─────────────────────────────────────────────────────
                title = _clean_text(m.title or "")
                if not title:
                    title = _clean_text(doc.filename.replace(".pdf", ""))

                # ── Authors ───────────────────────────────────────────────────
                authors_list = [_clean_text(a) for a in (m.authors or []) if a.strip()]
                if not authors_list:
                    authors_list = _extract_authors_from_text(full_text, title)
                authors = "; ".join(authors_list)

                # ── Date ──────────────────────────────────────────────────────
                date = _parse_pdf_date(m.created_at or "")

                # ── Abstract ──────────────────────────────────────────────────
                abstract = ""
                for section in (doc.sections or []):
                    if section.section_type.value == "abstract":
                        abstract = section.content[:2000].strip()
                        break
                if not abstract:
                    abstract = _clean_text(m.abstract or "")[:2000]

                # ── Bibliographic fields — prefer new schema fields ────────────
                doi       = m.doi       or _extract_doi(first3)
                issn      = m.issn      or _extract_issn(first3)
                publisher = m.publisher or _extract_publisher(first3)
                volume    = m.volume    or _extract_pattern(r"\bVol(?:ume)?\.?\s*(\d+)", first3)
                issue     = m.issue     or _extract_pattern(r"\bIssue\.?\s*(\d+)|\bNo\.?\s*(\d+)", first3)
                journal   = m.journal   or _extract_journal(first3)

                # ── Keywords ─────────────────────────────────────────────────
                keywords_list = m.keywords or []
                if not keywords_list:
                    keywords_list = _extract_keywords_list(full_text)
                keywords = "; ".join(keywords_list)

                # ── Citation ──────────────────────────────────────────────────
                citation = _build_citation(title, authors_list, date)

                rows.append({
                    "title"    : title,
                    "authors"  : authors,
                    "editor"   : "",
                    "date"     : date,
                    "page no"  : str(m.page_count) if m.page_count else "",
                    "abstract" : abstract,
                    "sponsor"  : "",
                    "citation" : citation,
                    "doi"      : doi,
                    "issn"     : issn,
                    "publisher": publisher,
                    "keywords" : keywords,
                    "type"     : "Research Paper",
                    "issue"    : issue,
                    "volume"   : volume,
                    "journal"  : journal,
                    "_filename": doc.filename,
                    "_doc_id"  : doc.doc_id,
                })

            except Exception as e:
                logger.error("Export failed for doc_id=%s: %s", doc_id, e)

        return rows


# ── Extraction helpers ────────────────────────────────────────────────────────

def _clean_text(text: str) -> str:
    """Decode HTML entities, strip whitespace."""
    return unescape(text).strip()


def _parse_pdf_date(raw: str) -> str:
    """
    Convert PDF date string to YYYY-MM-DD.
    Formats: 'D:20160823152638+05\'30\'' or '6th February 2016' or ISO.
    """
    if not raw:
        return ""

    # PDF format: D:YYYYMMDDHHmmss...
    m = re.match(r"D:(\d{4})(\d{2})(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # Try parsing human dates like "6th February 2016"
    raw_clean = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", raw)
    for fmt in ("%d %B %Y", "%B %d %Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw_clean.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Just extract 4-digit year if nothing else works
    yr = re.search(r"\b(19|20)\d{2}\b", raw)
    return yr.group(0) if yr else raw[:10]


def _extract_doi(text: str) -> str:
    """Extract DOI using standard pattern."""
    m = re.search(r"\b(10\.\d{4,9}/[^\s\"\'<>]+)", text, re.IGNORECASE)
    return m.group(1).rstrip(".,;)") if m else ""


def _extract_issn(text: str) -> str:
    """Extract ISSN — format: XXXX-XXXX."""
    m = re.search(r"\bISSN[:\s]*(\d{4}-\d{3}[\dXx])\b", text, re.IGNORECASE)
    if m:
        return m.group(1)
    # Bare ISSN without label
    m = re.search(r"\b(\d{4}-\d{3}[\dXx])\b", text)
    return m.group(1) if m else ""


def _extract_publisher(text: str) -> str:
    """Extract publisher name from common patterns."""
    patterns = [
        r"(?:Published|Publisher)[:\s]+([A-Z][^\n]{3,60})",
        r"([A-Z][a-z]+ (?:Press|Publishing|Publishers|Journal|Journals|Society|Elsevier|Springer|Wiley|Taylor|Nature|Sage|BMJ|Oxford|Cambridge)[^\n]{0,40})",
    ]
    for pat in patterns:
        m = re.search(pat, text[:3000])
        if m:
            return m.group(1).strip()[:80]
    return ""


def _extract_keywords(text: str, existing: list[str]) -> str:
    """
    Return existing keywords if present, otherwise extract from text.
    """
    if existing:
        return "; ".join(_clean_text(k) for k in existing if k.strip())

    # Look for 'Keywords:' section
    m = re.search(
        r"(?:Keywords?|Key\s+words?)[:\s]+([^\n]{10,300})",
        text[:5000], re.IGNORECASE,
    )
    if m:
        raw = m.group(1).strip()
        # Split on semicolons or commas
        kws = [k.strip() for k in re.split(r"[;,]", raw) if k.strip()]
        return "; ".join(kws[:10])
    return ""


def _extract_authors_from_text(text: str, title: str) -> list[str]:
    """
    Heuristic author extraction from first page of text.
    Looks for lines with proper names after the title.
    """
    lines = [l.strip() for l in text[:3000].split("\n") if l.strip()]
    title_found = False
    candidates: list[str] = []

    for line in lines:
        # Find title line
        if not title_found:
            if title and title[:30].lower() in line.lower():
                title_found = True
            continue

        lower = line.lower()
        # Stop at institution/abstract markers
        if any(kw in lower for kw in [
            "university", "department", "institute", "abstract",
            "introduction", "background", "email", "@", "http",
            "received", "accepted", "copyright", "doi", "keywords",
        ]):
            break

        # Lines that look like author lists (proper names, comma-separated)
        parts = [p.strip() for p in re.split(r"[,;]", line) if p.strip()]
        proper = [p for p in parts if re.match(r"^[A-Z][a-z]", p) and len(p) > 3]
        if len(proper) >= 1 and len(line) < 200:
            candidates.extend(proper[:6])
            if len(candidates) >= 8:
                break

    return candidates[:8]


def _build_citation(title: str, authors: list[str], date: str) -> str:
    """Build a clean APA-style citation."""
    parts = []
    if authors:
        author_str = "; ".join(authors[:3])
        if len(authors) > 3:
            author_str += " et al."
        parts.append(author_str)
    if title:
        parts.append(f'"{title}"')
    if date:
        year = date[:4] if len(date) >= 4 else date
        parts.append(f"({year})")
    return ". ".join(parts)


def _extract_keywords_list(text: str) -> list[str]:
    m = re.search(
        r"(?:Keywords?|Key\s+words?)[:\s]+([^\n]{10,400})",
        text[:6000], re.IGNORECASE,
    )
    if m:
        return [k.strip() for k in re.split(r"[;,]", m.group(1)) if k.strip()][:12]
    return []


def _extract_journal(text: str) -> str:
    patterns = [
        r"(?:Journal of|Journal for|International Journal of|Asian Journal of)\s+[A-Z][^\n]{5,60}",
        r"([A-Z][a-z]+(?: [A-Z][a-z]+){1,5} Journal[^\n]{0,30})",
    ]
    for pat in patterns:
        m = re.search(pat, text[:3000])
        if m:
            return m.group(0).strip()[:80]
    return ""


def _extract_pattern(pattern: str, text: str) -> str:
    m = re.search(pattern, text[:3000], re.IGNORECASE)
    if m:
        return next((g for g in m.groups() if g), "")
    return ""


# ── Singleton ─────────────────────────────────────────────────────────────────
export_service = ExportService()