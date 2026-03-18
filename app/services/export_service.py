"""
export_service.py — Export PDF metadata to XLSX / DOCX / CSV / JSON.

Aligned with the universal extraction_service.py:
- Reads all enriched schema fields directly (doi, issn, publisher, journal,
  volume, issue, keywords, abstract) — no re-extraction needed for new docs
- Falls back to regex extraction for older documents missing schema fields
- Date parsed from PDF raw format OR year string (from new extraction_service)
- Deduplication by doc_id — same doc never appears twice
- Citation built from clean title + authors + year
- XLSX matches For_Metadata.xlsx column template exactly
- DOCX includes journal + volume/issue in metadata table
- JSON strips internal _fields before output
"""

from __future__ import annotations

import csv
import io
import json
import re
from datetime import datetime
from html import unescape

from app.services.pdf_service import pdf_service
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Column order matching the For_Metadata.xlsx template ─────────────────────
XLSX_COLUMNS = [
    "title", "authors", "editor", "date", "page no",
    "abstract", "sponsor", "citation", "doi", "issn",
    "publisher", "journal", "keywords", "type", "issue", "volume",
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
            raise ImportError("pip install openpyxl")

        rows = self._collect_rows(doc_ids)
        wb   = Workbook()
        ws   = wb.active
        ws.title = "PDF Metadata"

        # ── Header styling ────────────────────────────────────────────────────
        header_font  = Font(name="Arial", bold=True, color="FFFFFF", size=11)
        header_fill  = PatternFill("solid", start_color="1A1A1A")
        accent_fill  = PatternFill("solid", start_color="BF3A14")
        header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
        thin         = Side(style="thin", color="DDDDDD")
        border       = Border(left=thin, right=thin, top=thin, bottom=thin)

        ws.row_dimensions[1].height = 32
        for col_i, col_name in enumerate(XLSX_COLUMNS, start=1):
            cell           = ws.cell(row=1, column=col_i, value=col_name.title().replace(" No", " No."))
            cell.font      = header_font
            cell.fill      = accent_fill if col_i == 1 else header_fill
            cell.alignment = header_align
            cell.border    = border

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
            "title": 45, "authors": 32, "abstract": 65, "journal": 35,
            "keywords": 32, "doi": 32, "citation": 42, "publisher": 25,
        }
        for col_i, col_name in enumerate(XLSX_COLUMNS, start=1):
            ws.column_dimensions[get_column_letter(col_i)].width = col_widths.get(col_name, 16)

        ws.freeze_panes = "A2"

        # ── Summary sheet ─────────────────────────────────────────────────────
        ws2 = wb.create_sheet("Summary")
        ws2["A1"] = "Export Summary"
        ws2["A1"].font = Font(name="Arial", bold=True, size=14)
        ws2["A3"] = "Total Documents"
        ws2["B3"] = len(rows)
        ws2["A4"] = "Exported At"
        ws2["B4"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        ws2["A5"] = "Columns"
        ws2["B5"] = ", ".join(XLSX_COLUMNS)
        for row in [ws2["A3"], ws2["A4"], ws2["A5"]]:
            row.font = Font(name="Arial", bold=True, size=11)

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
        rows   = self._collect_rows(doc_ids)
        buf    = io.StringIO()
        writer = csv.DictWriter(
            buf, fieldnames=XLSX_COLUMNS,
            extrasaction="ignore", lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
        logger.info("CSV export — %d documents", len(rows))
        return buf.getvalue().encode("utf-8-sig"), filename  # utf-8-sig for Excel compatibility

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
            raise ImportError("pip install python-docx")

        doc  = Document()
        rows = self._collect_rows(doc_ids)

        # Cover heading
        title_para = doc.add_heading("PDF Research Analysis Report", level=0)
        title_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
        if title_para.runs:
            title_para.runs[0].font.color.rgb = RGBColor(0xBF, 0x3A, 0x14)

        sub = doc.add_paragraph(
            f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
            f"  ·  Documents: {len(rows)}"
        )
        sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
        if sub.runs:
            sub.runs[0].font.size = Pt(10)
            sub.runs[0].font.color.rgb = RGBColor(0x85, 0x7F, 0x76)

        doc.add_page_break()

        for i, row in enumerate(rows, start=1):
            title = row.get("title") or row.get("_filename", f"Document {i}")
            h = doc.add_heading(f"{i}. {title[:120]}", level=1)
            if h.runs:
                h.runs[0].font.color.rgb = RGBColor(0xBF, 0x3A, 0x14)

            # Metadata table — includes journal, volume/issue
            vol   = row.get("volume", "")
            issue = row.get("issue",  "")
            vol_issue = (f"Vol {vol}" if vol else "") + (f", No {issue}" if issue else "") or "—"

            meta_fields = [
                ("Authors",      row.get("authors",   "") or "—"),
                ("Journal",      row.get("journal",   "") or "—"),
                ("Vol / Issue",  vol_issue),
                ("Date",         row.get("date",      "") or "—"),
                ("Publisher",    row.get("publisher", "") or "—"),
                ("DOI",          row.get("doi",       "") or "—"),
                ("ISSN",         row.get("issn",      "") or "—"),
                ("Pages",        row.get("page no",   "") or "—"),
                ("Keywords",     row.get("keywords",  "") or "—"),
                ("Type",         row.get("type",      "") or "—"),
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
                vc.text = str(value)
                if vc.paragraphs[0].runs:
                    vc.paragraphs[0].runs[0].font.size = Pt(9)
                lc.width = Inches(1.6)

            doc.add_paragraph()

            # Abstract
            abstract = row.get("abstract", "")
            if abstract:
                doc.add_heading("Abstract", level=2)
                p = doc.add_paragraph(abstract[:2000])
                if p.runs:
                    p.runs[0].font.size   = Pt(10)
                    p.runs[0].font.italic = True

            # Citation
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
        rows  = self._collect_rows(doc_ids)
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
        Load metadata for each unique doc_id and map to export columns.

        Priority for each field:
          1. Schema field (populated by new extraction_service)
          2. Section content (e.g. abstract from detected section)
          3. Regex fallback from full_text (for older documents)
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

                m      = doc.metadata
                first3 = "\n".join((doc.full_text or "").split("\n\n")[:60])

                # ── Title ─────────────────────────────────────────────────────
                title = _clean(m.title or "")
                if not title:
                    title = _clean(doc.filename.replace(".pdf", ""))

                # ── Authors ───────────────────────────────────────────────────
                authors_list = [_clean(a) for a in (m.authors or []) if a.strip()]
                if not authors_list:
                    authors_list = _fallback_authors(doc.full_text or "", title)
                authors = "; ".join(authors_list)

                # ── Date ──────────────────────────────────────────────────────
                # created_at is now a year string ("2016") from new extraction_service
                # OR a raw PDF date ("D:20160823...") from older docs
                date = _parse_date(m.created_at or "")

                # ── Abstract — section content takes priority ──────────────────
                abstract = ""
                for sec in (doc.sections or []):
                    if sec.section_type.value == "abstract":
                        abstract = sec.content[:2000].strip()
                        break
                if not abstract:
                    abstract = _clean(m.abstract or "")[:2000]

                # ── Bibliographic — schema first, regex fallback ───────────────
                doi       = m.doi       or _extract_doi(first3)
                issn      = m.issn      or _extract_issn(first3)
                publisher = m.publisher or _extract_publisher(first3)
                journal   = m.journal   or _extract_journal(first3)
                volume    = m.volume    or _extract_pattern(r"\bVol(?:ume)?\.?\s*(\d+)", first3)
                issue     = m.issue     or _extract_pattern(
                    r"\bIssue\.?\s*(\d+)|\bNo\.?\s*(\d+)|\(\s*(\d+)\s*\)", first3
                )

                # ── Keywords ─────────────────────────────────────────────────
                kws = m.keywords or []
                if not kws:
                    kws = _extract_keywords_list(doc.full_text or "")
                keywords = "; ".join(_clean(k) for k in kws if k.strip())

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
                    "journal"  : journal,
                    "keywords" : keywords,
                    "type"     : "Research Paper",
                    "issue"    : issue,
                    "volume"   : volume,
                    # Internal — stripped from JSON, not in XLSX
                    "_filename": doc.filename,
                    "_doc_id"  : doc.doc_id,
                })

            except Exception as e:
                logger.error("Export failed for doc_id=%s: %s", doc_id, e)

        return rows


# ══════════════════════════════════════════════════════════════════════════════
# Helper functions — all pure, no side effects
# ══════════════════════════════════════════════════════════════════════════════

def _clean(text: str) -> str:
    """Decode HTML entities and strip whitespace."""
    return unescape(text).strip()


def _parse_date(raw: str) -> str:
    """
    Convert any date representation to a clean string.

    Handles:
      - Year only:           "2016"           → "2016"
      - PDF raw format:      "D:20160823..."  → "2016-08-23"
      - Human date:          "6th February 2016" → "2016-02-06"
      - ISO:                 "2016-02-06"     → "2016-02-06"
    """
    if not raw:
        return ""
    raw = raw.strip()

    # Plain 4-digit year (output from new extraction_service)
    if re.match(r"^(19|20)\d{2}$", raw):
        return raw

    # PDF raw: D:YYYYMMDDHHmmss...
    m = re.match(r"D:(\d{4})(\d{2})(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # Human readable: "6th February 2016", "Feb 2016", etc.
    raw_clean = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", raw)
    for fmt in ("%d %B %Y", "%B %d %Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%B %Y"):
        try:
            return datetime.strptime(raw_clean.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Extract 4-digit year as last resort
    yr = re.search(r"\b(19|20)\d{2}\b", raw)
    return yr.group(0) if yr else raw[:10]


def _build_citation(title: str, authors: list[str], date: str) -> str:
    """Build an APA-style citation string."""
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


def _fallback_authors(text: str, title: str) -> list[str]:
    """
    Heuristic author extraction from full text when schema field is empty.
    Scans lines after the title, stops at institutional markers.
    """
    lines       = [l.strip() for l in text[:3000].split("\n") if l.strip()]
    title_found = False
    candidates  : list[str] = []
    title_low   = title[:30].lower() if title else ""

    for line in lines:
        if not title_found:
            if title_low and title_low in line.lower():
                title_found = True
            continue

        lower = line.lower()
        if any(kw in lower for kw in [
            "university", "department", "institute", "college",
            "abstract", "introduction", "background",
            "email", "@", "http", "received", "accepted",
            "copyright", "doi", "keywords",
        ]):
            break

        parts  = [p.strip() for p in re.split(r"[,;]", line) if p.strip()]
        proper = [
            p for p in parts
            if re.match(r"^[A-Z][a-z]", p) and 3 < len(p) < 50
        ]
        if proper and len(line) < 200:
            candidates.extend(proper[:6])
            if len(candidates) >= 8:
                break

    return candidates[:8]


def _extract_doi(text: str) -> str:
    # URL form
    m = re.search(r"https?://(?:dx\.)?doi\.org/(10\.\d{4,9}/[^\s\"'<>\]\)]+)", text)
    if m:
        return m.group(1).rstrip(".,;)]")
    # Labelled form
    m = re.search(r"\bdoi\s*:?\s*(10\.\d{4,9}/[^\s\"'<>\]\)]+)", text, re.IGNORECASE)
    if m:
        return m.group(1).rstrip(".,;)]")
    # Bare
    m = re.search(r"\b(10\.\d{4,9}/[^\s\"'<>\]\)]{3,})", text)
    return m.group(1).rstrip(".,;)]") if m else ""


def _extract_issn(text: str) -> str:
    m = re.search(r"\b[EP]-?ISSN[:\s]*(\d{4}-\d{3}[\dXx])\b", text, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"\bISSN[:\s]*(\d{4}-\d{3}[\dXx])\b", text, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(
        r"(?:journal|issn|copyright)[^\n]*\b(\d{4}-\d{3}[\dXx])\b",
        text, re.IGNORECASE,
    )
    if m:
        return m.group(1)
    m = re.search(r"\b(\d{4}-\d{3}[\dXx])\b", text[:1500])
    return m.group(1) if m else ""


def _extract_publisher(text: str) -> str:
    known = [
        "Wolters Kluwer", "Lippincott Williams",
        "Elsevier", "Springer", "Springer Nature",
        "Wiley", "Wiley-Blackwell",
        "Taylor & Francis", "Taylor and Francis",
        "BMJ Publishing", "Sage Publications", "SAGE",
        "Oxford University Press", "Cambridge University Press",
        "PLOS", "BioMed Central", "BMC",
        "Frontiers Media", "MDPI", "Hindawi", "Dove Medical Press",
        "American Chemical Society", "Royal Society of Chemistry",
        "IEEE", "ACM", "Karger", "Thieme",
    ]
    chunk = text[:4000]
    # Explicit label
    m = re.search(r"(?:Published\s+by|Publisher\s*:)\s*([A-Z][^\n]{3,70})", chunk)
    if m:
        pub = re.split(r"\s*(?:Inc\.|Ltd\.?|All rights|Copyright|\d{4})", m.group(1))[0]
        return pub.strip()[:80]
    # Known name
    chunk_l = chunk.lower()
    for pub in sorted(known, key=len, reverse=True):
        if pub.lower() in chunk_l:
            return pub
    return ""


def _extract_journal(text: str) -> str:
    patterns = [
        r"(?:published\s+in|journal\s*:)\s*([A-Z][^\n]{5,100})",
        r"((?:International\s+|European\s+|American\s+|British\s+|African\s+|"
        r"Asian\s+|Nigerian\s+|Indian\s+|Chinese\s+|Korean\s+|"
        r"Canadian\s+|Australian\s+)?(?:Journal|Review|Annals|Archives|"
        r"Bulletin|Proceedings|Transactions|Letters|Reports)\s+(?:of|for|on|in)"
        r"\s+[A-Z][^\n]{3,70})",
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,6}\s+Journal[^\n]{0,30})",
        r"(American Journal of [^\n]{5,60})",
        r"(British Journal of [^\n]{5,60})",
        r"(Asian Journal of [^\n]{5,60})",
        r"(European Journal of [^\n]{5,60})",
    ]
    for pat in patterns:
        m = re.search(pat, text[:5000], re.IGNORECASE)
        if m:
            j = m.group(1).strip()
            j = re.split(r"\s+\d{4}\b|\s+[Vv]ol|\s+\d+\s*[\(,]", j)[0]
            j = j.strip().rstrip(".,;:")
            if len(j) >= 8:
                return j[:100]
    return ""


def _extract_keywords_list(text: str) -> list[str]:
    m = re.search(
        r"(?:Keywords?|Key\s+words?|Index\s+[Tt]erms?)\s*[:—]\s*([^\n]{10,600})",
        text[:10000], re.IGNORECASE,
    )
    if not m:
        return []
    kws = [k.strip().strip("•·-–—") for k in re.split(r"[;,•·]", m.group(1))]
    return [k for k in kws if 2 < len(k) < 80][:15]


def _extract_pattern(pattern: str, text: str) -> str:
    m = re.search(pattern, text[:4000], re.IGNORECASE)
    if m:
        return next((g for g in m.groups() if g), "")
    return ""


# ── Singleton ─────────────────────────────────────────────────────────────────
export_service = ExportService()