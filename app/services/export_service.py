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
- JSON strips internal _fields before output (opt-in full export available)

Fixes over v1:
- vol_issue dead variable removed; DOCX uses a single shared helper
- _extract_pattern() uses named groups to avoid silent wrong-group selection
- _fallback_authors() no longer splits on commas inside author names
- _build_citation() initials handles hyphenated first names correctly
- Failed documents produce an error sentinel row so exports are traceable
- Author deduplication across schema + fallback paths
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

# ── Exact column order matching For_Metadata.xlsx ────────────────────────────
XLSX_COLUMNS = [
    "name original",  # PDF filename
    "authors",        # separated by ||
    "editor",         # journal editor
    "date",           # publication year / full date
    "page no",        # page range e.g. 7-14 or e398-e404
    "abstract",       # full abstract
    "sponsor",        # funding / sponsor statement
    "citation",       # formatted citation
    "doi",            # DOI
    "issn",           # ISSN
    "publisher",      # publisher name
    "keywords",       # separated by ||
    "title",          # paper title
    "type",           # article type
    "issue",          # journal issue
    "volume",         # journal volume
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
        error_fill    = PatternFill("solid", start_color="FFF0F0")
        data_font     = Font(name="Arial", size=10)
        error_font    = Font(name="Arial", size=10, color="CC0000", italic=True)
        data_align    = Alignment(vertical="top", wrap_text=True)

        for row_i, row in enumerate(rows, start=2):
            is_error = row.get("_error", False)
            if is_error:
                fill = error_fill
                font = error_font
            else:
                fill = row_fill_even if row_i % 2 == 0 else row_fill_odd
                font = data_font

            ws.row_dimensions[row_i].height = 60
            for col_i, col_name in enumerate(XLSX_COLUMNS, start=1):
                cell           = ws.cell(row=row_i, column=col_i, value=row.get(col_name, ""))
                cell.font      = font
                cell.fill      = fill
                cell.alignment = data_align
                cell.border    = border

        # ── Column widths ─────────────────────────────────────────────────────
        col_widths = {
            "name original": 28, "authors": 36, "editor": 22,
            "date": 12,          "page no": 12, "abstract": 60,
            "sponsor": 30,       "citation": 45, "doi": 32,
            "issn": 14,          "publisher": 24, "keywords": 32,
            "title": 45,         "type": 20,    "issue": 10, "volume": 10,
        }
        for col_i, col_name in enumerate(XLSX_COLUMNS, start=1):
            ws.column_dimensions[get_column_letter(col_i)].width = col_widths.get(col_name, 16)

        ws.freeze_panes = "A2"

        # ── Summary sheet ─────────────────────────────────────────────────────
        ws2 = wb.create_sheet("Summary")
        ws2["A1"] = "Export Summary"
        ws2["A1"].font = Font(name="Arial", bold=True, size=14)

        ok_count  = sum(1 for r in rows if not r.get("_error"))
        err_count = len(rows) - ok_count

        ws2["A3"] = "Total Documents"
        ws2["B3"] = len(rows)
        ws2["A4"] = "Exported OK"
        ws2["B4"] = ok_count
        ws2["A5"] = "Errors"
        ws2["B5"] = err_count
        ws2["A6"] = "Exported At"
        ws2["B6"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        ws2["A7"] = "Columns"
        ws2["B7"] = ", ".join(XLSX_COLUMNS)

        for cell in [ws2["A3"], ws2["A4"], ws2["A5"], ws2["A6"], ws2["A7"]]:
            cell.font = Font(name="Arial", bold=True, size=11)

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        logger.info("XLSX export — %d ok, %d errors", ok_count, err_count)
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
        ok_count = sum(1 for r in rows if not r.get("_error"))
        logger.info("CSV export — %d ok, %d errors", ok_count, len(rows) - ok_count)
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
            from docx.oxml.ns import qn
            from docx.oxml import OxmlElement
        except ImportError:
            raise ImportError("pip install python-docx")

        doc  = Document()
        rows = [r for r in self._collect_rows(doc_ids) if not r.get("_error")]

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

            # ── Shared vol/issue string ────────────────────────────────────────
            vol_issue = _format_vol_issue(row.get("volume", ""), row.get("issue", ""))

            meta_fields = [
                ("Authors",     row.get("authors",   "") or "—"),
                ("Editor",      row.get("editor",    "") or "—"),
                ("Journal",     row.get("_journal",  "") or "—"),
                ("Vol / Issue", vol_issue or "—"),
                ("Date",        row.get("date",      "") or "—"),
                ("Pages",       row.get("page no",   "") or "—"),
                ("Publisher",   row.get("publisher", "") or "—"),
                ("DOI",         row.get("doi",       "") or "—"),
                ("ISSN",        row.get("issn",      "") or "—"),
                ("Keywords",    row.get("keywords",  "") or "—"),
                ("Type",        row.get("type",      "") or "—"),
                ("Sponsor",     row.get("sponsor",   "") or "—"),
            ]

            table = doc.add_table(rows=len(meta_fields), cols=2)
            table.style = "Table Grid"

            # Set column widths via XML (python-docx requires this approach)
            _set_docx_col_widths(table, [Inches(1.6), Inches(4.9)])

            for r_i, (label, value) in enumerate(meta_fields):
                lc = table.cell(r_i, 0)
                vc = table.cell(r_i, 1)
                lc.text = label
                vc.text = str(value)
                for run in lc.paragraphs[0].runs:
                    run.bold      = True
                    run.font.size = Pt(9)
                for run in vc.paragraphs[0].runs:
                    run.font.size = Pt(9)

            doc.add_paragraph()

            abstract = row.get("abstract", "")
            if abstract:
                doc.add_heading("Abstract", level=2)
                p = doc.add_paragraph(abstract[:2000])
                for run in p.runs:
                    run.font.size   = Pt(10)
                    run.font.italic = True

            citation = row.get("citation", "")
            if citation:
                doc.add_heading("Citation", level=2)
                p = doc.add_paragraph(citation)
                for run in p.runs:
                    run.font.size = Pt(9)

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
        include_internal: bool = False,
    ) -> tuple[bytes, str]:
        """
        Export metadata as JSON.

        Args:
            include_internal: If True, includes _-prefixed internal fields
                              (e.g. _journal, _doc_id). Useful for debugging
                              or downstream consumers that need the full record.
        """
        rows = self._collect_rows(doc_ids)
        if include_internal:
            clean = rows
        else:
            clean = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]

        ok_count = sum(1 for r in rows if not r.get("_error"))
        data = {
            "exported_at"   : datetime.utcnow().isoformat(),
            "total"         : len(clean),
            "total_ok"      : ok_count,
            "total_errors"  : len(rows) - ok_count,
            "documents"     : clean,
        }
        logger.info("JSON export — %d ok, %d errors", ok_count, len(rows) - ok_count)
        return json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"), filename

    # ── Core data collection ──────────────────────────────────────────────────

    def _collect_rows(self, doc_ids: list[str]) -> list[dict]:
        """
        Load metadata for each unique doc_id and map to the XLSX column template.

        Field priority for each column:
          1. Schema field set by extraction_service (most reliable)
          2. Detected section content (e.g. abstract from sections list)
          3. Regex fallback from full_text (for older / re-processed docs)
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
                    rows.append(_error_row(doc_id, "Document not found"))
                    continue

                m      = doc.metadata
                first3 = "\n".join((doc.full_text or "").split("\n\n")[:60])
                full   = doc.full_text or ""

                # ── title ─────────────────────────────────────────────────────
                title = _clean(m.title or "")
                if not title:
                    title = _clean(doc.filename.replace(".pdf", ""))

                # ── authors — deduplicated, separated by || ───────────────────
                schema_authors = [_clean(a) for a in (m.authors or []) if a.strip()]
                if schema_authors:
                    authors_list = schema_authors
                else:
                    authors_list = _fallback_authors(full, title)
                # Final dedup preserving order
                authors_list = _dedupe_authors(authors_list)
                authors = " || ".join(authors_list)

                # ── editor ────────────────────────────────────────────────────
                editor = _clean(getattr(m, "editor", "") or "")
                if not editor:
                    editor = _extract_editor_fb(first3)

                # ── date ──────────────────────────────────────────────────────
                year = _clean(getattr(m, "year", "") or "")
                date = year or _parse_date(m.created_at or "")

                # ── page no ───────────────────────────────────────────────────
                pages   = _clean(getattr(m, "pages", "") or "")
                if not pages:
                    pages = _extract_pages_fb(first3)
                page_no = pages or (str(m.page_count) if m.page_count else "")

                # ── abstract ──────────────────────────────────────────────────
                abstract = ""
                for sec in (doc.sections or []):
                    if sec.section_type.value == "abstract":
                        abstract = sec.content[:2000].strip()
                        break
                if not abstract:
                    abstract = _clean(m.abstract or "")[:2000]
                if not abstract:
                    abstract = _extract_abstract_fb(full[:5000])

                # ── sponsor (funding) ─────────────────────────────────────────
                sponsor = _clean(getattr(m, "funding", "") or "")
                if not sponsor:
                    sponsor = _extract_funding_fb(full[:8000])

                # ── bibliographic — schema first, regex fallback ───────────────
                doi       = _clean(m.doi       or "") or _extract_doi(first3)
                issn      = _clean(m.issn      or "") or _extract_issn(first3)
                publisher = _clean(m.publisher or "") or _extract_publisher(first3)
                journal   = _clean(m.journal   or "") or _extract_journal(first3)
                volume    = _clean(m.volume    or "") or _extract_volume(first3)
                issue     = _clean(m.issue     or "") or _extract_issue(first3)

                # ── keywords — separated by || ────────────────────────────────
                kws = list(m.keywords or [])
                if not kws:
                    kws = _extract_keywords_list(full)
                keywords = " || ".join(_clean(k) for k in kws if k.strip())

                # ── type ──────────────────────────────────────────────────────
                article_type = _clean(getattr(m, "article_type", "") or "")
                if not article_type:
                    article_type = _extract_article_type_fb(first3)

                # ── citation ──────────────────────────────────────────────────
                citation = _build_citation(
                    title     = title,
                    authors   = authors_list,
                    date      = date,
                    journal   = journal,
                    volume    = volume,
                    issue     = issue,
                    pages     = pages,
                    doi       = doi,
                    publisher = publisher,
                )

                rows.append({
                    # XLSX/CSV visible columns
                    "name original" : doc.filename,
                    "authors"       : authors,
                    "editor"        : editor,
                    "date"          : date,
                    "page no"       : page_no,
                    "abstract"      : abstract,
                    "sponsor"       : sponsor,
                    "citation"      : citation,
                    "doi"           : doi,
                    "issn"          : issn,
                    "publisher"     : publisher,
                    "keywords"      : keywords,
                    "title"         : title,
                    "type"          : article_type or "Research Article",
                    "issue"         : issue,
                    "volume"        : volume,
                    # Internal fields (stripped from JSON by default)
                    "_filename"     : doc.filename,
                    "_doc_id"       : doc.doc_id,
                    "_journal"      : journal,
                    "_error"        : False,
                })

            except Exception as e:
                logger.error("Export failed for doc_id=%s: %s", doc_id, e, exc_info=True)
                rows.append(_error_row(doc_id, str(e)))

        return rows


# ══════════════════════════════════════════════════════════════════════════════
# Helper functions — all pure, no side effects
# ══════════════════════════════════════════════════════════════════════════════

def _error_row(doc_id: str, reason: str) -> dict:
    """Sentinel row for a document that could not be exported."""
    row = {col: "" for col in XLSX_COLUMNS}
    row["name original"] = doc_id
    row["title"]         = f"[Export error: {reason}]"
    row["_doc_id"]       = doc_id
    row["_error"]        = True
    return row


def _clean(text: str) -> str:
    return unescape(text).strip()


def _dedupe_authors(authors: list[str]) -> list[str]:
    """Deduplicate author list preserving order, case-insensitive."""
    seen:   set[str]  = set()
    result: list[str] = []
    for a in authors:
        key = a.lower().strip()
        if key and key not in seen:
            seen.add(key)
            result.append(a)
    return result


def _format_vol_issue(volume: str, issue: str) -> str:
    """Format volume/issue as 'Vol 14, No 2' or '' if both missing."""
    parts: list[str] = []
    if volume:
        parts.append(f"Vol {volume}")
    if issue:
        parts.append(f"No {issue}")
    return ", ".join(parts)


def _set_docx_col_widths(table, widths) -> None:
    """
    Set column widths on a python-docx table via direct XML manipulation.
    `widths` should be a list of Inches/Pt values matching column count.
    """
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement

    tbl = table._tbl
    tblGrid = tbl.find(qn("w:tblGrid"))
    if tblGrid is None:
        tblGrid = OxmlElement("w:tblGrid")
        tbl.insert(0, tblGrid)

    # Clear existing grid cols
    for gc in tblGrid.findall(qn("w:gridCol")):
        tblGrid.remove(gc)

    for width in widths:
        # Convert to twentieths of a point (EMU-based: 1 inch = 914400 EMU; 1 pt = 12700)
        w_twips = int(width.inches * 1440)
        gc = OxmlElement("w:gridCol")
        gc.set(qn("w:w"), str(w_twips))
        tblGrid.append(gc)


def _parse_date(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()

    # Plain 4-digit year
    if re.match(r"^(19|20)\d{2}$", raw):
        return raw

    # PDF raw format: D:YYYYMMDDHHmmss
    m = re.match(r"D:(\d{4})(\d{2})(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # Human readable
    raw_clean = re.sub(r"(\d+)(st|nd|rd|th)\b", r"\1", raw)
    for fmt in (
        "%d %B %Y", "%B %d %Y", "%d %b %Y", "%b %d %Y",
        "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%B %Y", "%b %Y",
    ):
        try:
            return datetime.strptime(raw_clean.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    yr = re.search(r"\b(19|20)\d{2}\b", raw)
    return yr.group(0) if yr else raw[:10]


def _build_citation(
    title     : str,
    authors   : list[str],
    date      : str,
    journal   : str = "",
    volume    : str = "",
    issue     : str = "",
    pages     : str = "",
    doi       : str = "",
    publisher : str = "",
) -> str:
    """
    Build a citation in Nigerian journal format:

      Last, F. N. and Last, F. N. (Year). Title of article.
      Journal Name, Volume(Issue):pages.

    Example:
      Shabi, I. N. and Adeagbo, O. O. (2015). Dynamics of library use
      and reading habits among senior secondary school students.
      Nigerian School Library Journal, 14(1):7-14.
    """
    parts: list[str] = []

    # ── Authors ───────────────────────────────────────────────────────────────
    if authors:
        formatted: list[str] = []
        for a in authors:
            a = a.strip()
            if not a:
                continue
            if "," in a:
                # Already "Last, First M." format
                formatted.append(a)
            else:
                # "First [Middle] Last" → "Last, F. [M.]"
                tokens   = a.split()
                last     = tokens[-1]
                # Handle hyphenated first names: "Mary-Jane" → "M."
                initials = []
                for tok in tokens[:-1]:
                    if tok:
                        first_letter = tok.lstrip("-")[0] if tok.lstrip("-") else tok[0]
                        initials.append(f"{first_letter.upper()}.")
                init_str = " ".join(initials)
                formatted.append(f"{last}, {init_str}" if init_str else last)

        author_str = " and ".join(formatted)
        parts.append(author_str)

    # ── Year ──────────────────────────────────────────────────────────────────
    year = (date[:4] if date and len(date) >= 4 else date) or ""
    parts.append(f"({year})." if re.match(r"^(19|20)\d{2}$", year) else "(n.d.).")

    # ── Title — sentence case, preserve acronyms + hyphenated proper nouns ────
    if title:
        words  = title.split()
        cased  : list[str] = []
        for i, word in enumerate(words):
            core = re.sub(r"[^A-Za-z]", "", word)
            if core and core == core.upper() and len(core) >= 2:
                cased.append(word)                           # acronym — preserve
            elif "-" in word and any(p[:1].isupper() for p in word.split("-") if p):
                cased.append(word)                           # hyphenated proper noun
            elif i == 0:
                cased.append(word[0].upper() + word[1:].lower())
            else:
                cased.append(word.lower())
        t = " ".join(cased)
        # Capitalise first word after ":"
        t = re.sub(r"(:\s+)([a-z])", lambda m: m.group(1) + m.group(2).upper(), t)
        parts.append(f"{t}.")

    # ── Journal, Volume(Issue):pages ─────────────────────────────────────────
    if journal:
        src = journal
        if volume:
            src += f", {volume}"
            if issue:
                src += f"({issue})"
        if pages:
            src += f":{pages}"
        src += "."
        parts.append(src)
    elif publisher:
        parts.append(f"{publisher}.")

    # ── DOI — only if no journal info ────────────────────────────────────────
    if doi and not journal:
        doi_url = doi if doi.startswith("http") else f"https://doi.org/{doi}"
        parts.append(doi_url)

    return " ".join(parts)


# ── Fallback extraction helpers ───────────────────────────────────────────────

def _extract_editor_fb(text: str) -> str:
    m = re.search(
        r"(?:Edited\s+by|Guest\s+Editor|Section\s+Editor|"
        r"Editor[-\s]?in[-\s]?Chief|Handling\s+Editor|Academic\s+Editor)"
        r"[:\s]+([A-Z][^\n]{3,80})",
        text[:4000], re.IGNORECASE,
    )
    if m:
        raw = m.group(1).strip().rstrip(".,;")
        raw = re.split(
            r"\s*[,;]\s*(?:PhD|MD|Dr|Prof|University|Institute)",
            raw, flags=re.IGNORECASE,
        )[0]
        return raw.strip()[:80]
    return ""


def _extract_pages_fb(text: str) -> str:
    m = re.search(
        r"\bpp?\.?\s*([eE]?\d{1,6}\s*[-–]\s*[eE]?\d{1,6})\b",
        text[:3000], re.IGNORECASE,
    )
    if m:
        return m.group(1).replace(" ", "").replace("–", "-")
    m = re.search(
        r"\bVol[^\n]{1,30},\s*([eE]?\d{1,6}[-–][eE]?\d{1,6})\b",
        text[:3000], re.IGNORECASE,
    )
    if m:
        return m.group(1).replace("–", "-")
    m = re.search(r"\b\d+:\s*([eE]?\d{1,6}[-–][eE]?\d{1,6})\b", text[:3000])
    if m:
        return m.group(1).replace("–", "-")
    return ""


def _extract_abstract_fb(text: str) -> str:
    m = re.search(
        r"\b(?:Abstract|Summary)\b\s*[:—]?\s*\n?([\s\S]{80,2000}?)"
        r"(?=\n\s*\n\s*(?:Keywords?|Introduction|Background|1\.|$))",
        text, re.IGNORECASE,
    )
    if m:
        return _clean(m.group(1))[:2000]
    paras = [p.strip() for p in re.split(r"\n\s*\n", text) if len(p.strip()) > 120]
    for para in paras[1:5]:
        if len(para) > 120:
            return _clean(para[:2000])
    return ""


def _extract_funding_fb(text: str) -> str:
    patterns = [
        r"(?:Funding|Funding\s+source|Financial\s+support|Grant)[:\s]+([^\n]{10,300})",
        r"(?:supported\s+by|funded\s+by|sponsored\s+by)\s+([^\n]{10,200})",
        r"(?:This\s+(?:study|work|research)\s+was\s+(?:supported|funded)\s+by)\s+([^\n]{10,200})",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()[:300]
    return ""


def _extract_article_type_fb(text: str) -> str:
    patterns = [
        r"\b(Systematic\s+Review(?:\s+and\s+Meta[-\s]?Analysis)?)\b",
        r"\b(Meta[-\s]?Analysis)\b",
        r"\b(Randomized\s+Controlled\s+Trial|RCT)\b",
        r"\b(Clinical\s+Trial)\b",
        r"\b(Case\s+Report)\b",
        r"\b(Review\s+Article|Literature\s+Review|Narrative\s+Review|Review\s+Paper)\b",
        r"\b(Scoping\s+Review)\b",
        r"\b(Original\s+(?:Research|Article|Paper))\b",
        r"\b(Research\s+Article|Research\s+Paper)\b",
        r"\b(Short\s+(?:Communication|Report|Note))\b",
        r"\b(Letter\s+to\s+the\s+Editor)\b",
        r"\b(Technical\s+(?:Note|Report))\b",
    ]
    for pat in patterns:
        m = re.search(pat, text[:3000], re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return "Research Article"


def _fallback_authors(text: str, title: str) -> list[str]:
    """
    Heuristic author extraction from full text when schema field is empty.
    Scans lines after the title, stops at institutional/section markers.
    Does NOT split on commas to avoid breaking "Last, First" name format.
    """
    lines       = [l.strip() for l in text[:3000].split("\n") if l.strip()]
    title_found = False
    candidates  : list[str] = []
    title_low   = title[:30].lower() if title else ""

    # Markers that signal the author block has ended
    _STOP = re.compile(
        r"\b(university|department|institute|college|abstract|"
        r"introduction|background|email|@|http|received|accepted|"
        r"copyright|doi|keywords?|school\s+of|faculty)\b",
        re.IGNORECASE,
    )

    for line in lines:
        if not title_found:
            if title_low and title_low[:15] in line.lower():
                title_found = True
            continue

        if _STOP.search(line) or len(line) > 200:
            break

        # Strip leading superscripts
        line = re.sub(r"^[\d,*†‡§¶\s]+", "", line).strip()
        if not line:
            continue

        # Split on " and " or ";" — NOT on commas (names use "Last, First")
        parts = re.split(r"\s+and\s+|\s*;\s*", line, flags=re.IGNORECASE)

        found_here = 0
        for part in parts:
            part = part.strip()
            # Accept name-like tokens: starts with capital, has lowercase, not too long
            if (
                re.match(r"^[A-Z\u00C0-\u024F]", part)
                and re.search(r"[a-z]", part)
                and 4 < len(part) < 70
            ):
                candidates.append(part)
                found_here += 1
                if len(candidates) >= 10:
                    break

        if found_here == 0 and candidates:
            break  # First non-author line after author block — stop

        if len(candidates) >= 10:
            break

    return candidates[:10]


def _extract_doi(text: str) -> str:
    flat = re.sub(r"\n\s*", " ", text[:5000])
    m = re.search(r"https?://(?:dx\.)?doi\.org/(10\.\d{4,9}/[^\s\"'<>\]\)]+)", flat, re.IGNORECASE)
    if m:
        return m.group(1).rstrip(".,;)]")
    m = re.search(r"\bdoi\s*:?\s*(10\.\d{4,9}/[^\s\"'<>\]\)]+)", flat, re.IGNORECASE)
    if m:
        return m.group(1).rstrip(".,;)]")
    m = re.search(r"\b(10\.\d{4,9}/[^\s\"'<>\]\)]{3,})", flat)
    return m.group(1).rstrip(".,;)]") if m else ""


def _extract_issn(text: str) -> str:
    m = re.search(r"\b[EP]-?ISSN[:\s]*(\d{4}-\d{3}[\dXx])\b", text[:4000], re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"\bISSN[:\s]*(\d{4}-\d{3}[\dXx])\b", text[:4000], re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(
        r"(?:journal|issn|copyright|print|online)[^\n]*\b(\d{4}-\d{3}[\dXx])\b",
        text[:4000], re.IGNORECASE,
    )
    if m:
        return m.group(1)
    m = re.search(r"\b(\d{4}-\d{3}[\dXx])\b", text[:2000])
    return m.group(1) if m else ""


def _extract_publisher(text: str) -> str:
    known = [
        "Wolters Kluwer", "Lippincott Williams", "Elsevier", "Cell Press",
        "Springer", "Springer Nature", "Nature Publishing", "Wiley",
        "Wiley-Blackwell", "Taylor & Francis", "Taylor and Francis",
        "BMJ Publishing", "Sage Publications", "SAGE",
        "Oxford University Press", "Cambridge University Press",
        "PLOS", "BioMed Central", "BMC", "Frontiers Media", "MDPI",
        "Hindawi", "Dove Medical Press", "American Chemical Society",
        "Royal Society of Chemistry", "IEEE", "ACM", "Karger", "Thieme",
        "African Journals Online", "AJOL",
    ]
    chunk = text[:5000]
    m = re.search(
        r"(?:Published\s+by|Publisher\s*:|©\s*\d{4}\s+)([A-Z][^\n]{3,70})",
        chunk, re.IGNORECASE,
    )
    if m:
        pub = re.split(r"\s*(?:Inc\.|Ltd\.?|All rights|Copyright|\d{4})", m.group(1))[0]
        return pub.strip()[:80]
    chunk_l = chunk.lower()
    for pub in sorted(known, key=len, reverse=True):
        if pub.lower() in chunk_l:
            return pub
    return ""


def _extract_journal(text: str) -> str:
    patterns = [
        r"(?:published\s+in|journal\s*:)\s*([A-Z][^\n]{5,100})",
        r"((?:International|European|American|British|African|Asian|"
        r"Nigerian|Indian|Chinese|Korean|Canadian|Australian)\s+"
        r"(?:Journal|Review|Annals|Archives|Bulletin|Proceedings|"
        r"Transactions|Letters|Reports)\s+(?:of|for|on|in)\s+[A-Z][^\n]{3,70})",
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,6}\s+Journal[^\n]{0,40})",
        r"(American Journal of [^\n]{5,60})",
        r"(British Journal of [^\n]{5,60})",
        r"(Journal of [A-Z][^\n]{5,60})",
    ]
    for pat in patterns:
        m = re.search(pat, text[:6000], re.IGNORECASE)
        if m:
            j = m.group(1).strip()
            j = re.split(r"\s+(?:\d{4}\b|\bVol|\bNo\b|\bIssue|\d+\s*[\(,])", j)[0]
            j = j.strip().rstrip(".,;:")
            if len(j) >= 8:
                return j[:120]
    return ""


def _extract_volume(text: str) -> str:
    m = re.search(r"\bVol(?:ume)?\.?\s*(\d+)", text[:5000], re.IGNORECASE)
    return m.group(1) if m else ""


def _extract_issue(text: str) -> str:
    # Parenthetical Vol X(Y)
    m = re.search(
        r"\bVol(?:ume)?\.?\s*\d+\s*[\(,]\s*(\d+)\s*[\),]",
        text[:5000], re.IGNORECASE,
    )
    if m:
        return m.group(1)
    # Labelled Issue / No / Number
    m = re.search(
        r"\b(?:Issue|No\.?|Number|Num\.?)\s*\.?\s*(\d+)",
        text[:5000], re.IGNORECASE,
    )
    return m.group(1) if m else ""


def _extract_keywords_list(text: str) -> list[str]:
    m = re.search(
        r"(?:Keywords?|Key\s+words?|Index\s+[Tt]erms?)\s*[:—]\s*([^\n]{10,600})",
        text[:12000], re.IGNORECASE,
    )
    if not m:
        return []
    kws = [k.strip().strip("•·-–—*") for k in re.split(r"[;,•·]", m.group(1))]
    return [k for k in kws if 2 < len(k) < 100 and not k.isdigit()][:20]


# ── Singleton ─────────────────────────────────────────────────────────────────
export_service = ExportService()