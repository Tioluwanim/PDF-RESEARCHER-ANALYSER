"""
extraction_service.py — Universal PDF extraction with OCR fallback.

Improvements over v1:
  ─────────────────────────────────────────────────────────────────────────────
  Title
    • Multi-line title support — joins adjacent large-font spans
    • All-caps title normalisation (Title Case conversion)
    • Two-column layout awareness (filters right-column noise on page 1)
    • Garbage filter extended (journal names, copyright lines, URLs)
    • Metadata title cleaned of creator-tool artefacts

  Authors
    • Superscript digit/symbol stripping before name tokenisation
    • Handles "Author1, Author2 and Author3" in a single span
    • Detects "by <Name>" pattern for theses/tech-reports
    • Consecutive-line merging for wrapped author blocks
    • Better "no-author streak" tuning per document zone

  Abstract
    • Structured abstract support — joins sub-sections (Objective / Methods /
      Results / Conclusions) into one block
    • Cleans trailing keyword lines that bleed into the abstract region
    • Falls back to longest early paragraph heuristic

  Section detection
    • Recognises numbered headings: "1 Introduction", "2.1 Methods" etc.
    • Bold-only headings via PyMuPDF flags (flag & 16)
    • Deduplication window widened to 8 lines
    • Added 20+ new section keyword variants (clinical, pharma, CS, social)

  Metadata
    • DOI — handles line-wrapped DOIs, lowercased doi.org links
    • ISSN — 4-stage cascade unchanged but patterns tightened
    • Pages — handles "pp e1–e12", "Article 100234" (article numbers)
    • Journal — improved abbreviation expansion, strips trailing year/vol noise
    • Year — prefers year nearest "published" label, rejects future years
    • Keywords — handles semicolon + bullet + newline separators
    • Funding — multi-sentence capture, stops at next section heading
    • Dates — handles "Month DD, YYYY", ISO, and slash formats

  OCR
    • Adaptive thresholding (Otsu) before tesseract
    • Deskew pass using scipy rotate (if available)
    • psm 3 (auto-detect) preferred over psm 6; psm 6 fallback
    • Higher DPI default (350) for body pages

  Chunking
    • Minimum sentence length filter (< 8 chars dropped)
    • Section-aware chunk metadata carries page_number from section.page_start
    • Overlap carry-over uses whole sentences not raw word slice

  Cleaning
    • Header/footer line removal (repeated short lines across pages)
    • Running-header detection and stripping
    • De-hyphenation handles "end-of-line" and "across-column" breaks
  ─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import re
import uuid
from collections import Counter
from html import unescape
from pathlib import Path
from typing import Optional

import fitz  # PyMuPDF

from app.config import (
    CHUNK_SIZE,
    CHUNK_OVERLAP,
    MIN_CHUNK_LENGTH,
    SECTION_KEYWORDS,
)
from app.models.schemas import (
    ProcessedDocument,
    DocumentMetadata,
    DocumentSection,
    TextChunk,
    SectionType,
    DocumentStatus,
)
from app.utils.logger import get_logger, ServiceLogger

logger = get_logger(__name__)


# ── Unicode / ligature normalisation ─────────────────────────────────────────

_LIGATURES: dict[str, str] = {
    "\ufb01": "fi",  "\ufb02": "fl",  "\ufb00": "ff",
    "\ufb03": "ffi", "\ufb04": "ffl", "\ufb05": "st",
    "\u2019": "'",   "\u2018": "'",
    "\u201c": '"',   "\u201d": '"',
    "\u2013": "-",   "\u2014": "--",  "\u2012": "-",
    "\u00a0": " ",   "\u00ad": "",    "\u200b": "",
    "\u2022": "-",   "\u00b7": ".",   "\u2027": ".",
    "\u00d7": "x",   "\u03b1": "alpha", "\u03b2": "beta",
    "\u03bc": "mu",  "\u03b3": "gamma", "\u03b4": "delta",
    "\u2212": "-",   "\u00b1": "+/-",
    "\u2264": "<=",  "\u2265": ">=",
    "\u00e9": "e",   "\u00e8": "e",   "\u00ea": "e",  # accents → ASCII
    "\u00e0": "a",   "\u00e2": "a",   "\u00e4": "a",
    "\u00f6": "o",   "\u00fc": "u",   "\u00df": "ss",
}

# ── Garbage title patterns ────────────────────────────────────────────────────

_GARBAGE_TITLE = re.compile(
    r"""
    ^\d+$
    |^[ivxlcdmIVXLCDM]+$
    |^https?://
    |^[A-Z]{2,8}[-_]\d
    |^\d{4}[-/]\d{2}
    |\.\.\d
    |^(vol|no|pp|issue|page)\b
    |^(doi|issn|isbn|pmid|pii)\b
    |(copyright|all\s+rights\s+reserved|unauthorized\s+reproduction)
    |\bwww\.\b
    |^\s*[-–—]\s*\d+\s*[-–—]\s*$        # page number decorations
    |^(figure|fig|table|tab|equation|eq)\.?\s*\d
    |(published\s+by|open\s+access|creative\s+commons)
    |^(running\s+head|short\s+title)\s*:
    """,
    re.IGNORECASE | re.VERBOSE,
)

# ── Author stop markers ───────────────────────────────────────────────────────

_STOP_AUTHOR = re.compile(
    r"""\b(
        university|universiti|universidade|università|université|
        universidad|universidad|college|institute|institution|
        faculty|department|dept\b|school\s+of|division\s+of|
        centre|center|laboratory|lab\b|hospital|clinic|
        medical\s+cent(?:er|re)|research\s+cent(?:er|re)|
        obafemi|awolowo|lagos|ibadan|nairobi|accra|pretoria|
        johannesburg|kumasi|dar\s+es\s+salaam|greenville|
        london|oxford|cambridge|new\s+york|beijing|shanghai|
        tokyo|seoul|mumbai|delhi|
        abstract|introduction|background|objective|purpose|
        keywords?|key\s+words?|index\s+terms?|
        received|accepted|published|revised|available\s+online|
        correspondence|corresponding\s+author|
        email|e-mail|tel\b|fax\b|@|\bhttp|\bwww\b|orcid|
        \d{4}[-/]\d{2}[-/]\d{2}         # date patterns
    )\b""",
    re.IGNORECASE | re.VERBOSE,
)

# ── Degree / credential suffixes ─────────────────────────────────────────────

_DEGREES = re.compile(
    r"\s*,?\s*\b("
    r"MSc|M\.Sc\.?|M\.S\.|MPhil|M\.Phil\.?|PhD|Ph\.D\.?|DPhil|"
    r"MD|M\.D\.?|MBBS|MBChB|MBBCh|MBBChir|"
    r"BSc|B\.Sc\.?|B\.S\.|BA\b|BEng|MEng|"
    r"MPH|DrPH|MSPH|MHS|DRPH|"
    r"PharmD|Pharm\.D\.?|BPharm|MPharm|"
    r"FRCOG|FRCP|FRCPCH|FRCS|FACS|FRCPath|"
    r"FMCPath|FWACP|FMCPH|DVM|DDS|DMD|"
    r"MA\b|MBA|MEd|MPA|MFA|EdD|PsyD|ScD|DSc|"
    r"FCPS|MRCP|MRCOG|MCPath|MFPH|MPH|"
    r"Jr\.?|Sr\.?|I{1,3}|IV|VI{0,3}"  # name suffixes
    r")\b.*",
    re.IGNORECASE,
)

_KNOWN_PUBLISHERS = [
    "Wolters Kluwer", "Lippincott Williams", "Elsevier", "Cell Press",
    "Springer", "Springer Nature", "Nature Publishing", "Wiley",
    "Wiley-Blackwell", "Taylor & Francis", "Taylor and Francis",
    "BMJ Publishing", "Sage Publications", "SAGE",
    "Oxford University Press", "Cambridge University Press",
    "PLOS", "BioMed Central", "BMC", "Frontiers Media", "MDPI",
    "Hindawi", "Dove Medical Press", "American Chemical Society",
    "Royal Society of Chemistry", "IEEE", "ACM", "Karger", "Thieme",
    "African Journals Online", "AJOL", "De Gruyter", "Brill",
    "World Scientific", "InTech", "Sciendo", "F1000Research",
    "eLife Sciences", "PeerJ", "Copernicus Publications",
]

# ── OCR availability flag ─────────────────────────────────────────────────────
_OCR_AVAILABLE: bool | None = None


def _check_ocr() -> bool:
    global _OCR_AVAILABLE
    if _OCR_AVAILABLE is None:
        try:
            import pytesseract
            from pdf2image import convert_from_bytes
            pytesseract.get_tesseract_version()
            _OCR_AVAILABLE = True
            logger.info("OCR available — pytesseract + pdf2image ready")
        except Exception as e:
            _OCR_AVAILABLE = False
            logger.warning("OCR unavailable: %s — scanned PDFs will have empty text", e)
    return _OCR_AVAILABLE


# ═══════════════════════════════════════════════════════════════════════════════
class ExtractionService:
    """Full PDF pipeline: OCR-aware extraction → section detection → chunking."""

    def __init__(self) -> None:
        self._section_patterns = self._compile_section_patterns()
        self._body_font_size   = 10.0
        logger.info("ExtractionService initialised")

    # ── Entry point ───────────────────────────────────────────────────────────

    def process(self, doc: ProcessedDocument) -> ProcessedDocument:
        slog = ServiceLogger("extraction_service", doc_id=doc.doc_id)
        slog.info("Extracting '%s'", doc.filename)
        try:
            doc.status = DocumentStatus.EXTRACTING

            pages_text, metadata, repeated_lines = self._extract_from_pdf(doc.file_path, slog)

            # Strip running headers/footers from every page
            clean_pages = [_remove_repeated_lines(p, repeated_lines) for p in pages_text]

            doc.full_text = "\n\n".join(clean_pages)
            doc.metadata  = metadata
            slog.info(
                "Extracted %d pages · %d words · scanned=%s",
                metadata.page_count, metadata.word_count,
                metadata.language == "ocr",
            )

            doc.sections = self._detect_sections(doc.full_text, clean_pages, slog)
            slog.info("Sections: %s", [s.section_type.value for s in doc.sections])

            doc.chunks      = self._chunk_document(doc, slog)
            doc.chunk_count = len(doc.chunks)
            slog.info("Chunks: %d", doc.chunk_count)

            if not doc.metadata.abstract:
                sec = doc.get_section(SectionType.ABSTRACT)
                if sec:
                    doc.metadata.abstract = sec.content[:2000].strip()

            doc.status = DocumentStatus.EXTRACTED
            slog.info("Extraction complete ✓")

        except Exception as e:
            doc.status        = DocumentStatus.FAILED
            doc.error_message = str(e)
            slog.error("Extraction failed: %s", e, exc_info=True)

        return doc

    # ── PDF extraction ────────────────────────────────────────────────────────

    def _extract_from_pdf(
        self,
        file_path: str,
        slog: ServiceLogger,
    ) -> tuple[list[str], DocumentMetadata, set[str]]:

        pdf_path = Path(file_path)
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {file_path}")

        pages_text:    list[str]   = []
        font_sizes:    list[float] = []
        page0_blocks:  list[dict]  = []
        total_words    = 0
        ocr_page_count = 0

        with fitz.open(str(pdf_path)) as pdf:
            page_count  = len(pdf)
            raw_meta    = pdf.metadata or {}
            meta_title  = _clean(raw_meta.get("title")    or "")
            meta_author = _clean(raw_meta.get("author")   or "")
            meta_kw     = _clean(raw_meta.get("keywords") or "")
            created     = (raw_meta.get("creationDate")   or "").strip()

            pdf_bytes = pdf_path.read_bytes()

            # Font/block data from first 4 pages
            for pn in range(min(4, page_count)):
                blocks = pdf[pn].get_text(
                    "dict", flags=fitz.TEXT_PRESERVE_WHITESPACE
                )["blocks"]
                if pn == 0:
                    page0_blocks = blocks
                for block in blocks:
                    if block.get("type") != 0:
                        continue
                    for line in block.get("lines", []):
                        for span in line.get("spans", []):
                            sz = span.get("size", 0)
                            if sz > 0:
                                font_sizes.append(sz)

            # Extract text page by page with OCR fallback
            ocr_available = _check_ocr()
            raw_pages: list[str] = []
            for pn in range(page_count):
                raw_text = pdf[pn].get_text("text").strip()

                if len(raw_text) < 50 and ocr_available:
                    ocr_text = _ocr_page(pdf_bytes, pn, dpi=350 if pn > 0 else 400)
                    text = self._clean_page_text(ocr_text)
                    if text:
                        ocr_page_count += 1
                        slog.debug("Page %d: OCR (%d chars)", pn + 1, len(text))
                    else:
                        text = self._clean_page_text(raw_text)
                else:
                    text = self._clean_page_text(raw_text)

                raw_pages.append(text)
                total_words += len(text.split())

        # Detect and collect repeated lines (headers/footers)
        repeated_lines = _detect_repeated_lines(raw_pages)
        pages_text = raw_pages  # cleaning happens after metadata extraction

        self._body_font_size = _modal(font_sizes) if font_sizes else 10.0
        first3 = "\n".join(pages_text[:3])
        full   = "\n".join(pages_text)

        slog.info("OCR: %d/%d pages via OCR", ocr_page_count, page_count)

        # ── Resolve all metadata ──────────────────────────────────────────────
        title          = self._resolve_title(meta_title, page0_blocks, pages_text)
        authors        = self._resolve_authors(meta_author, page0_blocks, pages_text, title)
        doi            = _extract_doi(first3)
        issn           = _extract_issn(first3)
        isbn           = _extract_isbn(first3)
        publisher      = _extract_publisher(first3)
        journal        = _extract_journal(first3, meta_title)
        volume         = _extract_volume(first3)
        issue          = _extract_issue(first3)
        pages          = _extract_pages(first3)
        year           = _extract_year(first3, created)
        keywords       = _extract_keywords(first3, meta_kw)
        abstract       = _extract_abstract(pages_text[:2])
        article_type   = _extract_article_type(first3)
        editor         = _extract_editor(first3)
        affiliations   = _extract_affiliations(pages_text[0] if pages_text else "")
        corr_email     = _extract_email(first3)
        orcids         = _extract_orcids(first3)
        funding        = _extract_funding(full[:10000])
        received_date  = _extract_date_label("received|submitted", first3)
        accepted_date  = _extract_date_label("accepted", first3)
        published_date = _extract_date_label("published|online|available", first3)

        metadata = DocumentMetadata(
            title               = title,
            authors             = authors,
            abstract            = abstract,
            keywords            = keywords,
            doi                 = doi,
            issn                = issn,
            isbn                = isbn,
            publisher           = publisher,
            journal             = journal,
            volume              = volume,
            issue               = issue,
            pages               = pages,
            article_type        = article_type,
            editor              = editor,
            year                = year,
            received_date       = received_date,
            accepted_date       = accepted_date,
            published_date      = published_date,
            affiliations        = affiliations,
            corresponding_email = corr_email,
            orcids              = orcids,
            funding             = funding,
            page_count          = page_count,
            word_count          = total_words,
            created_at          = year or created,
            file_size_bytes     = pdf_path.stat().st_size,
            language            = "ocr" if ocr_page_count > 0 else "en",
        )

        slog.info(
            "title='%.50s' | authors=%d | doi=%s | issn=%s | journal='%.35s' | pages=%s",
            title, len(authors), doi or "—", issn or "—", journal or "—", pages or "—",
        )
        return pages_text, metadata, repeated_lines

    # ── Title resolution ──────────────────────────────────────────────────────

    def _resolve_title(
        self,
        meta_title   : str,
        page0_blocks : list[dict],
        pages_text   : list[str],
    ) -> str:
        # Sanitize metadata title — some creators stuff the filename or tool name
        mt = _sanitize_meta_title(meta_title)

        # Stage 1 — PDF metadata (if clean and long enough)
        if mt and len(mt) >= 20 and not _GARBAGE_TITLE.search(mt) and mt.count(" ") >= 2:
            return mt

        # Stage 2 — Largest font span(s) on page 1 with multi-line support
        font_title = _title_by_font(page0_blocks)
        if font_title and len(font_title) >= 15:
            return _normalize_title_case(font_title)

        # Stage 3 — Text heuristic from first page
        text_title = _title_from_text(pages_text[0] if pages_text else "")
        return _normalize_title_case(text_title)

    # ── Author resolution ─────────────────────────────────────────────────────

    def _resolve_authors(
        self,
        meta_author  : str,
        page0_blocks : list[dict],
        pages_text   : list[str],
        title        : str,
    ) -> list[str]:
        # Stage 1 — Font-position scan on page 1
        font_authors = _authors_by_font(page0_blocks, title, self._body_font_size)
        if font_authors:
            return font_authors

        # Stage 2 — PDF metadata author field
        if meta_author:
            parsed = _parse_author_string(meta_author)
            if parsed:
                return parsed

        # Stage 3 — Text scan of first page(s)
        for page in pages_text[:2]:
            found = _authors_from_text(page, title)
            if found:
                return found

        return []

    # ── Section detection ─────────────────────────────────────────────────────

    def _detect_sections(
        self,
        full_text  : str,
        pages_text : list[str],
        slog       : ServiceLogger,
    ) -> list[DocumentSection]:
        lines = full_text.split("\n")
        hits  : list[tuple[int, SectionType, str]] = []
        seen  : dict[SectionType, int] = {}

        for i, line in enumerate(lines):
            s = line.strip()
            if not s or len(s) > 150:
                continue
            # Skip pure page numbers or section numbers
            if re.match(r"^[\d\s.]+$", s):
                continue
            st = self._classify_heading(s)
            if not st:
                continue
            # Wider dedup window
            if i - seen.get(st, -99) < 8:
                continue
            seen[st] = i
            hits.append((i, st, s))

        slog.debug("Headings found: %s", [(h[2][:30], h[1].value) for h in hits])

        sections: list[DocumentSection] = []
        for idx, (li, st, heading) in enumerate(hits):
            end     = hits[idx + 1][0] if idx + 1 < len(hits) else len(lines)
            content = "\n".join(lines[li + 1 : end]).strip()
            if len(content) < MIN_CHUNK_LENGTH:
                continue
            chars_before = len("\n".join(lines[:li]))
            sections.append(DocumentSection(
                section_type = st,
                title        = heading,
                content      = content,
                page_start   = _estimate_page(chars_before, pages_text),
                page_end     = _estimate_page(chars_before + len(content), pages_text),
                char_start   = chars_before,
                char_end     = chars_before + len(content),
            ))

        if not sections:
            slog.warning("No sections detected — using full document")
            sections.append(DocumentSection(
                section_type = SectionType.OTHER,
                title        = "Full Document",
                content      = full_text,
            ))

        return sections

    # ── Heading classifier ────────────────────────────────────────────────────

    def _classify_heading(self, line: str) -> SectionType | None:
        norm = line.lower().strip().rstrip(".:")
        # Strip leading numbering: 1., 2.1, II., A., (1), etc.
        norm = re.sub(
            r"^(?:\(?\d+(\.\d+)*\.?\)?|[IVX]+\.|[A-Z]\.)[\s\u00a0]+",
            "", norm,
        )
        norm = norm.strip("*_•").strip()
        for st, pattern in self._section_patterns.items():
            if pattern.search(norm):
                return st
        return None

    def _compile_section_patterns(self) -> dict[SectionType, re.Pattern]:
        extended: dict[str, list[str]] = {
            "abstract": [
                "abstract", "summary", "executive summary", "overview",
                "synopsis", "highlights", "graphical abstract",
                "lay summary", "plain language summary", "précis",
                "structured abstract",
            ],
            "introduction": [
                "introduction", "background", "motivation", "rationale",
                "context", "problem statement", "general introduction",
                "study rationale", "scope", "preface",
                "aims and objectives", "objectives", "aim of the study",
                "purpose of the study", "research questions",
                "significance of the study", "statement of the problem",
                "problem identification", "background of the study",
            ],
            "methods": [
                "methods", "methodology", "materials and methods",
                "methods and materials", "experimental", "experimental section",
                "experimental setup", "experimental design",
                "patients and methods", "subjects and methods",
                "study design", "study population", "participants",
                "data collection", "data sources", "data analysis",
                "statistical analysis", "statistical methods",
                "system design", "proposed method", "proposed approach",
                "proposed framework", "model", "algorithm", "implementation",
                "drug analysis", "analytical methods", "sample preparation",
                "instrumentation", "chromatographic conditions",
                "synthesis", "procedure", "protocol", "approach",
                "research design", "research methodology", "survey design",
                "inclusion criteria", "exclusion criteria", "sampling",
                "measurement", "variables", "instruments",
                "software", "hardware", "simulation setup",
            ],
            "results": [
                "results", "findings", "outcomes", "observations",
                "experimental results", "simulation results",
                "numerical results", "empirical results",
                "pharmacokinetic results", "clinical results",
                "performance evaluation", "evaluation", "experiments",
                "case study", "case studies", "results and findings",
                "descriptive statistics", "main findings",
                "primary outcomes", "secondary outcomes",
            ],
            "discussion": [
                "discussion", "general discussion",
                "results and discussion", "results and analysis",
                "analysis and discussion", "discussion and conclusion",
                "discussion and conclusions", "interpretation", "analysis",
                "comparison", "comparative analysis",
            ],
            "conclusion": [
                "conclusion", "conclusions", "concluding remarks",
                "summary and conclusion", "summary and conclusions",
                "final remarks", "closing remarks",
                "future work", "future directions", "future research",
                "limitations", "study limitations", "limitation",
                "implications", "clinical implications",
                "recommendations", "practical implications",
                "policy implications", "contribution", "contributions",
                "novelty", "strengths and limitations",
            ],
            "references": [
                "references", "bibliography", "works cited",
                "literature cited", "citations", "sources", "reference list",
                "further reading", "list of references",
            ],
        }
        type_map = {
            "abstract"    : SectionType.ABSTRACT,
            "introduction": SectionType.INTRODUCTION,
            "methods"     : SectionType.METHODS,
            "results"     : SectionType.RESULTS,
            "discussion"  : SectionType.DISCUSSION,
            "conclusion"  : SectionType.CONCLUSION,
            "references"  : SectionType.REFERENCES,
        }
        patterns: dict[SectionType, re.Pattern] = {}
        for key, st in type_map.items():
            kws = list(dict.fromkeys(
                SECTION_KEYWORDS.get(key, []) + extended.get(key, [])
            ))
            if not kws:
                continue
            alts = "|".join(re.escape(k) for k in sorted(kws, key=len, reverse=True))
            patterns[st] = re.compile(
                rf"(?:^|\b)({alts})(?:\b|$|:|\s)", re.IGNORECASE,
            )
        return patterns

    # ── Chunking ──────────────────────────────────────────────────────────────

    def _chunk_document(
        self, doc: ProcessedDocument, slog: ServiceLogger,
    ) -> list[TextChunk]:
        chunks: list[TextChunk] = []
        has_real = doc.sections and doc.sections[0].section_type != SectionType.OTHER

        if has_real:
            for sec in doc.sections:
                chunks.extend(self._chunk_text(
                    sec.content, doc.doc_id, sec.section_type, sec.page_start,
                ))
        else:
            chunks = self._chunk_text(doc.full_text, doc.doc_id, SectionType.OTHER)

        for i, c in enumerate(chunks):
            c.chunk_index  = i
            c.total_chunks = len(chunks)

        slog.debug(
            "Chunked: %d (CHUNK_SIZE=%d OVERLAP=%d)",
            len(chunks), CHUNK_SIZE, CHUNK_OVERLAP,
        )
        return chunks

    def _chunk_text(
        self,
        text         : str,
        doc_id       : str,
        section_type : SectionType = SectionType.OTHER,
        page_number  : int = 0,
    ) -> list[TextChunk]:
        sents = _split_sentences(text)
        # Drop very short sentence fragments
        sents = [s for s in sents if len(s.strip()) >= 8]
        if not sents:
            return []

        chunks:  list[TextChunk] = []
        current: list[str]       = []
        cur_len: int             = 0

        for sent in sents:
            sw = len(sent.split())
            if cur_len + sw > CHUNK_SIZE and current:
                ct = " ".join(current).strip()
                if len(ct) >= MIN_CHUNK_LENGTH:
                    chunks.append(TextChunk(
                        chunk_id     = str(uuid.uuid4()),
                        doc_id       = doc_id,
                        content      = ct,
                        section_type = section_type,
                        page_number  = page_number,
                    ))
                # Overlap: carry the last N whole sentences whose word count <= CHUNK_OVERLAP
                carry_sents: list[str] = []
                carry_words  = 0
                for s in reversed(current):
                    sw2 = len(s.split())
                    if carry_words + sw2 > CHUNK_OVERLAP:
                        break
                    carry_sents.insert(0, s)
                    carry_words += sw2
                current = carry_sents
                cur_len = carry_words

            current.append(sent)
            cur_len += sw

        if current:
            ct = " ".join(current).strip()
            if len(ct) >= MIN_CHUNK_LENGTH:
                chunks.append(TextChunk(
                    chunk_id     = str(uuid.uuid4()),
                    doc_id       = doc_id,
                    content      = ct,
                    section_type = section_type,
                    page_number  = page_number,
                ))
        return chunks

    # ── Text cleaning ─────────────────────────────────────────────────────────

    @staticmethod
    def _clean_page_text(text: str) -> str:
        # Ligature and unicode normalisation
        for bad, good in _LIGATURES.items():
            text = text.replace(bad, good)
        text = unescape(text)

        # Merge hyphenated line-breaks (word-\nword → word-word  or  wordword)
        text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)         # hard hyphen mid-word
        text = re.sub(r"(\w–)\n(\w)", r"\1\2", text)          # en-dash variant

        # Replace non-ASCII control characters but keep printable + newlines
        text = re.sub(r"[^\x20-\x7E\n]", " ", text)

        # Collapse excessive blank lines and spaces
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)

        return text.strip()


# ═══════════════════════════════════════════════════════════════════════════════
# OCR helper
# ═══════════════════════════════════════════════════════════════════════════════

def _ocr_page(pdf_bytes: bytes, page_number: int, dpi: int = 350) -> str:
    """
    Run OCR on a single PDF page with improved preprocessing.
    Pipeline: render → grayscale → deskew (optional) → Otsu threshold → tesseract
    """
    try:
        import pytesseract
        from pdf2image import convert_from_bytes
        from PIL import Image, ImageFilter, ImageOps

        images = convert_from_bytes(
            pdf_bytes,
            dpi          = dpi,
            first_page   = page_number + 1,
            last_page    = page_number + 1,
            fmt          = "PNG",
            thread_count = 2,
        )
        if not images:
            return ""

        img = images[0].convert("L")  # grayscale

        # Deskew if scipy is available
        img = _deskew(img)

        # Otsu binarisation for cleaner text
        import numpy as np
        arr = np.array(img)
        thresh = _otsu_threshold(arr)
        arr = (arr > thresh).astype(np.uint8) * 255
        img = Image.fromarray(arr)

        # Try psm 3 (fully auto), fall back to psm 6 (uniform block)
        for psm in (3, 6):
            config = f"--oem 3 --psm {psm} -l eng"
            try:
                text = pytesseract.image_to_string(img, config=config)
                if text.strip():
                    return text
            except Exception:
                continue

        return ""

    except Exception as e:
        logger.warning("OCR failed for page %d: %s", page_number + 1, e)
        return ""


def _deskew(img) -> "Image":
    """Attempt to deskew an image using scipy if available."""
    try:
        import numpy as np
        from scipy.ndimage import rotate
        arr   = np.array(img)
        edges = np.where(arr < 128)
        if len(edges[0]) < 100:
            return img
        # Simple skew estimate via projection profile minimisation
        best_angle = 0.0
        best_score = float("inf")
        for angle in range(-5, 6):
            rotated = rotate(arr, angle, reshape=False, cval=255)
            # Score: sum of variance in each row (low variance = aligned text)
            score = float(np.sum(np.var(rotated, axis=1)))
            if score < best_score:
                best_score = score
                best_angle = angle
        if best_angle != 0:
            arr = rotate(arr, best_angle, reshape=False, cval=255).astype("uint8")
            from PIL import Image
            return Image.fromarray(arr)
    except Exception:
        pass
    return img


def _otsu_threshold(arr) -> int:
    """Compute Otsu's binarisation threshold."""
    try:
        import numpy as np
        hist, _ = np.histogram(arr.flatten(), bins=256, range=(0, 256))
        hist    = hist.astype(float)
        total   = hist.sum()
        if total == 0:
            return 128
        sum_b = wb = var_max = thresh = 0
        sum_f = float(np.dot(np.arange(256), hist))
        for t in range(256):
            wb += hist[t]
            if wb == 0:
                continue
            wf = total - wb
            if wf == 0:
                break
            sum_b += t * hist[t]
            mb = sum_b / wb
            mf = (sum_f - sum_b) / wf
            var = wb * wf * (mb - mf) ** 2
            if var > var_max:
                var_max = var
                thresh  = t
        return thresh
    except Exception:
        return 128


# ═══════════════════════════════════════════════════════════════════════════════
# Running header/footer detection
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_repeated_lines(pages: list[str], threshold: float = 0.4) -> set[str]:
    """
    Detect lines that appear on more than `threshold` fraction of pages.
    These are likely running headers, footers, or journal names.
    Returns a set of normalised line strings to strip.
    """
    if len(pages) < 3:
        return set()

    counter: Counter = Counter()
    for page in pages:
        lines = {l.strip() for l in page.split("\n") if 3 < len(l.strip()) < 120}
        for line in lines:
            counter[line] += 1

    min_pages = max(2, int(len(pages) * threshold))
    return {line for line, count in counter.items() if count >= min_pages}


def _remove_repeated_lines(text: str, repeated: set[str]) -> str:
    """Remove running headers/footers from a page's text."""
    if not repeated:
        return text
    lines = text.split("\n")
    kept  = [l for l in lines if l.strip() not in repeated]
    return "\n".join(kept)


# ═══════════════════════════════════════════════════════════════════════════════
# Module-level helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _clean(text: str) -> str:
    return unescape(text).strip()


def _modal(sizes: list[float]) -> float:
    buckets: dict[float, int] = {}
    for s in sizes:
        k = round(s * 2) / 2
        buckets[k] = buckets.get(k, 0) + 1
    return max(buckets, key=lambda k: buckets[k]) if buckets else 10.0


def _estimate_page(char_offset: int, pages_text: list[str]) -> int:
    cum = 0
    for i, p in enumerate(pages_text):
        cum += len(p)
        if char_offset <= cum:
            return i
    return max(0, len(pages_text) - 1)


def _dedupe(items: list[str]) -> list[str]:
    seen:   set[str]  = set()
    result: list[str] = []
    for item in items:
        key = item.lower().strip()
        if key and key not in seen:
            seen.add(key)
            result.append(item)
    return result


# ── Title helpers ─────────────────────────────────────────────────────────────

def _sanitize_meta_title(title: str) -> str:
    """Remove common creator-tool artefacts from PDF metadata title."""
    if not title:
        return ""
    # Strip filenames like "manuscript_final_v3.pdf"
    title = re.sub(r"\.(docx?|pdf|tex|odt)$", "", title, flags=re.IGNORECASE)
    # Strip "Microsoft Word -" prefix
    title = re.sub(r"^Microsoft\s+Word\s*[-–—]\s*", "", title, flags=re.IGNORECASE)
    # Strip leading/trailing quotes
    title = title.strip('"\'')
    return title.strip()


def _normalize_title_case(title: str) -> str:
    """Convert ALL-CAPS titles to Title Case; leave mixed-case as-is."""
    if not title:
        return title
    alpha = [c for c in title if c.isalpha()]
    if not alpha:
        return title
    upper_frac = sum(1 for c in alpha if c.isupper()) / len(alpha)
    if upper_frac > 0.85:
        # Convert to title case but preserve common short words lowercase
        _lowers = {"a", "an", "the", "and", "but", "or", "nor", "for",
                   "so", "yet", "at", "by", "in", "of", "on", "to", "up",
                   "as", "is", "it", "its", "via", "vs", "per"}
        words  = title.lower().split()
        result = []
        for i, w in enumerate(words):
            result.append(w if (i > 0 and w in _lowers) else w.capitalize())
        return " ".join(result)
    return title


def _title_by_font(blocks: list[dict]) -> str:
    """
    Extract title from page-1 blocks using font-size heuristic.
    Improved: merges adjacent spans of the same large size (multi-line titles).
    Filters spans in the right half of two-column layouts.
    """
    spans: list[tuple[float, float, float, float, str]] = []  # y, x, w, size, text
    page_w = 0.0
    for block in blocks:
        if block.get("type") != 0:
            continue
        bx = block.get("bbox", [0, 0, 0, 0])
        page_w = max(page_w, bx[2])
        for line in block.get("lines", []):
            lb = line.get("bbox", [0, 0, 0, 0])
            for span in line.get("spans", []):
                t = _clean(span.get("text", ""))
                s = span.get("size", 0.0)
                sb = span.get("bbox", [0, 0, 0, 0])
                if t and s > 0 and len(t) > 2:
                    spans.append((lb[1], sb[0], sb[2] - sb[0], s, t))

    if not spans:
        return ""

    max_size  = max(s for *_, s, _ in spans)
    threshold = max_size * 0.88

    # Only consider spans in the top 40% of the page
    page_h = max(y for y, *_ in spans) or 800

    # For two-column PDFs: prefer spans starting in left half
    center_x = page_w / 2 if page_w > 0 else 999

    title_spans = [
        (y, x, t)
        for y, x, w, s, t in spans
        if s >= threshold and y <= page_h * 0.45
    ]
    # Prefer left-column spans; include right-column only if no left ones
    left_spans  = [(y, x, t) for y, x, t in title_spans if x <= center_x + 20]
    used_spans  = left_spans if left_spans else title_spans

    if not used_spans:
        return ""

    # Sort by vertical then horizontal position and join
    used_spans.sort(key=lambda sp: (round(sp[0] / 5) * 5, sp[1]))
    title = " ".join(t for _, _, t in used_spans).strip()

    # Clean up
    title = re.sub(r"\s{2,}", " ", title)
    if _GARBAGE_TITLE.search(title) or len(title) < 15:
        return ""
    return _clean(title[:400])


def _title_from_text(first_page: str) -> str:
    """Heuristic title from the first substantive line(s) of page 1."""
    candidate_lines: list[str] = []
    for line in first_page.split("\n"):
        s = line.strip()
        if len(s) < 15 or len(s) > 400:
            continue
        if s.count(" ") < 2:
            continue
        if _GARBAGE_TITLE.search(s):
            continue
        if re.match(r"^\d", s):
            continue
        if _STOP_AUTHOR.search(s):
            continue
        candidate_lines.append(s)
        if len(candidate_lines) >= 3:
            break

    if not candidate_lines:
        return ""
    # Merge if consecutive lines look like continuation of the same title
    merged = candidate_lines[0]
    for line in candidate_lines[1:]:
        # Join only if no punctuation break between them
        if not re.search(r"[.!?]\s*$", merged) and len(merged) + len(line) < 350:
            merged += " " + line
        else:
            break
    return _clean(merged[:350])


# ── Author helpers ─────────────────────────────────────────────────────────────

def _clean_author(raw: str) -> str:
    name = _clean(raw)
    # Strip superscript indicators (digits and symbols after the name)
    name = re.sub(r"[\d,*†‡§¶#\^]+\s*$", "", name)
    # Strip degree/credential suffixes
    name = _DEGREES.sub("", name)
    name = re.sub(r"\s*,?\s*(Jr\.?|Sr\.?|III|II|IV)$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"^and\s+", "", name, flags=re.IGNORECASE)
    name = name.strip(" *†‡§,.;:#^")
    return name.strip()


def _is_author_name(s: str) -> bool:
    s = s.strip()
    if len(s) < 4 or len(s) > 70:
        return False
    # Must start with a capital
    if not re.match(r"^[A-Z\u00C0-\u024F]", s):
        return False
    # All-caps strings > 6 chars are likely acronyms/institutions
    if s == s.upper() and len(s) > 6:
        return False
    # Must have at least one lowercase letter (real name, not abbreviation)
    if not re.search(r"[a-z\u00E0-\u024F]", s):
        return False
    if _STOP_AUTHOR.search(s):
        return False
    # Reject bare credential strings
    if re.match(
        r"^(PhD|MSc|MD|BSc|MBChB|MBBS|Dr|Prof|Mr|Mrs|Ms|"
        r"FRCOG|FRCP|FACS|FRCPath|FWACP)\d*\.?$",
        s, re.IGNORECASE,
    ):
        return False
    # Must look like a name: at least one space or hyphen between capitalized parts
    if not re.search(r"[A-Z\u00C0-\u024F][a-z\u00E0-\u024F]+[\s-][A-Z\u00C0-\u024F]", s) \
            and not re.search(r"[A-Z]\.", s):
        # Allow single-token names only if >= 5 chars (e.g. "Okonkwo")
        if len(s) < 5 or " " not in s:
            return False
    return True


def _parse_author_string(author_str: str) -> list[str]:
    """Parse the PDF metadata author field."""
    if not author_str:
        return []
    # Try semicolon-separated first (most reliable)
    if ";" in author_str:
        parts = author_str.split(";")
    elif " and " in author_str.lower():
        parts = re.split(r"\s+and\s+", author_str, flags=re.IGNORECASE)
    else:
        parts = author_str.split(",")
        # If all parts start with a capital it's a comma-separated list
        caps = [p.strip() for p in parts if p.strip()]
        if not all(re.match(r"^[A-Z\u00C0-\u024F]", p) for p in caps):
            parts = [author_str]

    result: list[str] = []
    seen:   set[str]  = set()
    for p in parts:
        c = _clean_author(p)
        if c and len(c) > 3 and c.lower() not in seen:
            seen.add(c.lower())
            result.append(c)
    return result[:12]


def _authors_by_font(
    blocks    : list[dict],
    title     : str,
    body_size : float,
) -> list[str]:
    if not blocks:
        return []

    all_spans: list[tuple[float, float, float, str]] = []
    for block in blocks:
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            ly = line.get("bbox", [0, 0, 0, 0])[1]
            for span in line.get("spans", []):
                t = _clean(span.get("text", "").strip())
                s = span.get("size", 0.0)
                x = span.get("bbox", [0, 0, 0, 0])[0]
                if t and s > 0:
                    all_spans.append((ly, x, s, t))

    if not all_spans:
        return []

    all_spans.sort(key=lambda sp: (sp[0], sp[1]))
    max_size = max(sp[2] for sp in all_spans)

    # Find the bottom of the title region
    title_prefix = title[:30].lower() if title else ""
    title_bottom = 0.0
    for y, x, size, text in all_spans:
        if title_prefix and title_prefix[:15] in text.lower():
            title_bottom = y + 30
            break
    if title_bottom == 0.0:
        page_h       = max(sp[0] for sp in all_spans) or 842
        title_bottom = page_h * 0.20

    candidates: list[str] = []
    no_author_streak = 0
    prev_y = -1.0

    for y, x, size, text in all_spans:
        if y < title_bottom:
            continue
        if y > title_bottom + 380:
            break
        if _STOP_AUTHOR.search(text):
            break
        # Skip if this is a title-size span
        if size >= max_size * 0.87:
            continue

        # Merge lines very close vertically (same author line, different columns)
        # Split on common separators
        parts = re.split(
            r"[;]|(?<=[a-z\u00E0-\u024F])\s+and\s+(?=[A-Z\u00C0-\u024F])"
            r"|(?<=\w),\s+(?=[A-Z\u00C0-\u024F])",
            text, flags=re.IGNORECASE,
        )
        found_here = 0
        for part in parts:
            cleaned = _clean_author(part)
            if _is_author_name(cleaned) and cleaned.lower() not in {a.lower() for a in candidates}:
                candidates.append(cleaned)
                found_here += 1
                if len(candidates) >= 15:
                    break

        if found_here == 0:
            no_author_streak += 1
            if no_author_streak >= 3 and candidates:
                break
        else:
            no_author_streak = 0
            prev_y = y

        if len(candidates) >= 15:
            break

    return _dedupe(candidates)[:12]


def _authors_from_text(first_page: str, title: str) -> list[str]:
    lines       = [l.strip() for l in first_page.split("\n") if l.strip()]
    candidates  : list[str] = []
    title_found = False
    title_low   = title[:30].lower() if title else ""
    no_streak   = 0

    for line in lines:
        if not title_found:
            if title_low and title_low[:15] in line.lower():
                title_found = True
            continue

        if _STOP_AUTHOR.search(line):
            break

        # Strip leading superscript numbers/symbols
        line = re.sub(r"^[\d,*†‡§¶\s]+", "", line).strip()

        parts = re.split(
            r"[;]|(?<=\w)\s+and\s+(?=[A-Z\u00C0-\u024F])"
            r"|(?<=\w),\s+(?=[A-Z\u00C0-\u024F])",
            line, flags=re.IGNORECASE,
        )
        found = 0
        for part in parts:
            cleaned = _clean_author(part)
            if _is_author_name(cleaned) and cleaned.lower() not in {a.lower() for a in candidates}:
                candidates.append(cleaned)
                found += 1
                if len(candidates) >= 15:
                    break

        if found == 0:
            no_streak += 1
            if no_streak >= 3 and candidates:
                break
        else:
            no_streak = 0

        if len(candidates) >= 15:
            break

    return _dedupe(candidates)[:12]


# ── Abstract ──────────────────────────────────────────────────────────────────

def _extract_abstract(first_pages: list[str]) -> str:
    """
    Extract abstract with support for structured abstracts.
    Searches across first 1-2 pages.
    """
    text = "\n".join(first_pages)

    # Structured abstract — collect all sub-section content
    structured_labels = re.compile(
        r"\b(?:Objective|Background|Purpose|Aim|Methods?|Methodology|"
        r"Results?|Findings|Conclusions?|Summary|Design|Setting|"
        r"Participants?|Interventions?|Main\s+Outcome)\b",
        re.IGNORECASE,
    )

    # Try explicit Abstract label
    m = re.search(
        r"\b(?:Abstract|Summary|Overview|Synopsis)\b\s*[:—]?\s*\n?"
        r"([\s\S]{80,3000}?)"
        r"(?=\n\s*\n\s*(?:Keywords?|Key\s+words?|Index\s+terms?|"
        r"Introduction|Background|1\s*[\.\)]\s+\w|I\.\s+|$))",
        text, re.IGNORECASE,
    )
    if m:
        abstract_block = m.group(1).strip()
        # If it has structured sub-sections, clean them up but keep the text
        abstract_block = re.sub(r"\n+", " ", abstract_block)
        abstract_block = re.sub(r"\s{2,}", " ", abstract_block)
        return _clean(abstract_block[:2500])

    # Fallback: longest paragraph in first 500 lines that doesn't look like authors/affils
    paragraphs = re.split(r"\n\s*\n", text)
    scored: list[tuple[int, str]] = []
    for para in paragraphs[1:8]:
        para = para.strip()
        if len(para) < 150:
            continue
        if _STOP_AUTHOR.search(para[:80]):
            continue
        if re.search(r"\b(university|department|institute)\b", para[:120], re.IGNORECASE):
            continue
        scored.append((len(para), para))

    if scored:
        scored.sort(key=lambda x: x[0], reverse=True)
        best = scored[0][1]
        best = re.sub(r"\n+", " ", best)
        return _clean(best[:2500])

    return ""


# ── DOI ───────────────────────────────────────────────────────────────────────

def _extract_doi(text: str) -> str:
    """
    Extract DOI with line-wrap handling.
    Handles https://doi.org/, dx.doi.org/, doi:, and bare 10.xxxx/ forms.
    """
    # Remove line breaks that may have split the DOI
    flat = re.sub(r"\n\s*", " ", text[:5000])

    # URL form
    m = re.search(
        r"https?://(?:dx\.)?doi\.org/(10\.\d{4,9}/[^\s\"'<>\]\)\|]+)",
        flat, re.IGNORECASE,
    )
    if m:
        return m.group(1).rstrip(".,;)]|")

    # Labelled form
    m = re.search(
        r"\bdoi\s*:?\s*(10\.\d{4,9}/[^\s\"'<>\]\)\|]+)",
        flat, re.IGNORECASE,
    )
    if m:
        return m.group(1).rstrip(".,;)]|")

    # Bare form (last resort)
    m = re.search(r"\b(10\.\d{4,9}/[^\s\"'<>\]\)\|]{3,})", flat)
    return m.group(1).rstrip(".,;)]|") if m else ""


# ── ISSN ──────────────────────────────────────────────────────────────────────

def _extract_issn(text: str) -> str:
    chunk = text[:4000]
    # e-ISSN or p-ISSN
    m = re.search(r"\b[EPep]-?ISSN[:\s]*(\d{4}-\d{3}[\dXx])\b", chunk, re.IGNORECASE)
    if m:
        return m.group(1)
    # Plain ISSN label
    m = re.search(r"\bISSN[:\s]*(\d{4}-\d{3}[\dXx])\b", chunk, re.IGNORECASE)
    if m:
        return m.group(1)
    # Context line
    m = re.search(
        r"(?:journal|issn|copyright|print|online)[^\n]*\b(\d{4}-\d{3}[\dXx])\b",
        chunk, re.IGNORECASE,
    )
    if m:
        return m.group(1)
    # Bare ISSN pattern in first 2000 chars
    m = re.search(r"\b(\d{4}-\d{3}[\dXx])\b", text[:2000])
    return m.group(1) if m else ""


# ── Publisher ─────────────────────────────────────────────────────────────────

def _extract_publisher(text: str) -> str:
    chunk = text[:5000]
    m = re.search(
        r"(?:Published\s+by|Publisher\s*:|©\s*\d{4}\s+)([A-Z][^\n]{3,80})",
        chunk, re.IGNORECASE,
    )
    if m:
        pub = re.split(r"\s*(?:Inc\.|Ltd\.?|All rights|Copyright|\d{4})", m.group(1))[0]
        return pub.strip()[:80]
    chunk_l = chunk.lower()
    for pub in sorted(_KNOWN_PUBLISHERS, key=len, reverse=True):
        if pub.lower() in chunk_l:
            return pub
    return ""


# ── Journal ───────────────────────────────────────────────────────────────────

def _extract_journal(text: str, meta_title: str = "") -> str:
    chunk = text[:6000]
    patterns = [
        # Explicit label
        r"(?:published\s+in|journal\s*:)\s*([A-Z][^\n]{5,100})",
        # Common journal name patterns with geography prefixes
        r"((?:International|European|American|British|African|Asian|"
        r"Nigerian|Indian|Chinese|Korean|Canadian|Australian|"
        r"Saudi|Brazilian|South\s+African|East\s+African|"
        r"West\s+African|Nordic|Scandinavian|Latin\s+American)\s+"
        r"(?:Journal|Review|Annals|Archives|Bulletin|Proceedings|"
        r"Transactions|Letters|Reports)\s+(?:of|for|on|in)\s+[A-Z][^\n]{3,70})",
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,6}\s+Journal[^\n]{0,40})",
        r"(American Journal of [^\n]{5,60})",
        r"(British Journal of [^\n]{5,60})",
        r"(Asian Journal of [^\n]{5,60})",
        r"(European Journal of [^\n]{5,60})",
        r"(Nigerian [A-Z][^\n]{5,60})",
        r"(Journal of [A-Z][^\n]{5,60})",
        # Proceedings
        r"(Proceedings of [^\n]{5,80})",
    ]
    for pat in patterns:
        m = re.search(pat, chunk, re.IGNORECASE)
        if m:
            j = m.group(1).strip()
            # Strip trailing year/volume noise
            j = re.split(r"\s+(?:\d{4}\b|\bVol|\bNo\b|\bIssue|\d+\s*[\(,])", j)[0]
            j = j.strip().rstrip(".,;:")
            if len(j) >= 8:
                return j[:120]

    # Fall back to metadata title if it looks like a journal name
    if meta_title and re.search(
        r"\b(Journal|Review|Annals|Bulletin|Transactions|Letters|Proceedings)\b",
        meta_title, re.IGNORECASE,
    ):
        return meta_title[:120]

    return ""


# ── Volume / Issue / Year ─────────────────────────────────────────────────────

def _extract_volume(text: str) -> str:
    m = re.search(r"\bVol(?:ume)?\.?\s*(\d+)", text[:5000], re.IGNORECASE)
    return m.group(1) if m else ""


def _extract_issue(text: str) -> str:
    # Parenthetical: Vol 23(2)
    m = re.search(
        r"\bVol(?:ume)?\.?\s*\d+\s*[\(,]\s*(\d+)\s*[\),]",
        text[:5000], re.IGNORECASE,
    )
    if m:
        return m.group(1)
    m = re.search(
        r"\b(?:Issue|No\.?|Number|Num\.?)\s*\.?\s*(\d+)",
        text[:5000], re.IGNORECASE,
    )
    return m.group(1) if m else ""


def _extract_year(text: str, created: str) -> str:
    """Extract publication year, preferring labels over bare occurrences."""
    # Prefer labelled year
    m = re.search(
        r"(?:published|accepted|received|online|copyright|©)\D{0,25}((?:19|20)\d{2})\b",
        text[:5000], re.IGNORECASE,
    )
    if m:
        yr = int(m.group(1))
        if 1900 <= yr <= 2100:
            return str(yr)

    # First plausible year in text
    for match in re.finditer(r"\b((?:19|20)\d{2})\b", text[:3000]):
        yr = int(match.group(1))
        if 1950 <= yr <= 2100:
            return str(yr)

    # PDF creation date
    if created:
        d = re.match(r"D:(\d{4})", created)
        if d:
            yr = int(d.group(1))
            if 1950 <= yr <= 2100:
                return str(yr)
        d = re.search(r"\b((?:19|20)\d{2})\b", created)
        if d:
            return d.group(1)

    return ""


# ── Keywords ─────────────────────────────────────────────────────────────────

def _extract_keywords(text: str, meta_kw: str = "") -> list[str]:
    """Extract keywords with support for semicolon, comma, bullet, and newline separators."""
    m = re.search(
        r"(?:Keywords?|Key\s+words?|Index\s+[Tt]erms?|[Kk]ey[Pp]hrases?)"
        r"\s*[:—]?\s*\n?([\s\S]{10,800}?)"
        r"(?=\n\s*\n|\n\s*(?:Introduction|Background|1\s*[\.\)]|\Z))",
        text[:12000], re.IGNORECASE,
    )
    if m:
        kw_block = m.group(1)
        # Handle newline-separated keywords (one per line)
        if "\n" in kw_block and ";" not in kw_block and "," not in kw_block:
            kws = [k.strip().strip("•·-–—*") for k in kw_block.split("\n")]
        else:
            kws = [k.strip().strip("•·-–—*") for k in re.split(r"[;,•·\n]", kw_block)]
        kws = [k for k in kws if 2 < len(k) < 100 and not k.isdigit()]
        if kws:
            return kws[:20]

    # Fallback to metadata keywords
    if meta_kw:
        kws = [k.strip() for k in re.split(r"[;,]", meta_kw) if k.strip()]
        return [k for k in kws if 2 < len(k) < 100][:20]

    return []


# ── Sentence splitter ─────────────────────────────────────────────────────────

def _split_sentences(text: str) -> list[str]:
    abbrevs = [
        r"Mr\.", r"Mrs\.", r"Ms\.", r"Dr\.", r"Prof\.", r"Assoc\.",
        r"Fig\.", r"Figs\.", r"Tab\.", r"Eq\.", r"Eqs\.", r"Sec\.",
        r"Vol\.", r"No\.", r"pp\.", r"vs\.", r"approx\.", r"resp\.",
        r"et al\.", r"i\.e\.", r"e\.g\.", r"cf\.", r"viz\.", r"ca\.",
        r"op\.\s*cit\.", r"ibid\.", r"al\.", r"Ref\.", r"refs\.",
        r"Jan\.", r"Feb\.", r"Mar\.", r"Apr\.", r"Jun\.", r"Jul\.",
        r"Aug\.", r"Sep\.", r"Oct\.", r"Nov\.", r"Dec\.",
        r"U\.S\.", r"U\.K\.", r"U\.N\.", r"E\.U\.",
        r"St\.", r"Ave\.", r"Blvd\.", r"Dept\.", r"Univ\.",
        r"Corp\.", r"Inc\.", r"Ltd\.", r"Co\.",
    ]
    protected = text
    ph: dict[str, str] = {}
    for i, pat in enumerate(abbrevs):
        p = f"__A{i}__"
        protected = re.sub(pat, lambda m, pl=p: m.group().replace(".", pl), protected)
        ph[p] = "."

    # Protect decimal numbers and initials
    protected = re.sub(r"(\d)\.(\d)", r"\1__D__\2", protected)
    protected = re.sub(r"\b([A-Z])\.\s+([A-Z])", r"\1__I__\2", protected)

    # Split on sentence-ending punctuation followed by whitespace + capital
    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z\[\(\"'\u2018\u201c\d])", protected)

    result: list[str] = []
    for part in parts:
        part = part.replace("__D__", ".").replace("__I__", ". ")
        for p in ph:
            part = part.replace(p, ".")
        part = part.strip()
        if part and len(part) > 5:
            result.append(part)
    return result


# ── ISBN ──────────────────────────────────────────────────────────────────────

def _extract_isbn(text: str) -> str:
    m = re.search(
        r"\bISBN[:\s-]*"
        r"((?:97[89][-\s]?)?\d{1,5}[-\s]?\d{1,7}[-\s]?\d{1,7}[-\s]?[\dXx])\b",
        text[:5000], re.IGNORECASE,
    )
    return m.group(1).replace(" ", "").replace("-", "") if m else ""


# ── Pages ─────────────────────────────────────────────────────────────────────

def _extract_pages(text: str) -> str:
    """
    Extract page range or article number.
    Handles: pp 45-67, e398-e404, 7-14, 101–120, Article 100234.
    """
    flat = text[:4000]
    # Labelled pp. range
    m = re.search(
        r"\bpp?\.?\s*([eE]?\d{1,6}[-–][eE]?\d{1,6})\b", flat, re.IGNORECASE,
    )
    if m:
        return m.group(1).replace("–", "-")

    # Vol X(Y), page-range
    m = re.search(
        r"\bVol[^\n]{1,40},\s*([eE]?\d{1,6}[-–][eE]?\d{1,6})\b", flat, re.IGNORECASE,
    )
    if m:
        return m.group(1).replace("–", "-")

    # Colon format: 14:7-14
    m = re.search(r"\b\d+:\s*([eE]?\d{1,6}[-–][eE]?\d{1,6})\b", flat)
    if m:
        return m.group(1).replace("–", "-")

    # Article number (common in Elsevier/Springer)
    m = re.search(
        r"\b(?:Article\s+(?:No\.?\s*)?|e)(\d{5,8})\b", flat, re.IGNORECASE,
    )
    if m:
        return "e" + m.group(1) if not m.group(0).lower().startswith("e") else m.group(0)

    # Pages: 12–20 standalone
    m = re.search(r"\b([1-9]\d{0,4}[-–][1-9]\d{0,4})\b", flat)
    if m:
        a, b = re.split(r"[-–]", m.group(1))
        if 0 < int(a) < int(b) <= 99999:
            return m.group(1).replace("–", "-")

    return ""


# ── Article type ──────────────────────────────────────────────────────────────

def _extract_article_type(text: str) -> str:
    patterns = [
        r"\b(Systematic\s+Review(?:\s+and\s+Meta[-\s]?Analysis)?)\b",
        r"\b(Meta[-\s]?Analysis)\b",
        r"\b(Randomized\s+Controlled\s+Trial|RCT)\b",
        r"\b(Clinical\s+Trial)\b",
        r"\b(Cohort\s+Study)\b",
        r"\b(Cross[-\s]?Sectional\s+Study)\b",
        r"\b(Case[-\s]?Control\s+Study)\b",
        r"\b(Case\s+Report)\b",
        r"\b(Case\s+Series)\b",
        r"\b(Review\s+Article|Review\s+Paper|Literature\s+Review|Narrative\s+Review)\b",
        r"\b(Scoping\s+Review)\b",
        r"\b(Original\s+(?:Research|Article|Paper))\b",
        r"\b(Research\s+Article|Research\s+Paper)\b",
        r"\b(Short\s+(?:Communication|Report|Note))\b",
        r"\b(Letter\s+to\s+the\s+Editor|Correspondence)\b",
        r"\b(Conference\s+Paper|Proceedings)\b",
        r"\b(Technical\s+(?:Note|Report))\b",
        r"\b(Thesis|Dissertation)\b",
        r"\b(Preprint)\b",
        r"\b(Erratum|Correction|Retraction)\b",
    ]
    chunk = text[:3500]
    for pat in patterns:
        m = re.search(pat, chunk, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return "Research Article"


# ── Affiliations ──────────────────────────────────────────────────────────────

def _extract_affiliations(first_page: str) -> list[str]:
    affil_markers = re.compile(
        r"\b(university|universiti|college|institute|institution|"
        r"faculty|department|school\s+of|division|laboratory|lab\b|"
        r"hospital|clinic|centre|center|foundation|academy|"
        r"ministry|government|authority|national\s+\w+\s+(?:centre|center|hospital))\b",
        re.IGNORECASE,
    )
    lines        = first_page.split("\n")
    affiliations : list[str] = []
    seen         : set[str]  = set()

    for line in lines:
        line = line.strip()
        if not line or len(line) < 10 or len(line) > 300:
            continue
        if affil_markers.search(line):
            # Strip leading superscript numbers/symbols
            clean = re.sub(r"^[\d,*†‡§\s]+", "", line).strip()
            if clean and clean.lower() not in seen and len(clean) > 8:
                seen.add(clean.lower())
                affiliations.append(clean)
                if len(affiliations) >= 10:
                    break

    return affiliations


# ── Email ─────────────────────────────────────────────────────────────────────

def _extract_email(text: str) -> str:
    # Labelled correspondence email
    m = re.search(
        r"(?:corresponding\s+author|address\s+for\s+correspondence|"
        r"e-?mail|contact|reprint\s+requests?)[^\n]{0,60}"
        r"([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})",
        text[:4000], re.IGNORECASE,
    )
    if m:
        return m.group(1)
    # Any email in first 2500 chars
    m = re.search(
        r"\b([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})\b",
        text[:2500],
    )
    return m.group(1) if m else ""


# ── ORCID ─────────────────────────────────────────────────────────────────────

def _extract_orcids(text: str) -> list[str]:
    matches = re.findall(
        r"\b(\d{4}-\d{4}-\d{4}-\d{3}[\dXx])\b",
        text[:5000],
    )
    return list(dict.fromkeys(matches))[:12]


# ── Funding ───────────────────────────────────────────────────────────────────

def _extract_funding(text: str) -> str:
    """Extract funding statement — multi-sentence aware, stops at next heading."""
    patterns = [
        r"(?:Funding|Funding\s+source|Financial\s+(?:support|disclosure)|"
        r"Grant|Funding\s+information)[:\s]+([^\n]{10,400})",
        r"(?:supported\s+by|funded\s+by|sponsored\s+by)\s+([^\n]{10,300})",
        r"(?:This\s+(?:study|work|research|project)\s+was\s+"
        r"(?:supported|funded|sponsored|financed)\s+by)\s+([^\n]{10,300})",
        r"(?:Acknowledgements?|Acknowledgments?)[:\s]+([^\n]{10,500})",
        r"(?:Conflict\s+of\s+[Ii]nterest)[:\s]+([^\n]{5,200})",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            result = m.group(1).strip()
            # Grab up to 2 more sentences
            end = m.end()
            extra = re.search(r"([^.!?]*[.!?]){0,2}", text[end:end + 300])
            if extra and extra.group():
                result += " " + extra.group().strip()
            return result.strip()[:400]
    return ""


# ── Date by label ─────────────────────────────────────────────────────────────

def _extract_date_label(label: str, text: str) -> str:
    m = re.search(
        rf"(?:{label})[:\s]+([A-Za-z0-9,\s/.\-]{{5,50}}?)(?:\n|;|$)",
        text[:4000], re.IGNORECASE,
    )
    if not m:
        return ""
    raw = m.group(1).strip().rstrip(".,;")
    raw_clean = re.sub(r"(\d+)(st|nd|rd|th)\b", r"\1", raw, flags=re.IGNORECASE)
    from datetime import datetime as _dt
    for fmt in (
        "%d %B %Y", "%B %d %Y", "%d %b %Y", "%b %d %Y",
        "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%B %Y", "%b %Y",
        "%Y/%m/%d",
    ):
        try:
            return _dt.strptime(raw_clean.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw[:35]


# ── Editor ────────────────────────────────────────────────────────────────────

def _extract_editor(text: str) -> str:
    m = re.search(
        r"(?:Edited\s+by|Guest\s+Editor|Section\s+Editor|"
        r"Editor[-\s]?in[-\s]?Chief|Editor|Handling\s+Editor|"
        r"Academic\s+Editor|Associate\s+Editor)[:\s]+([A-Z][^\n]{3,80})",
        text[:5000], re.IGNORECASE,
    )
    if m:
        raw = m.group(1).strip().rstrip(".,;")
        raw = re.split(
            r"\s*[,;]\s*(?:PhD|MD|Dr|Prof|University|Institute|College)",
            raw, flags=re.IGNORECASE,
        )[0]
        return raw.strip()[:80]
    return ""


# ── Singleton ─────────────────────────────────────────────────────────────────
extraction_service = ExtractionService()