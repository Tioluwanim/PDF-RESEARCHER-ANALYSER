"""
extraction_service.py — Universal PDF extraction with OCR fallback.

Supports:
  1. Digital PDFs  — PyMuPDF dict-mode (font-size aware, fast, accurate)
  2. Scanned PDFs  — pytesseract OCR via pdf2image (auto-detected, fallback)
  3. Mixed PDFs    — page-by-page: digital pages use PyMuPDF, scanned use OCR

Detection logic:
  - A page is considered "scanned" if PyMuPDF extracts < 50 chars of text
  - OCR runs only on those pages (saves time on hybrid docs)
  - OCR DPI: 300 for body pages, 400 for first page (better metadata extraction)

Extraction improvements:
  - Title: font-size cascade → metadata validation → text heuristic
  - Authors: font-position scan → metadata field → text heuristic
    Both cleaned of 35+ degree types, superscripts, asterisks
  - Abstract: explicit label → first substantial paragraph
  - DOI: URL form / labelled / bare (3-stage)
  - ISSN: E-ISSN / ISSN labelled / context-line / bare (4-stage)
  - Journal: 8 pattern variants including geography prefixes
  - Volume/Issue: parenthetical Vol X(Y) + standalone patterns
  - Year: published/accepted labels → first year in text → PDF date
  - Keywords: Keywords: / Key words: / Index terms: labels

Section detection:
  - 60+ keywords across clinical, pharma, CS, social science, engineering
  - Handles numbered (1., 2.1, II., A.) and bold-marked headings
  - Deduplication within 5 lines

Chunking:
  - Sentence-aware sliding window
  - Overlap carry-over preserves context across chunks
"""

from __future__ import annotations

import re
import uuid
from html import unescape
from pathlib import Path

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

# ── Unicode / ligature map ────────────────────────────────────────────────────
_LIGATURES: dict[str, str] = {
    "\ufb01": "fi",  "\ufb02": "fl",  "\ufb00": "ff",
    "\ufb03": "ffi", "\ufb04": "ffl",
    "\u2019": "'",   "\u2018": "'",
    "\u201c": '"',   "\u201d": '"',
    "\u2013": "-",   "\u2014": "--",
    "\u00a0": " ",   "\u00ad": "",
    "\u2022": "-",   "\u00b7": ".",
    "\u00d7": "x",   "\u03b1": "alpha",
    "\u03b2": "beta","\u03bc": "mu",
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
    |^(doi|issn|isbn|pmid)\b
    |(copyright|all\s+rights\s+reserved|unauthorized\s+reproduction)
    |\bwww\.\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# ── Author stop markers ───────────────────────────────────────────────────────
_STOP_AUTHOR = re.compile(
    r"""\b(
        university|universiti|universidade|università|université|
        universidad|college|institute|institution|faculty|department|
        dept\b|school\s+of|division\s+of|centre|center|laboratory|
        lab\b|hospital|clinic|medical\s+center|
        obafemi|awolowo|lagos|ibadan|nairobi|accra|pretoria|
        johannesburg|kumasi|dar\s+es\s+salaam|greenville|carolina|
        london|oxford|cambridge|new\s+york|beijing|shanghai|tokyo|
        abstract|introduction|background|objective|purpose|
        keywords?|key\s+words?|index\s+terms?|
        received|accepted|published|revised|available\s+online|
        correspondence|corresponding\s+author|
        email|e-mail|tel\b|fax\b|@|\bhttp|\bwww\b|orcid
    )\b""",
    re.IGNORECASE | re.VERBOSE,
)

# ── Degree suffixes ───────────────────────────────────────────────────────────
_DEGREES = re.compile(
    r"\s*,?\s*\b("
    r"MSc|M\.Sc|M\.S\.|MPhil|PhD|Ph\.D|DPhil|"
    r"MD|M\.D|MBBS|MBChB|MBBCh|MBBChir|"
    r"BSc|B\.Sc|B\.S\.|BA\b|MPH|DrPH|MSPH|"
    r"PharmD|Pharm\.D|BPharm|MPharm|"
    r"FRCOG|FRCP|FRCPCH|FRCS|FACS|FRCPath|"
    r"FMCPath|FWACP|FMCPH|DVM|DDS|DMD|"
    r"MA\b|MBA|MEd|MPA|MFA|EdD|PsyD|ScD|DSc|"
    r"FCPS|MRCP|MRCOG|MCPath"
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
    "African Journals Online", "AJOL",
]

# ── OCR availability flag (lazy-checked) ─────────────────────────────────────
_OCR_AVAILABLE: bool | None = None


def _check_ocr() -> bool:
    """Check once whether pytesseract + pdf2image are available."""
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

            pages_text, metadata = self._extract_from_pdf(doc.file_path, slog)
            doc.full_text = "\n\n".join(pages_text)
            doc.metadata  = metadata
            slog.info(
                "Extracted %d pages · %d words · scanned=%s",
                metadata.page_count, metadata.word_count,
                metadata.language == "ocr",
            )

            doc.sections = self._detect_sections(doc.full_text, pages_text, slog)
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
            slog.error("Extraction failed: %s", e)

        return doc

    # ── PDF extraction ────────────────────────────────────────────────────────

    def _extract_from_pdf(
        self,
        file_path : str,
        slog      : ServiceLogger,
    ) -> tuple[list[str], DocumentMetadata]:

        pdf_path = Path(file_path)
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {file_path}")

        pages_text:   list[str]   = []
        font_sizes:   list[float] = []
        page0_blocks: list[dict]  = []
        total_words   = 0
        ocr_page_count = 0

        with fitz.open(str(pdf_path)) as pdf:
            page_count  = len(pdf)
            raw_meta    = pdf.metadata or {}
            meta_title  = _clean(raw_meta.get("title")    or "")
            meta_author = _clean(raw_meta.get("author")   or "")
            meta_kw     = _clean(raw_meta.get("keywords") or "")
            created     = (raw_meta.get("creationDate")   or "").strip()

            # Read raw PDF bytes once for OCR (avoid re-opening)
            pdf_bytes = pdf_path.read_bytes()

            # Collect font/block data from first 3 pages
            for pn in range(min(3, page_count)):
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

            # Extract text page by page — OCR fallback for scanned pages
            ocr_available = _check_ocr()
            for pn in range(page_count):
                raw_text = pdf[pn].get_text("text").strip()

                if len(raw_text) < 50 and ocr_available:
                    # Scanned page — use OCR
                    ocr_text = _ocr_page(pdf_bytes, pn, dpi=400 if pn == 0 else 300)
                    text = self._clean_page_text(ocr_text)
                    if text:
                        ocr_page_count += 1
                        slog.debug("Page %d: OCR (%d chars)", pn + 1, len(text))
                    else:
                        text = self._clean_page_text(raw_text)
                else:
                    text = self._clean_page_text(raw_text)

                pages_text.append(text)
                total_words += len(text.split())

        self._body_font_size = _modal(font_sizes) if font_sizes else 10.0
        first3 = "\n".join(pages_text[:3])
        full   = "\n".join(pages_text)

        slog.info("OCR: %d/%d pages via OCR", ocr_page_count, page_count)

        # ── Resolve all metadata fields ───────────────────────────────────────
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
        abstract       = _extract_abstract(pages_text[0] if pages_text else "")
        article_type   = _extract_article_type(first3)
        editor         = _extract_editor(first3)
        affiliations   = _extract_affiliations(pages_text[0] if pages_text else "")
        corr_email     = _extract_email(first3)
        orcids         = _extract_orcids(first3)
        funding        = _extract_funding(full[:8000])
        received_date  = _extract_date_label("received", first3)
        accepted_date  = _extract_date_label("accepted", first3)
        published_date = _extract_date_label("published|online", first3)

        metadata = DocumentMetadata(
            title              = title,
            authors            = authors,
            abstract           = abstract,
            keywords           = keywords,
            doi                = doi,
            issn               = issn,
            isbn               = isbn,
            publisher          = publisher,
            journal            = journal,
            volume             = volume,
            issue              = issue,
            pages              = pages,
            article_type       = article_type,
            editor             = editor,
            year               = year,
            received_date      = received_date,
            accepted_date      = accepted_date,
            published_date     = published_date,
            affiliations       = affiliations,
            corresponding_email= corr_email,
            orcids             = orcids,
            funding            = funding,
            page_count         = page_count,
            word_count         = total_words,
            created_at         = year or created,
            file_size_bytes    = pdf_path.stat().st_size,
            language           = "ocr" if ocr_page_count > 0 else "en",
        )

        slog.info(
            "title='%.50s' | authors=%d | doi=%s | issn=%s | journal='%.35s' | pages=%s",
            title, len(authors), doi or "—", issn or "—", journal or "—", pages or "—",
        )
        return pages_text, metadata

    # ── Title resolution ──────────────────────────────────────────────────────

    def _resolve_title(
        self,
        meta_title   : str,
        page0_blocks : list[dict],
        pages_text   : list[str],
    ) -> str:
        # Stage 1 — PDF metadata (only if usable)
        if (meta_title
                and len(meta_title) >= 20
                and not _GARBAGE_TITLE.search(meta_title)
                and meta_title.count(" ") >= 2):
            return meta_title

        # Stage 2 — Largest font span on page 1
        font_title = _title_by_font(page0_blocks)
        if font_title and len(font_title) >= 15:
            return font_title

        # Stage 3 — Text heuristic
        return _title_from_text(pages_text[0] if pages_text else "")

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

        # Stage 3 — Text scan
        if pages_text:
            return _authors_from_text(pages_text[0], title)

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
            if not s or len(s) > 120:
                continue
            if re.match(r"^\d+\.?\s*$", s):
                continue
            st = self._classify_heading(s)
            if not st:
                continue
            if i - seen.get(st, -99) < 5:
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
                page_end     = _estimate_page(chars_before, pages_text),
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
        norm = re.sub(r"^(\d+(\.\d+)*\.?|[IVX]+\.|[A-Z]\.)[\s\u00a0]+", "", norm)
        norm = norm.strip("*_").strip()
        for st, pattern in self._section_patterns.items():
            if pattern.search(norm):
                return st
        return None

    def _compile_section_patterns(self) -> dict[SectionType, re.Pattern]:
        extended: dict[str, list[str]] = {
            "abstract": [
                "abstract", "summary", "executive summary", "overview",
                "synopsis", "highlights", "graphical abstract",
                "lay summary", "plain language summary",
            ],
            "introduction": [
                "introduction", "background", "motivation", "rationale",
                "context", "problem statement", "general introduction",
                "study rationale", "scope", "preface",
                "aims and objectives", "objectives", "aim of the study",
                "purpose of the study",
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
            ],
            "results": [
                "results", "findings", "outcomes", "observations",
                "experimental results", "simulation results",
                "numerical results", "empirical results",
                "pharmacokinetic results", "clinical results",
                "performance evaluation", "evaluation", "experiments",
                "case study", "case studies",
            ],
            "discussion": [
                "discussion", "general discussion",
                "results and discussion", "results and analysis",
                "analysis and discussion", "discussion and conclusion",
                "discussion and conclusions", "interpretation", "analysis",
            ],
            "conclusion": [
                "conclusion", "conclusions", "concluding remarks",
                "summary and conclusion", "summary and conclusions",
                "final remarks", "closing remarks", "summary",
                "future work", "future directions", "future research",
                "limitations", "study limitations",
                "implications", "clinical implications",
                "recommendations", "practical implications",
            ],
            "references": [
                "references", "bibliography", "works cited",
                "literature cited", "citations", "sources", "reference list",
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

    def _chunk_document(self, doc: ProcessedDocument, slog: ServiceLogger) -> list[TextChunk]:
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
            "Chunked: %d (CHUNK_SIZE=%d OVERLAP=%d)", len(chunks), CHUNK_SIZE, CHUNK_OVERLAP,
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
                        chunk_id=str(uuid.uuid4()), doc_id=doc_id,
                        content=ct, section_type=section_type,
                        page_number=page_number,
                    ))
                all_words = " ".join(current).split()
                carry     = all_words[-CHUNK_OVERLAP:] if len(all_words) > CHUNK_OVERLAP else all_words
                current   = [" ".join(carry)]
                cur_len   = len(carry)
            current.append(sent)
            cur_len += sw

        if current:
            ct = " ".join(current).strip()
            if len(ct) >= MIN_CHUNK_LENGTH:
                chunks.append(TextChunk(
                    chunk_id=str(uuid.uuid4()), doc_id=doc_id,
                    content=ct, section_type=section_type,
                    page_number=page_number,
                ))
        return chunks

    # ── Text cleaning ─────────────────────────────────────────────────────────

    @staticmethod
    def _clean_page_text(text: str) -> str:
        for bad, good in _LIGATURES.items():
            text = text.replace(bad, good)
        text = unescape(text)
        text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)
        text = re.sub(r"[^\x20-\x7E\n]", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text.strip()


# ═══════════════════════════════════════════════════════════════════════════════
# OCR helper
# ═══════════════════════════════════════════════════════════════════════════════

def _ocr_page(pdf_bytes: bytes, page_number: int, dpi: int = 300) -> str:
    """
    Run OCR on a single page of a PDF.
    Uses pdf2image to render the page to an image, then pytesseract for OCR.
    Returns empty string if OCR fails for any reason.
    """
    try:
        import pytesseract
        from pdf2image import convert_from_bytes
        from PIL import Image

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

        img = images[0]

        # Pre-process for better OCR accuracy:
        # Convert to grayscale, increase contrast slightly
        img = img.convert("L")

        # Tesseract config: treat as a block of text, single column
        config = (
            "--oem 3 "      # LSTM engine
            "--psm 6 "      # assume uniform block of text
            "-l eng"        # English (add more languages if needed)
        )
        text = pytesseract.image_to_string(img, config=config)
        return text

    except Exception as e:
        logger.warning("OCR failed for page %d: %s", page_number + 1, e)
        return ""


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
        if item.lower() not in seen:
            seen.add(item.lower())
            result.append(item)
    return result


# ── Title ─────────────────────────────────────────────────────────────────────

def _title_by_font(blocks: list[dict]) -> str:
    spans: list[tuple[float, float, str]] = []
    for block in blocks:
        if block.get("type") != 0:
            continue
        by = block.get("bbox", [0, 0, 0, 0])[1]
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                t = _clean(span.get("text", ""))
                s = span.get("size", 0.0)
                if t and s > 0 and len(t) > 2:
                    spans.append((by, s, t))

    if not spans:
        return ""

    max_size  = max(s for _, s, _ in spans)
    threshold = max_size * 0.91

    title_spans = sorted(
        [(y, t) for y, s, t in spans if s >= threshold],
        key=lambda x: x[0],
    )
    page_h      = max(y for y, _, _ in spans) or 800
    title_spans = [(y, t) for y, t in title_spans if y <= page_h * 0.45]

    if not title_spans:
        return ""

    title = " ".join(t for _, t in title_spans).strip()
    if _GARBAGE_TITLE.search(title) or len(title) < 15:
        return ""
    return _clean(title[:300])


def _title_from_text(first_page: str) -> str:
    for line in first_page.split("\n"):
        s = line.strip()
        if len(s) < 15 or len(s) > 350:
            continue
        if s.count(" ") < 2:
            continue
        if _GARBAGE_TITLE.search(s):
            continue
        if re.match(r"^\d", s):
            continue
        return _clean(s)
    return ""


# ── Authors ───────────────────────────────────────────────────────────────────

def _clean_author(raw: str) -> str:
    name = _clean(raw)
    name = _DEGREES.sub("", name)
    name = re.sub(r"[,\s]*[\d,]+[*†‡§]*\s*$", "", name)
    name = re.sub(r"^and\s+", "", name, flags=re.IGNORECASE)
    name = name.strip(" *†‡§,.;:")
    return name.strip()


def _is_author_name(s: str) -> bool:
    s = s.strip()
    if len(s) < 4 or len(s) > 60:
        return False
    if not re.match(r"^[A-Z]", s):
        return False
    if s == s.upper() and len(s) > 6:
        return False
    if not re.search(r"[a-z]", s):
        return False
    if _STOP_AUTHOR.search(s):
        return False
    if re.match(
        r"^(PhD|MSc|MD|BSc|MBChB|MBBS|Dr|Prof|Mr|Mrs|Ms|"
        r"FRCOG|FRCP|FACS|FRCPath|FWACP)\d*\.?$",
        s, re.IGNORECASE,
    ):
        return False
    return True


def _parse_author_string(author_str: str) -> list[str]:
    if not author_str:
        return []
    if ";" in author_str:
        parts = author_str.split(";")
    elif " and " in author_str.lower():
        parts = re.split(r"\s+and\s+", author_str, flags=re.IGNORECASE)
    else:
        parts = author_str.split(",")
        caps = [p.strip() for p in parts if p.strip()]
        if not all(re.match(r"^[A-Z]", p) for p in caps):
            parts = [author_str]

    result: list[str] = []
    seen:   set[str]  = set()
    for p in parts:
        c = _clean_author(p)
        if c and len(c) > 3 and c.lower() not in seen:
            seen.add(c.lower())
            result.append(c)
    return result[:10]


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

    title_prefix = title[:20].lower() if title else ""
    title_bottom = 0.0
    for y, x, size, text in all_spans:
        if title_prefix and title_prefix in text.lower():
            title_bottom = y + 20
            break
    if title_bottom == 0.0:
        page_h       = max(sp[0] for sp in all_spans) or 842
        title_bottom = page_h * 0.18

    candidates: list[str] = []
    no_author_streak = 0

    for y, x, size, text in all_spans:
        if y < title_bottom:
            continue
        if y > title_bottom + 320:
            break
        if _STOP_AUTHOR.search(text):
            break
        if size >= max_size * 0.87:
            continue

        parts = re.split(r"[,;]|(?<=[a-z])\s+and\s+(?=[A-Z])", text, flags=re.IGNORECASE)
        found_here = 0
        for part in parts:
            cleaned = _clean_author(part)
            if _is_author_name(cleaned):
                candidates.append(cleaned)
                found_here += 1
                if len(candidates) >= 12:
                    break

        if found_here == 0:
            no_author_streak += 1
            if no_author_streak >= 3 and candidates:
                break
        else:
            no_author_streak = 0

        if len(candidates) >= 12:
            break

    return _dedupe(candidates)[:10]


def _authors_from_text(first_page: str, title: str) -> list[str]:
    lines       = [l.strip() for l in first_page.split("\n") if l.strip()]
    candidates  : list[str] = []
    title_found = False
    title_low   = title[:30].lower() if title else ""
    no_streak   = 0

    for line in lines:
        if not title_found:
            if title_low and title_low in line.lower():
                title_found = True
            continue
        if _STOP_AUTHOR.search(line):
            break
        parts = re.split(r"[,;]|(?<=\w)\s+and\s+(?=[A-Z])", line, flags=re.IGNORECASE)
        found = 0
        for part in parts:
            cleaned = _clean_author(part)
            if _is_author_name(cleaned):
                candidates.append(cleaned)
                found += 1
                if len(candidates) >= 12:
                    break
        if found == 0:
            no_streak += 1
            if no_streak >= 3 and candidates:
                break
        else:
            no_streak = 0
        if len(candidates) >= 12:
            break

    return _dedupe(candidates)[:10]


# ── Abstract ──────────────────────────────────────────────────────────────────

def _extract_abstract(first_page: str) -> str:
    m = re.search(
        r"\b(?:Abstract|Summary|Overview|Synopsis)\b\s*[:—]?\s*\n?"
        r"([\s\S]{80,2500}?)"
        r"(?=\n\s*\n\s*(?:Keywords?|Key\s+words?|Index\s+terms?|"
        r"Introduction|Background|1\.|I\.|$))",
        first_page, re.IGNORECASE,
    )
    if m:
        return _clean(m.group(1))[:2000]

    paragraphs = [
        p.strip() for p in re.split(r"\n\s*\n", first_page)
        if len(p.strip()) > 120
    ]
    for para in paragraphs[1:5]:
        if not _STOP_AUTHOR.search(para[:100]) and len(para) > 120:
            return _clean(para[:2000])

    return ""


# ── DOI ───────────────────────────────────────────────────────────────────────

def _extract_doi(text: str) -> str:
    m = re.search(r"https?://(?:dx\.)?doi\.org/(10\.\d{4,9}/[^\s\"'<>\]\)]+)", text)
    if m:
        return m.group(1).rstrip(".,;)]")
    m = re.search(r"\bdoi\s*:?\s*(10\.\d{4,9}/[^\s\"'<>\]\)]+)", text, re.IGNORECASE)
    if m:
        return m.group(1).rstrip(".,;)]")
    m = re.search(r"\b(10\.\d{4,9}/[^\s\"'<>\]\)]{3,})", text)
    return m.group(1).rstrip(".,;)]") if m else ""


# ── ISSN ──────────────────────────────────────────────────────────────────────

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


# ── Publisher ─────────────────────────────────────────────────────────────────

def _extract_publisher(text: str) -> str:
    chunk = text[:4000]
    m = re.search(
        r"(?:Published\s+by|Publisher\s*:)\s*([A-Z][^\n]{3,80})",
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
    chunk = text[:5000]
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
        r"(Nigerian [A-Z][^\n]{5,60})",
    ]
    for pat in patterns:
        m = re.search(pat, chunk, re.IGNORECASE)
        if m:
            j = m.group(1).strip()
            j = re.split(r"\s+\d{4}\b|\s+[Vv]ol|\s+\d+\s*[\(,]", j)[0]
            j = j.strip().rstrip(".,;:")
            if len(j) >= 8:
                return j[:100]

    if meta_title and re.search(
        r"\b(Journal|Review|Annals|Bulletin|Transactions|Letters)\b",
        meta_title, re.IGNORECASE,
    ):
        return meta_title[:100]

    return ""


# ── Volume / Issue / Year ─────────────────────────────────────────────────────

def _extract_volume(text: str) -> str:
    m = re.search(r"\bVol(?:ume)?\.?\s*(\d+)", text[:4000], re.IGNORECASE)
    return m.group(1) if m else ""


def _extract_issue(text: str) -> str:
    m = re.search(r"\bVol(?:ume)?\.?\s*\d+\s*[\(,]\s*(\d+)\s*[\),]", text[:4000], re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"\b(?:Issue|No\.?|Number)\s*(\d+)", text[:4000], re.IGNORECASE)
    return m.group(1) if m else ""


def _extract_year(text: str, created: str) -> str:
    m = re.search(
        r"(?:published|accepted|received|online|copyright)\D{0,20}((?:19|20)\d{2})\b",
        text[:4000], re.IGNORECASE,
    )
    if m:
        return m.group(1)
    m = re.search(r"\b((?:19|20)\d{2})\b", text[:2000])
    if m:
        yr = int(m.group(1))
        if 1900 <= yr <= 2030:
            return str(yr)
    if created:
        d = re.match(r"D:(\d{4})", created)
        if d:
            return d.group(1)
        d = re.search(r"\b((?:19|20)\d{2})\b", created)
        if d:
            return d.group(1)
    return ""


# ── Keywords ─────────────────────────────────────────────────────────────────

def _extract_keywords(text: str, meta_kw: str = "") -> list[str]:
    m = re.search(
        r"(?:Keywords?|Key\s+words?|Index\s+[Tt]erms?|[Kk]ey[Pp]hrases?)"
        r"\s*[:—]\s*([^\n]{10,600})",
        text[:10000], re.IGNORECASE,
    )
    if m:
        kws = [k.strip().strip("•·-–—") for k in re.split(r"[;,•·]", m.group(1))]
        kws = [k for k in kws if 2 < len(k) < 80]
        if kws:
            return kws[:15]
    if meta_kw:
        kws = [k.strip() for k in re.split(r"[;,]", meta_kw) if k.strip()]
        return [k for k in kws if 2 < len(k) < 80][:15]
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
        r"U\.S\.", r"U\.K\.", r"U\.N\.",
    ]
    protected = text
    ph: dict[str, str] = {}
    for i, pat in enumerate(abbrevs):
        p = f"__A{i}__"
        protected = re.sub(pat, lambda m, pl=p: m.group().replace(".", pl), protected)
        ph[p] = "."

    protected = re.sub(r"(\d)\.(\d)", r"\1__D__\2", protected)
    protected = re.sub(r"\b([A-Z])\.\s+([A-Z])", r"\1__I__\2", protected)

    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z\[\(\"'\u2018\u201c])", protected)

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
    """Extract ISBN-13 or ISBN-10."""
    m = re.search(
        r"\bISBN[:\s-]*"
        r"((?:97[89][-\s]?)?\d{1,5}[-\s]?\d{1,7}[-\s]?\d{1,7}[-\s]?[\dXx])\b",
        text[:4000], re.IGNORECASE,
    )
    return m.group(1).replace(" ", "").replace("-", "") if m else ""


# ── Pages ─────────────────────────────────────────────────────────────────────

def _extract_pages(text: str) -> str:
    """
    Extract page range from common patterns:
      e398-e404, 7-14, 101–120, pp. 45-67, pages 12-20
    """
    # Labelled: pp. 45-67 or pages 12-20
    m = re.search(r"\bpp?\.?\s*(\d{1,5}[eE]?\d*\s*[-–]\s*\d{1,5}[eE]?\d*)\b", text[:3000], re.IGNORECASE)
    if m:
        return m.group(1).replace(" ", "")
    # Journal format: Vol 23(2), e398-e404
    m = re.search(r"\bVol[^\n]{1,30},\s*([eE]?\d{1,5}[-–][eE]?\d{1,5})\b", text[:3000], re.IGNORECASE)
    if m:
        return m.group(1)
    # Colon-separated: 14:7-14
    m = re.search(r"\b\d+:\s*([eE]?\d{1,5}[-–][eE]?\d{1,5})\b", text[:3000])
    if m:
        return m.group(1)
    return ""


# ── Article type ──────────────────────────────────────────────────────────────

def _extract_article_type(text: str) -> str:
    """
    Detect article type from common labels near the top of the paper.
    """
    patterns = [
        r"\b(Systematic\s+Review(?:\s+and\s+Meta[-\s]?Analysis)?)\b",
        r"\b(Meta[-\s]?Analysis)\b",
        r"\b(Randomized\s+Controlled\s+Trial|RCT)\b",
        r"\b(Clinical\s+Trial)\b",
        r"\b(Case\s+Report)\b",
        r"\b(Case\s+Series)\b",
        r"\b(Review\s+Article|Review\s+Paper|Literature\s+Review)\b",
        r"\b(Original\s+(?:Research|Article|Paper))\b",
        r"\b(Research\s+Article|Research\s+Paper)\b",
        r"\b(Short\s+(?:Communication|Report|Note))\b",
        r"\b(Letter\s+to\s+the\s+Editor|Correspondence)\b",
        r"\b(Conference\s+Paper|Proceedings)\b",
        r"\b(Technical\s+(?:Note|Report))\b",
        r"\b(Thesis|Dissertation)\b",
    ]
    chunk = text[:3000]
    for pat in patterns:
        m = re.search(pat, chunk, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return "Research Article"


# ── Affiliations ──────────────────────────────────────────────────────────────

def _extract_affiliations(first_page: str) -> list[str]:
    """
    Extract institutional affiliations from the first page.
    Looks for lines containing university/department/institute keywords.
    """
    affil_markers = re.compile(
        r"\b(university|universiti|college|institute|institution|"
        r"faculty|department|school\s+of|division|laboratory|lab\b|"
        r"hospital|clinic|centre|center|foundation|academy)\b",
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
            # Clean superscript numbers/symbols at start
            clean = re.sub(r"^[\d,*†‡§\s]+", "", line).strip()
            if clean and clean.lower() not in seen and len(clean) > 8:
                seen.add(clean.lower())
                affiliations.append(clean)
                if len(affiliations) >= 8:
                    break

    return affiliations


# ── Email ─────────────────────────────────────────────────────────────────────

def _extract_email(text: str) -> str:
    """Extract corresponding author email address."""
    # Labelled: "corresponding author: name@domain.com"
    m = re.search(
        r"(?:corresponding\s+author|address\s+for\s+correspondence|"
        r"e-?mail|contact)[^\n]{0,50}([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
        text[:3000], re.IGNORECASE,
    )
    if m:
        return m.group(1)
    # Any email in first page
    m = re.search(
        r"\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b",
        text[:2000],
    )
    return m.group(1) if m else ""


# ── ORCID ─────────────────────────────────────────────────────────────────────

def _extract_orcids(text: str) -> list[str]:
    """Extract ORCID identifiers."""
    matches = re.findall(
        r"\b(\d{4}-\d{4}-\d{4}-\d{3}[\dXx])\b",
        text[:4000],
    )
    return list(dict.fromkeys(matches))[:10]  # dedupe, max 10


# ── Funding ───────────────────────────────────────────────────────────────────

def _extract_funding(text: str) -> str:
    """
    Extract funding/acknowledgement statement.
    Looks for 'Funding:', 'Grant', 'supported by' patterns.
    """
    patterns = [
        r"(?:Funding|Funding\s+source|Financial\s+support|Grant)[:\s]+([^\n]{10,300})",
        r"(?:supported\s+by|funded\s+by|sponsored\s+by)\s+([^\n]{10,200})",
        r"(?:This\s+(?:study|work|research)\s+was\s+(?:supported|funded|sponsored)\s+by)\s+([^\n]{10,200})",
        r"(?:Acknowledgements?|Acknowledgments?)[:\s]+([^\n]{10,400})",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()[:300]
    return ""


# ── Date by label ─────────────────────────────────────────────────────────────

def _extract_date_label(label: str, text: str) -> str:
    """
    Extract a specific date labelled in the paper.
    label can be a regex alternation e.g. "received|submitted"
    Returns cleaned date string or empty string.
    """
    m = re.search(
        rf"(?:{label})[:\s]+([A-Za-z0-9,\s/.-]{{5,40}}?)(?:\n|;|$)",
        text[:3000], re.IGNORECASE,
    )
    if not m:
        return ""
    raw = m.group(1).strip().rstrip(".,;")
    # Try to normalise to readable date
    raw_clean = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", raw)
    for fmt in ("%d %B %Y", "%B %d %Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%B %Y", "%d %b %Y"):
        try:
            from datetime import datetime as _dt
            return _dt.strptime(raw_clean.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw[:30]


# ── Editor ────────────────────────────────────────────────────────────────────

def _extract_editor(text: str) -> str:
    """
    Extract editor name from common patterns:
      "Edited by", "Editor:", "Guest Editor:", "Editor-in-Chief:"
    """
    m = re.search(
        r"(?:Edited\s+by|Guest\s+Editor|Editor[-\s]?in[-\s]?Chief|"
        r"Editor|Handling\s+Editor)[:\s]+([A-Z][^\n]{3,80})",
        text[:4000], re.IGNORECASE,
    )
    if m:
        raw = m.group(1).strip().rstrip(".,;")
        # Strip institutional suffixes
        raw = re.split(r"\s*[,;]\s*(?:PhD|MD|Dr|Prof|University|Institute)", raw, flags=re.IGNORECASE)[0]
        return raw.strip()[:80]
    return ""


# ── Singleton ─────────────────────────────────────────────────────────────────
extraction_service = ExtractionService()
