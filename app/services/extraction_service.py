"""
extraction_service.py — Universal PDF extraction for any research paper/journal.

Strategy (in priority order for each field):
  Title   : 1) Largest font on page 1 (font-dict mode)
             2) Reject garbage metadata (journal IDs, codes, short strings)
             3) Text heuristic (first long line on page 1)

  Authors : 1) PyMuPDF span-level scan below title on page 1
             2) PDF metadata author field (often empty/wrong)
             3) Text heuristic (proper-name lines after title)

  Abstract: 1) Detected ABSTRACT section content
             2) First paragraph-sized block after title/authors on page 1

  DOI     : regex 10.XXXX/... anywhere in first 3 pages
  ISSN    : regex XXXX-XXXX (labelled or bare) in first 3 pages
  Volume/Issue: regex Vol/No patterns
  Publisher: known publisher name patterns + "Published by"
  Keywords : "Keywords:" labelled block in first 3 pages
  Journal  : journal name patterns in first 3 pages

Works across:
  - Elsevier, Springer, Wiley, Taylor & Francis, Nature, BMJ, Sage,
    Oxford/Cambridge UP, Wolters Kluwer, PLOS, BioMed Central,
    arXiv preprints, IEEE, ACM, APA, ACS, APA, RSC, Frontiers,
    MDPI, Hindawi, African/Nigerian/Asian journals (like Adeagbo_B2.pdf)
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

# ── Constants ─────────────────────────────────────────────────────────────────

_LIGATURES: dict[str, str] = {
    "\ufb01": "fi",  "\ufb02": "fl",  "\ufb00": "ff",
    "\ufb03": "ffi", "\ufb04": "ffl", "\u2019": "'",
    "\u2018": "'",   "\u201c": '"',   "\u201d": '"',
    "\u2013": "-",   "\u2014": "--",  "\u00a0": " ",
    "\u2022": "-",   "\u00b7": ".",   "\u00ad": "",   # soft hyphen
}

# Patterns that indicate a line is NOT a title
_GARBAGE_TITLE = re.compile(
    r"""
    ^\d+$                           # pure number
    | ^https?://                    # URL
    | ^[A-Z]{2,8}[-_]\d            # journal ID like AJT-201427
    | ^\d{4}[-/]\d{2}              # date like 2016-02
    | \.\.\d                        # page range like 398..404
    | ^(vol|no|pp|issue)\b         # bibliographic code
    | ^(doi|issn|isbn)\b           # metadata code
    | copyright                     # copyright notice
    | all rights reserved
    | unauthorized reproduction
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Markers that signal we've gone past the author block
_STOP_AUTHOR = re.compile(
    r"""
    \b(university|universiti|college|institute|institution|faculty|
    department|dept\.|school\s+of|division\s+of|
    obafemi|awolowo|lagos|nigeria|ghana|kenya|south\s+africa|
    greenville|carolina|london|oxford|cambridge|new\s+york|
    abstract|introduction|background|keywords|received|accepted|
    published|copyright|email|@|\bhttp|\bwww\b)
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Known major publishers for extraction
_KNOWN_PUBLISHERS = [
    "Wolters Kluwer", "Elsevier", "Springer", "Wiley", "Taylor & Francis",
    "Taylor and Francis", "Nature Publishing", "BMJ Publishing", "Sage Publications",
    "Oxford University Press", "Cambridge University Press", "PLOS", "BioMed Central",
    "Frontiers Media", "MDPI", "Hindawi", "IEEE", "ACM", "American Chemical Society",
    "Royal Society of Chemistry", "American Psychological Association", "Karger",
    "Lippincott", "Thieme", "Dove Medical", "Informa Healthcare",
]


class ExtractionService:
    """Full PDF pipeline: extraction → section detection → chunking."""

    def __init__(self) -> None:
        self._section_patterns = self._compile_section_patterns()
        self._body_font_size   = 10.0
        logger.info("ExtractionService initialised")

    # ── Entry point ───────────────────────────────────────────────────────────

    def process(self, doc: ProcessedDocument) -> ProcessedDocument:
        slog = ServiceLogger("extraction_service", doc_id=doc.doc_id)
        slog.info("Starting extraction for '%s'", doc.filename)
        try:
            doc.status = DocumentStatus.EXTRACTING

            pages_text, metadata = self._extract_from_pdf(doc.file_path, slog)
            doc.full_text = "\n\n".join(pages_text)
            doc.metadata  = metadata
            slog.info("Extracted %d pages, %d words", metadata.page_count, metadata.word_count)

            doc.sections = self._detect_sections(doc.full_text, pages_text, slog)
            slog.info("Sections: %s", [s.section_type.value for s in doc.sections])

            doc.chunks      = self._chunk_document(doc, slog)
            doc.chunk_count = len(doc.chunks)
            slog.info("Generated %d chunks", doc.chunk_count)

            # Pull abstract into metadata if not already filled
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

        pages_text : list[str]  = []
        font_sizes : list[float]= []
        page0_blocks: list[dict]= []
        total_words = 0

        with fitz.open(str(pdf_path)) as pdf:
            page_count = len(pdf)
            raw_meta   = pdf.metadata or {}
            meta_title  = _clean(raw_meta.get("title")        or "")
            meta_author = _clean(raw_meta.get("author")       or "")
            created     = (raw_meta.get("creationDate")       or "").strip()
            subject     = _clean(raw_meta.get("subject")      or "")
            keywords_m  = _clean(raw_meta.get("keywords")     or "")

            # Collect font/block data from first 3 pages
            for pn in range(min(3, page_count)):
                blocks = pdf[pn].get_text("dict")["blocks"]
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

            # Extract plain text for all pages
            for pn in range(page_count):
                text = pdf[pn].get_text("text")
                text = self._clean_page_text(text)
                pages_text.append(text)
                total_words += len(text.split())

        self._body_font_size = _modal(font_sizes) if font_sizes else 10.0
        first3 = "\n".join(pages_text[:3])

        # ── Title ─────────────────────────────────────────────────────────────
        title = self._resolve_title(meta_title, page0_blocks, pages_text)

        # ── Authors ───────────────────────────────────────────────────────────
        authors = self._resolve_authors(meta_author, page0_blocks, pages_text, title)

        # ── Bibliographic fields ───────────────────────────────────────────────
        doi       = _extract_doi(first3)
        issn      = _extract_issn(first3)
        publisher = _extract_publisher(first3)
        journal   = _extract_journal(first3)
        volume    = _extract_pattern(r"\bVol(?:ume)?\.?\s*(\d+)", first3)
        issue     = _extract_pattern(
            r"\bIssue\.?\s*(\d+)"
            r"|\bNo\.?\s*(\d+)"
            r"|\(\s*(\d+)\s*\)",
            first3
        )
        keywords  = _extract_keywords_from_text(first3)

        # Fall back to PDF metadata keywords if text had none
        if not keywords and keywords_m:
            keywords = [k.strip() for k in re.split(r"[;,]", keywords_m) if k.strip()]

        # Abstract: try ABSTRACT section first, then first long paragraph on page 1
        abstract = _extract_abstract_from_text(pages_text[0] if pages_text else "", title)

        metadata = DocumentMetadata(
            title           = title,
            authors         = authors,
            abstract        = abstract,
            keywords        = keywords,
            doi             = doi,
            issn            = issn,
            publisher       = publisher,
            journal         = journal,
            volume          = volume,
            issue           = issue,
            page_count      = page_count,
            word_count      = total_words,
            created_at      = created,
            file_size_bytes = pdf_path.stat().st_size,
        )

        slog.info(
            "Meta — title='%.55s' authors=%s doi=%s issn=%s",
            title, [a[:20] for a in authors[:3]], doi, issn,
        )
        return pages_text, metadata

    # ── Title resolution ──────────────────────────────────────────────────────

    def _resolve_title(
        self,
        meta_title   : str,
        page0_blocks : list[dict],
        pages_text   : list[str],
    ) -> str:
        # Step 1: check if PDF metadata title is usable
        if meta_title and len(meta_title) >= 20 and not _GARBAGE_TITLE.search(meta_title):
            return meta_title

        # Step 2: largest-font text on page 1
        font_title = _title_by_font(page0_blocks)
        if font_title and len(font_title) >= 15:
            return font_title

        # Step 3: text heuristic — longest reasonable line in top third of page 1
        return _title_from_text(pages_text[0] if pages_text else "")

    # ── Author resolution ─────────────────────────────────────────────────────

    def _resolve_authors(
        self,
        meta_author  : str,
        page0_blocks : list[dict],
        pages_text   : list[str],
        title        : str,
    ) -> list[str]:
        # Step 1: font-position scan on page 1 (most reliable)
        font_authors = _authors_by_font(page0_blocks, title, self._body_font_size)
        if font_authors:
            return font_authors

        # Step 2: PDF metadata author field
        if meta_author:
            parsed = _parse_author_string(meta_author)
            if parsed:
                return parsed

        # Step 3: text heuristic
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
            st = self._classify_heading(s)
            if not st:
                continue
            # Suppress same section type within 5 lines (handles duplicate headings)
            if i - seen.get(st, -99) < 5:
                continue
            seen[st] = i
            hits.append((i, st, s))

        slog.debug("Found %d headings: %s", len(hits), [h[1].value for h in hits])

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
            slog.warning("No sections — treating full document as one section")
            sections.append(DocumentSection(
                section_type = SectionType.OTHER,
                title        = "Full Document",
                content      = full_text,
            ))

        return sections

    # ── Heading classifier ────────────────────────────────────────────────────

    def _classify_heading(self, line: str) -> SectionType | None:
        norm = line.lower().strip().rstrip(".:")
        # Strip leading section numbers: "1.", "2.1", "II.", "A."
        norm = re.sub(r"^(\d+(\.\d+)*\.?|[IVX]+\.|[A-Z]\.)\s+", "", norm)
        for st, pattern in self._section_patterns.items():
            if pattern.search(norm):
                return st
        return None

    def _compile_section_patterns(self) -> dict[SectionType, re.Pattern]:
        # Extended keyword sets for broad journal compatibility
        extended: dict[str, list[str]] = {
            "abstract": [
                "abstract", "summary", "overview", "synopsis",
                "précis", "highlights",
            ],
            "introduction": [
                "introduction", "background", "motivation",
                "problem statement", "rationale", "context",
                "general introduction", "study rationale",
            ],
            "methods": [
                "methods", "methodology", "materials and methods",
                "experimental", "experimental section", "experimental setup",
                "patients and methods", "subjects and methods",
                "study design", "study population", "participants",
                "data collection", "data analysis", "statistical analysis",
                "drug analysis", "analytical methods", "procedure",
                "proposed method", "approach",
            ],
            "results": [
                "results", "findings", "experiments", "evaluation",
                "experimental results", "outcomes", "observations",
                "pharmacokinetic results", "clinical results",
            ],
            "discussion": [
                "discussion", "analysis", "interpretation",
                "results and discussion", "discussion and conclusion",
                "general discussion",
            ],
            "conclusion": [
                "conclusion", "conclusions", "concluding remarks",
                "summary", "summary and conclusion", "final remarks",
                "future work", "limitations", "study limitations",
                "implications",
            ],
            "references": [
                "references", "bibliography", "works cited",
                "literature cited", "citations",
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
            # Merge config keywords with extended defaults
            kws = list(dict.fromkeys(
                SECTION_KEYWORDS.get(key, []) + extended.get(key, [])
            ))
            if not kws:
                continue
            alts = "|".join(re.escape(k) for k in sorted(kws, key=len, reverse=True))
            patterns[st] = re.compile(
                rf"(?:^|\b)({alts})(?:\b|$|:|\s)",
                re.IGNORECASE,
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

        slog.debug("Chunked: %d (size=%d overlap=%d)", len(chunks), CHUNK_SIZE, CHUNK_OVERLAP)
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

        chunks: list[TextChunk] = []
        current: list[str]      = []
        cur_len: int            = 0

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
                carry   = all_words[-CHUNK_OVERLAP:] if len(all_words) > CHUNK_OVERLAP else all_words
                current = [" ".join(carry)]
                cur_len = len(carry)
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
        text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)      # hyphenated line breaks
        text = re.sub(r"[^\x20-\x7E\n]", " ", text)       # non-printable
        text = re.sub(r"\n{3,}", "\n\n", text)              # excess blank lines
        text = re.sub(r"[ \t]{2,}", " ", text)              # excess spaces
        return text.strip()


# ═════════════════════════════════════════════════════════════════════════════
# Module-level helper functions
# ═════════════════════════════════════════════════════════════════════════════

def _clean(text: str) -> str:
    return unescape(text).strip()


def _modal(sizes: list[float]) -> float:
    """Most common font size (body text baseline)."""
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


# ── Title extraction ──────────────────────────────────────────────────────────

def _title_by_font(blocks: list[dict]) -> str:
    """Largest-font text on page 1 = title."""
    spans: list[tuple[float, float, str]] = []  # (y, size, text)
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

    max_size = max(s[1] for s in spans)
    threshold = max_size * 0.92  # allow slight size variation within same title

    # Collect all spans at max size, in vertical order
    title_spans = sorted(
        [(y, t) for y, s, t in spans if s >= threshold],
        key=lambda x: x[0],
    )

    # Only take spans in the top 40% of the page height
    if title_spans:
        page_height = max(y for y, _, _ in spans) or 800
        title_spans = [(y, t) for y, t in title_spans if y <= page_height * 0.45]

    if not title_spans:
        return ""

    title = " ".join(t for _, t in title_spans).strip()

    # Reject if it looks like garbage
    if _GARBAGE_TITLE.search(title) or len(title) < 15:
        return ""

    return _clean(title[:300])


def _title_from_text(first_page: str) -> str:
    """Heuristic: first substantial line that isn't garbage."""
    for line in first_page.split("\n"):
        s = line.strip()
        if len(s) < 15 or len(s) > 300:
            continue
        if s.count(" ") < 2:
            continue
        if _GARBAGE_TITLE.search(s):
            continue
        if re.match(r"^\d", s):
            continue
        return _clean(s)
    return ""


# ── Author extraction ─────────────────────────────────────────────────────────

def _clean_author(raw: str) -> str:
    """Normalize a single author name: strip degrees, superscripts, symbols."""
    name = _clean(raw)
    # Strip trailing degree qualifiers and everything after
    name = re.sub(
        r"\s*,?\s*\b(MSc|M\.Sc|PhD|Ph\.D|MD|M\.D|BSc|B\.Sc|MBChB|MPH|"
        r"DrPH|PharmD|MBBS|MPharm|BPharm|FRCOG|FRCP|FACS|FRCPath|"
        r"DVM|DDS|DMD|MDS|MA|MBA|MEd|MPA|MFA|EdD|PsyD|ScD|DSc)\b.*",
        "", name, flags=re.IGNORECASE,
    )
    # Strip numeric superscripts: "1", "1,2", ",1,2*"
    name = re.sub(r"[,\s]*[\d,]+\*?\s*$", "", name)
    # Strip leading "and"
    name = re.sub(r"^and\s+", "", name, flags=re.IGNORECASE)
    # Strip symbols
    name = name.strip(" *†‡§,.")
    return name.strip()


def _is_author_name(s: str) -> bool:
    """True if string looks like a proper author name."""
    s = s.strip()
    if len(s) < 4 or len(s) > 50:
        return False
    # Must start with capital letter
    if not re.match(r"^[A-Z]", s):
        return False
    # Must have at least one lowercase letter (not ALL CAPS like a heading)
    if s == s.upper():
        return False
    # Must not be a standalone degree or institution word
    if _STOP_AUTHOR.search(s):
        return False
    if re.match(r"^(PhD|MSc|MD|BSc|MBChB|Dr|Prof|Mr|Mrs)\d*\.?$", s, re.IGNORECASE):
        return False
    return True


def _parse_author_string(author_str: str) -> list[str]:
    """Parse PDF metadata author string into list of clean names."""
    if not author_str:
        return []
    # Split on semicolons, " and ", commas (careful: "Last, First" format)
    # Try semicolons first (most unambiguous)
    if ";" in author_str:
        parts = [p.strip() for p in author_str.split(";")]
    elif " and " in author_str.lower():
        parts = re.split(r"\s+and\s+", author_str, flags=re.IGNORECASE)
    else:
        parts = [p.strip() for p in author_str.split(",")]

    result = []
    for p in parts:
        cleaned = _clean_author(p)
        if cleaned and len(cleaned) > 3:
            result.append(cleaned)
    return result[:10]


def _authors_by_font(
    blocks      : list[dict],
    title       : str,
    body_size   : float,
) -> list[str]:
    """
    Font-position based author extraction.
    Scans spans vertically below the title region on page 1.
    Authors are medium-sized text (between body and title size),
    appearing within ~250pt below the title area.
    """
    if not blocks:
        return []

    # Flatten all spans with position info
    all_spans: list[tuple[float, float, float, str]] = []  # (y, x, size, text)
    for block in blocks:
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            ly = line.get("bbox", [0, 0, 0, 0])[1]
            for span in line.get("spans", []):
                t = _clean(span.get("text", "").strip())
                s = span.get("size", 0.0)
                x = span.get("bbox", [0])[0] if span.get("bbox") else 0
                if t and s > 0:
                    all_spans.append((ly, x, s, t))

    if not all_spans:
        return []

    all_spans.sort(key=lambda sp: (sp[0], sp[1]))
    max_size = max(sp[2] for sp in all_spans)

    # Find bottom y of title region
    title_low = title[:25].lower() if title else ""
    title_bottom = 0.0
    for y, x, size, text in all_spans:
        if title_low and title_low in text.lower():
            title_bottom = y + 30  # a little below the title line
            break
    # If title not found by text match, use top 25% of page
    if title_bottom == 0.0:
        page_h = max(sp[0] for sp in all_spans) or 800
        title_bottom = page_h * 0.15

    candidates: list[str] = []
    for y, x, size, text in all_spans:
        if y < title_bottom:
            continue
        if y > title_bottom + 280:
            break

        # Hard stop at institutional/abstract markers
        if _STOP_AUTHOR.search(text):
            break

        # Skip title-sized spans (still part of title continuation)
        if size >= max_size * 0.88:
            continue

        # Split on common author separators
        parts = [p.strip() for p in re.split(r"[,;]|(?<=[a-z])\s+and\s+", text, flags=re.IGNORECASE)]
        for part in parts:
            cleaned = _clean_author(part)
            if _is_author_name(cleaned):
                candidates.append(cleaned)
                if len(candidates) >= 10:
                    break
        if len(candidates) >= 10:
            break

    # Deduplicate while preserving order
    seen: set[str] = set()
    result = []
    for c in candidates:
        if c.lower() not in seen:
            seen.add(c.lower())
            result.append(c)
    return result[:10]


def _authors_from_text(first_page: str, title: str) -> list[str]:
    """
    Text-based author extraction: scan lines just after title on page 1.
    Handles multi-author lines with degree suffixes and superscripts.
    """
    lines = [l.strip() for l in first_page.split("\n") if l.strip()]
    candidates: list[str] = []
    title_found = False
    title_low   = title[:30].lower() if title else ""

    for line in lines:
        if not title_found:
            if title_low and title_low in line.lower():
                title_found = True
            continue

        if _STOP_AUTHOR.search(line):
            break

        # Split aggressively on separators
        parts = re.split(r"[,;]|(?<=\w)\s+and\s+(?=[A-Z])", line, flags=re.IGNORECASE)
        found_in_line = 0
        for part in parts:
            cleaned = _clean_author(part)
            if _is_author_name(cleaned):
                candidates.append(cleaned)
                found_in_line += 1
                if len(candidates) >= 10:
                    break
        if len(candidates) >= 10:
            break
        # Stop scanning after 4 consecutive lines with no authors
        if found_in_line == 0 and len(candidates) > 0:
            break

    # Deduplicate
    seen: set[str] = set()
    result = []
    for c in candidates:
        if c.lower() not in seen:
            seen.add(c.lower())
            result.append(c)
    return result[:10]


# ── Abstract extraction ───────────────────────────────────────────────────────

def _extract_abstract_from_text(first_page: str, title: str) -> str:
    """
    Try to extract abstract text from page 1.
    Looks for 'Abstract' heading, or falls back to first substantial paragraph
    that appears after the author block.
    """
    text = first_page

    # Pattern 1: explicit Abstract label
    m = re.search(
        r"\bAbstract\b[:\s]*\n?([\s\S]{80,2000?}?)(?=\n\s*\n|\bKeywords?\b|\bIntroduction\b|\bBackground\b)",
        text, re.IGNORECASE,
    )
    if m:
        return _clean(m.group(1))[:2000]

    # Pattern 2: first long paragraph after a blank line (likely the abstract body)
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if len(p.strip()) > 100]
    # Skip the first 1-2 (likely title + author block)
    for para in paragraphs[1:4]:
        if len(para) > 100 and not _STOP_AUTHOR.search(para[:80]):
            return _clean(para[:2000])

    return ""


# ── Bibliographic field extractors ────────────────────────────────────────────

def _extract_doi(text: str) -> str:
    """Extract DOI — standard 10.XXXX/... format."""
    m = re.search(r"\b(10\.\d{4,9}/[^\s\"'<>\]\)]+)", text, re.IGNORECASE)
    return m.group(1).rstrip(".,;)]") if m else ""


def _extract_issn(text: str) -> str:
    """Extract ISSN — XXXX-XXXX format, labelled or bare."""
    # Labelled first (most reliable)
    m = re.search(r"\bE?-?ISSN[:\s]*(\d{4}-\d{3}[\dXx])\b", text, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"\bISSN[:\s]*(\d{4}-\d{3}[\dXx])\b", text, re.IGNORECASE)
    if m:
        return m.group(1)
    # Bare ISSN after journal name or copyright line
    m = re.search(r"(?:journal|issn|copyright)[^\n]*\b(\d{4}-\d{3}[\dXx])\b", text, re.IGNORECASE)
    if m:
        return m.group(1)
    # Last resort: bare pattern (higher false-positive risk, so only in first 1000 chars)
    m = re.search(r"\b(\d{4}-\d{3}[\dXx])\b", text[:1000])
    return m.group(1) if m else ""


def _extract_publisher(text: str) -> str:
    """Extract publisher name from known list or explicit label."""
    # Explicit label
    m = re.search(r"(?:Published\s+by|Publisher\s*:)\s*([A-Z][^\n]{3,70})", text[:4000])
    if m:
        pub = re.split(r"\s*[,.]?\s*(?:Inc\.|Ltd\.?|All rights|Copyright|\d{4})", m.group(1))[0]
        return pub.strip()[:80]

    # Known publisher names
    for pub in _KNOWN_PUBLISHERS:
        if pub.lower() in text[:4000].lower():
            return pub

    return ""


def _extract_journal(text: str) -> str:
    """Extract journal name from common patterns."""
    patterns = [
        # "Published in: Journal Name"
        r"(?:published\s+in|journal\s*:)\s*([A-Z][^\n]{5,80})",
        # "Journal of X" / "International Journal of X"
        r"((?:International\s+)?(?:Journal|Review|Annals|Archives|Bulletin|"
        r"Proceedings|Transactions|Letters)\s+(?:of|for|on)\s+[A-Z][^\n]{3,60})",
        # Specific patterns like "American Journal of Therapeutics"
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,5}\s+Journal[^\n]{0,30})",
        # "Asian Journal of ..."
        r"(Asian Journal of [^\n]{5,60})",
        r"(American Journal of [^\n]{5,60})",
        r"(British Journal of [^\n]{5,60})",
        r"(European Journal of [^\n]{5,60})",
    ]
    for pat in patterns:
        m = re.search(pat, text[:4000], re.IGNORECASE)
        if m:
            journal = m.group(1).strip()
            # Trim at year or issue number
            journal = re.split(r"\s+\d{4}\b|\s+Vol|\s+\d+\s*\(", journal)[0]
            return journal.strip()[:100]
    return ""


def _extract_keywords_from_text(text: str) -> list[str]:
    """Extract keywords from labelled 'Keywords:' block."""
    m = re.search(
        r"(?:Keywords?|Key\s+words?|Index\s+terms?)\s*[:—]\s*([^\n]{10,500})",
        text[:8000], re.IGNORECASE,
    )
    if not m:
        return []
    raw  = m.group(1).strip()
    # Split on semicolons, commas, or bullets
    kws  = [k.strip().strip("•·-") for k in re.split(r"[;,•·]", raw) if k.strip()]
    # Filter out empty strings and overly long entries
    kws  = [k for k in kws if 2 < len(k) < 60]
    return kws[:15]


def _extract_pattern(pattern: str, text: str) -> str:
    """Generic pattern extraction — returns first non-None group."""
    m = re.search(pattern, text[:4000], re.IGNORECASE)
    if m:
        return next((g for g in m.groups() if g), "")
    return ""


# ── Sentence splitter ─────────────────────────────────────────────────────────

def _split_sentences(text: str) -> list[str]:
    """
    Two-pass sentence splitter: protect abbreviations + decimals,
    then split on sentence-ending punctuation followed by capital.
    """
    abbrevs = [
        r"Mr\.", r"Mrs\.", r"Ms\.", r"Dr\.", r"Prof\.", r"Fig\.", r"Tab\.",
        r"Eq\.", r"Sec\.", r"Vol\.", r"No\.", r"pp\.", r"vs\.", r"approx\.",
        r"et al\.", r"i\.e\.", r"e\.g\.", r"cf\.", r"viz\.", r"resp\.",
        r"ca\.", r"op\. cit\.", r"ibid\.", r"al\.",
    ]
    protected = text
    ph: dict[str, str] = {}
    for i, pat in enumerate(abbrevs):
        p = f"__A{i}__"
        protected = re.sub(pat, lambda m, pl=p: m.group().replace(".", pl), protected)
        ph[p] = "."

    protected = re.sub(r"(\d)\.(\d)", r"\1__D__\2", protected)

    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z\[\(\"'])", protected)

    result: list[str] = []
    for part in parts:
        part = part.replace("__D__", ".")
        for p in ph:
            part = part.replace(p, ".")
        part = part.strip()
        if part and len(part) > 5:
            result.append(part)
    return result


# ── Singleton ─────────────────────────────────────────────────────────────────
extraction_service = ExtractionService()