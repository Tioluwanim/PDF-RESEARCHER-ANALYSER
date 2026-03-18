"""
extraction_service.py — Universal PDF extraction for ANY research/journal paper.

Works across ALL major publishers and formats:
  Elsevier · Springer · Wiley · Nature · BMJ · Sage · Oxford UP · Cambridge UP
  Wolters Kluwer · PLOS · BioMed Central · Frontiers · MDPI · Hindawi
  IEEE · ACM · ACS · RSC · APA · Taylor & Francis · Informa
  arXiv preprints · African/Asian/Nigerian local journals
  Two-column · Single-column · Conference proceedings · Theses

Pipeline per document:
  1. Extract raw text + font/block metadata via PyMuPDF dict mode
  2. Resolve title  — font-size → metadata validation → text heuristic
  3. Resolve authors — font-position → metadata field → text heuristic
  4. Extract biblio  — DOI · ISSN · journal · publisher · volume · issue · year
  5. Extract keywords + abstract
  6. Detect sections — extended keyword set covering clinical, CS, social science
  7. Sentence-aware sliding-window chunking with overlap
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

# ── Unicode / ligature normalisation map ─────────────────────────────────────
_LIGATURES: dict[str, str] = {
    "\ufb01": "fi",  "\ufb02": "fl",  "\ufb00": "ff",
    "\ufb03": "ffi", "\ufb04": "ffl",
    "\u2019": "'",   "\u2018": "'",
    "\u201c": '"',   "\u201d": '"',
    "\u2013": "-",   "\u2014": "--",
    "\u00a0": " ",   "\u00ad": "",      # non-breaking space, soft-hyphen
    "\u2022": "-",   "\u00b7": ".",
    "\u00d7": "x",   "\u03b1": "alpha", # common in science papers
    "\u03b2": "beta","\u03bc": "mu",
}

# ── Garbage title patterns ────────────────────────────────────────────────────
_GARBAGE_TITLE = re.compile(
    r"""
    ^\d+$                              # pure number (page number)
    |^[ivxlcdmIVXLCDM]+$             # roman numerals only
    |^https?://                        # URL
    |^[A-Z]{2,8}[-_]\d               # journal ID: AJT-201427, PLOS-2021
    |^\d{4}[-/]\d{2}                  # date: 2016-02
    |\.\.\d                            # page range: 398..404
    |^(vol|no|pp|issue|page)\b        # bibliographic code
    |^(doi|issn|isbn|pmid|pmcid)\b   # metadata code
    |(copyright|all\s+rights\s+reserved|unauthorized\s+reproduction)
    |^\s*\d+\s*$                      # whitespace-padded number
    """,
    re.IGNORECASE | re.VERBOSE,
)

# ── Stop-words for author scanning ───────────────────────────────────────────
# Signals we have passed the author block and entered affiliations/abstract
_STOP_AUTHOR = re.compile(
    r"""
    \b(
        university|universiti|universidade|università|université|
        universidad|universitaet|universität|
        college|institute|institution|faculty|department|dept\b|
        school\s+of|division\s+of|centre\s+of|center\s+of|
        laboratory|lab\b|hospital|clinic|medical\s+center|
        obafemi|awolowo|lagos|ibadan|nairobi|accra|pretoria|
        johannesburg|kumasi|kampala|dar\s+es\s+salaam|
        greenville|carolina|london|oxford|cambridge|new\s+york|
        beijing|shanghai|tokyo|seoul|delhi|mumbai|
        abstract|introduction|background|objective|purpose|
        keywords?|key\s+words?|index\s+terms?|
        received|accepted|published|revised|available\s+online|
        correspondence|corresponding\s+author|
        email|e-mail|tel\b|fax\b|@|\bhttp|\bwww\b|orcid
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# ── Degree suffixes (for cleaning author strings) ────────────────────────────
_DEGREES = re.compile(
    r"\s*,?\s*\b("
    r"MSc|M\.Sc|M\.S\.|MPhil|M\.Phil|"
    r"PhD|Ph\.D|DPhil|D\.Phil|"
    r"MD|M\.D|MBBS|MBChB|MBBCh|MBBChir|"
    r"BSc|B\.Sc|B\.S\.|BA\b|B\.A\.|"
    r"MPH|DrPH|MSPH|MHS|"
    r"PharmD|Pharm\.D|BPharm|MPharm|"
    r"FRCOG|FRCP|FRCPCH|FRCS|FACS|FRCPath|FRCPE|"
    r"FMCPath|FWACP|FMCPH|"
    r"DVM|DDS|DMD|MDS|BDS|"
    r"MA\b|MBA|MEd|MPA|MFA|EdD|PsyD|ScD|DSc|DrSc|"
    r"FCPS|MRCP|MRCOG|MCPath|"
    r"I{1,3}|IV|[A-Z]{2,6}  # roman-numeral suffixes or unrecognised letter codes"
    r")\b.*",
    re.IGNORECASE,
)

# ── Known major publishers ────────────────────────────────────────────────────
_KNOWN_PUBLISHERS = [
    "Wolters Kluwer", "Lippincott Williams",
    "Elsevier", "Cell Press", "Lancet",
    "Springer", "Springer Nature", "Nature Publishing",
    "Wiley", "Wiley-Blackwell", "John Wiley",
    "Taylor & Francis", "Taylor and Francis", "Informa Healthcare",
    "BMJ Publishing", "BMJ Group",
    "Sage Publications", "SAGE",
    "Oxford University Press", "Cambridge University Press",
    "PLOS", "Public Library of Science",
    "BioMed Central", "BMC",
    "Frontiers Media", "Frontiers in",
    "MDPI", "Multidisciplinary Digital Publishing",
    "Hindawi", "Dove Medical Press", "Dove Press",
    "American Chemical Society", "ACS Publications",
    "Royal Society of Chemistry", "RSC",
    "American Psychological Association",
    "IEEE", "ACM", "Association for Computing Machinery",
    "Thieme", "Karger", "S. Karger",
    "Mary Ann Liebert", "Future Medicine",
    "African Journals Online", "AJOL",
    "Asian Journal",
]


# ═══════════════════════════════════════════════════════════════════════════════
class ExtractionService:
    """Full PDF pipeline: extraction → section detection → sentence-aware chunking."""

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
            slog.info("Extracted %d pages, %d words", metadata.page_count, metadata.word_count)

            doc.sections = self._detect_sections(doc.full_text, pages_text, slog)
            slog.info("Sections: %s", [s.section_type.value for s in doc.sections])

            doc.chunks      = self._chunk_document(doc, slog)
            doc.chunk_count = len(doc.chunks)
            slog.info("Chunks: %d", doc.chunk_count)

            # Fill abstract from section if metadata didn't capture it
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
        total_words = 0

        with fitz.open(str(pdf_path)) as pdf:
            page_count  = len(pdf)
            raw_meta    = pdf.metadata or {}
            meta_title  = _clean(raw_meta.get("title")    or "")
            meta_author = _clean(raw_meta.get("author")   or "")
            meta_kw     = _clean(raw_meta.get("keywords") or "")
            created     = (raw_meta.get("creationDate")   or "").strip()

            # Collect rich font/block data from first 3 pages
            for pn in range(min(3, page_count)):
                blocks = pdf[pn].get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)["blocks"]
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

        # Body font = most common size (used for heading detection)
        self._body_font_size = _modal(font_sizes) if font_sizes else 10.0

        # Search space for bibliographic fields (first 3 pages)
        first3 = "\n".join(pages_text[:3])

        # ── Resolve fields ────────────────────────────────────────────────────
        title     = self._resolve_title(meta_title, page0_blocks, pages_text)
        authors   = self._resolve_authors(meta_author, page0_blocks, pages_text, title)
        doi       = _extract_doi(first3)
        issn      = _extract_issn(first3)
        publisher = _extract_publisher(first3)
        journal   = _extract_journal(first3, meta_title)
        volume    = _extract_volume(first3)
        issue     = _extract_issue(first3)
        year      = _extract_year(first3, created)
        keywords  = _extract_keywords(first3, meta_kw)
        abstract  = _extract_abstract(pages_text[0] if pages_text else "")

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
            created_at      = year or created,
            file_size_bytes = pdf_path.stat().st_size,
        )

        slog.info(
            "title='%.50s' | authors=%d | doi=%s | issn=%s | journal='%.40s'",
            title, len(authors), doi or "—", issn or "—", journal or "—",
        )
        return pages_text, metadata

    # ── Title resolution (3-stage cascade) ───────────────────────────────────

    def _resolve_title(
        self,
        meta_title   : str,
        page0_blocks : list[dict],
        pages_text   : list[str],
    ) -> str:
        # Stage 1 — PDF metadata title (only if it looks like a real title)
        if (meta_title
                and len(meta_title) >= 20
                and not _GARBAGE_TITLE.search(meta_title)
                and meta_title.count(" ") >= 2):
            return meta_title

        # Stage 2 — Largest-font span(s) on page 1
        font_title = _title_by_font(page0_blocks)
        if font_title and len(font_title) >= 15:
            return font_title

        # Stage 3 — Text heuristic: first substantial non-garbage line on page 1
        return _title_from_text(pages_text[0] if pages_text else "")

    # ── Author resolution (3-stage cascade) ──────────────────────────────────

    def _resolve_authors(
        self,
        meta_author  : str,
        page0_blocks : list[dict],
        pages_text   : list[str],
        title        : str,
    ) -> list[str]:
        # Stage 1 — Font+position scan below title on page 1
        font_authors = _authors_by_font(page0_blocks, title, self._body_font_size)
        if font_authors:
            return font_authors

        # Stage 2 — PDF metadata author field
        if meta_author:
            parsed = _parse_author_string(meta_author)
            if parsed:
                return parsed

        # Stage 3 — Text scan below title
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
            # Headings are short and not body text
            if not s or len(s) > 120:
                continue
            # Must have at least 2 chars and not be purely numeric
            if re.match(r"^\d+\.?\s*$", s):
                continue
            st = self._classify_heading(s)
            if not st:
                continue
            # Suppress same section within 5 lines (catches duplicate headings)
            if i - seen.get(st, -99) < 5:
                continue
            seen[st] = i
            hits.append((i, st, s))

        slog.debug("Headings: %s", [(h[2], h[1].value) for h in hits])

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
            slog.warning("No sections detected — full document as one chunk")
            sections.append(DocumentSection(
                section_type = SectionType.OTHER,
                title        = "Full Document",
                content      = full_text,
            ))

        return sections

    # ── Heading classifier ────────────────────────────────────────────────────

    def _classify_heading(self, line: str) -> SectionType | None:
        norm = line.lower().strip().rstrip(".:")
        # Strip leading numbering: "1.", "2.1", "II.", "A.", "1.2.3"
        norm = re.sub(r"^(\d+(\.\d+)*\.?|[IVX]+\.|[A-Z]\.)[\s\u00a0]+", "", norm)
        # Also handle bold markers sometimes left in text: "**Introduction**"
        norm = norm.strip("*_").strip()
        for st, pattern in self._section_patterns.items():
            if pattern.search(norm):
                return st
        return None

    def _compile_section_patterns(self) -> dict[SectionType, re.Pattern]:
        """
        Extended section keyword sets covering all major paper types:
        clinical, pharmacology, chemistry, CS/AI, social science,
        economics, education, engineering, environmental science.
        """
        extended: dict[str, list[str]] = {
            "abstract": [
                "abstract", "summary", "executive summary", "overview",
                "synopsis", "précis", "highlights", "graphical abstract",
                "lay summary", "plain language summary",
            ],
            "introduction": [
                "introduction", "background", "motivation", "rationale",
                "context", "problem statement", "general introduction",
                "study rationale", "scope", "overview", "preface",
                "aims and objectives", "objectives", "aim of the study",
            ],
            "methods": [
                # Generic
                "methods", "methodology", "materials and methods",
                "methods and materials",
                # Experimental
                "experimental", "experimental section", "experimental setup",
                "experimental design", "experimental procedure",
                # Clinical/Medical
                "patients and methods", "subjects and methods",
                "study design", "study population", "participants",
                "study participants", "inclusion criteria",
                "exclusion criteria", "ethical approval",
                # Data
                "data collection", "data sources", "data analysis",
                "statistical analysis", "statistical methods",
                "statistical approach",
                # Engineering/CS
                "system design", "proposed method", "proposed approach",
                "proposed framework", "model", "algorithm", "implementation",
                "architecture",
                # Lab/Chemistry
                "drug analysis", "analytical methods", "sample preparation",
                "instrumentation", "chromatographic conditions",
                "synthesis", "preparation",
                # Other
                "procedure", "approach", "protocol",
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
                "analysis and discussion",
                "discussion and conclusion", "discussion and conclusions",
                "interpretation", "analysis",
            ],
            "conclusion": [
                "conclusion", "conclusions", "concluding remarks",
                "concluding thoughts",
                "summary and conclusion", "summary and conclusions",
                "final remarks", "closing remarks",
                "summary", "overview",
                "future work", "future directions", "future research",
                "limitations", "study limitations", "limitations of the study",
                "implications", "clinical implications", "policy implications",
                "recommendations", "practical implications",
            ],
            "references": [
                "references", "bibliography", "works cited",
                "literature cited", "citations", "sources",
                "reference list",
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
            # Merge config keywords with extended (config takes priority)
            kws = list(dict.fromkeys(
                SECTION_KEYWORDS.get(key, []) + extended.get(key, [])
            ))
            if not kws:
                continue
            # Sort longest first so multi-word phrases match before single words
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

        slog.debug("Chunked: %d (CHUNK_SIZE=%d OVERLAP=%d)", len(chunks), CHUNK_SIZE, CHUNK_OVERLAP)
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
                # Carry-over overlap words
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
        text = unescape(text)                                   # HTML entities
        text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)           # hyphenated line breaks
        text = re.sub(r"[^\x20-\x7E\n]", " ", text)            # non-printable
        text = re.sub(r"\n{3,}", "\n\n", text)                  # excess blank lines
        text = re.sub(r"[ \t]{2,}", " ", text)                  # excess spaces
        return text.strip()


# ═══════════════════════════════════════════════════════════════════════════════
# Module-level helpers — pure functions, fully tested independently
# ═══════════════════════════════════════════════════════════════════════════════

def _clean(text: str) -> str:
    return unescape(text).strip()


def _modal(sizes: list[float]) -> float:
    """Most common font size = body text baseline."""
    buckets: dict[float, int] = {}
    for s in sizes:
        k = round(s * 2) / 2       # quantise to 0.5pt
        buckets[k] = buckets.get(k, 0) + 1
    return max(buckets, key=lambda k: buckets[k]) if buckets else 10.0


def _estimate_page(char_offset: int, pages_text: list[str]) -> int:
    cum = 0
    for i, p in enumerate(pages_text):
        cum += len(p)
        if char_offset <= cum:
            return i
    return max(0, len(pages_text) - 1)


# ── Title ─────────────────────────────────────────────────────────────────────

def _title_by_font(blocks: list[dict]) -> str:
    """
    Extract title from page 1 by collecting all spans at/near the maximum
    font size, sorted by vertical position (top → bottom).
    Research paper titles are almost always the biggest text on page 1.
    """
    spans: list[tuple[float, float, str]] = []   # (y, size, text)
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
    threshold = max_size * 0.91   # ±9% to catch title lines in same font

    # Keep spans at max size, in vertical (top-to-bottom) order
    title_spans = sorted(
        [(y, t) for y, s, t in spans if s >= threshold],
        key=lambda x: x[0],
    )

    # Only within the top 45% of page height (titles don't appear at the bottom)
    page_h = max(y for y, _, _ in spans) or 800
    title_spans = [(y, t) for y, t in title_spans if y <= page_h * 0.45]

    if not title_spans:
        return ""

    title = " ".join(t for _, t in title_spans).strip()

    if _GARBAGE_TITLE.search(title) or len(title) < 15:
        return ""
    return _clean(title[:300])


def _title_from_text(first_page: str) -> str:
    """
    Heuristic fallback: first substantial line (≥15 chars, ≥3 words,
    not garbage) on page 1.
    """
    for line in first_page.split("\n"):
        s = line.strip()
        if len(s) < 15 or len(s) > 350:
            continue
        if s.count(" ") < 2:
            continue
        if _GARBAGE_TITLE.search(s):
            continue
        if re.match(r"^\d", s):         # starts with digit (page number, date)
            continue
        return _clean(s)
    return ""


# ── Authors ───────────────────────────────────────────────────────────────────

def _clean_author(raw: str) -> str:
    """
    Normalise one author token:
      - Strip academic degree suffixes and everything after them
      - Strip numeric/symbol superscripts (affiliation markers)
      - Strip leading 'and', trailing punctuation
    """
    name = _clean(raw)
    # Strip degrees and everything after
    name = _DEGREES.sub("", name)
    # Strip numeric affiliation superscripts: ",1,2*" at end
    name = re.sub(r"[,\s]*[\d,]+[*†‡§]*\s*$", "", name)
    # Strip leading "and "
    name = re.sub(r"^and\s+", "", name, flags=re.IGNORECASE)
    # Strip surrounding symbols
    name = name.strip(" *†‡§,.;:")
    return name.strip()


def _is_author_name(s: str) -> bool:
    """
    Validate that a token is a plausible human author name.
    Returns False for headings, institutions, degrees, URLs, etc.
    """
    s = s.strip()
    if len(s) < 4 or len(s) > 60:
        return False
    if not re.match(r"^[A-Z]", s):          # must start with capital
        return False
    if s == s.upper() and len(s) > 6:       # ALL CAPS = heading/acronym
        return False
    if not re.search(r"[a-z]", s):          # must have lowercase
        return False
    if _STOP_AUTHOR.search(s):
        return False
    # Reject lone degree tokens
    if re.match(
        r"^(PhD|MSc|MD|BSc|MBChB|MBBS|Dr|Prof|Mr|Mrs|Ms|"
        r"FRCOG|FRCP|FACS|FRCPath|FWACP)\d*\.?$",
        s, re.IGNORECASE,
    ):
        return False
    # Must contain at least one space OR an initial (e.g. "J. Smith", "Adegbola")
    # Single-token surnames without initials are valid in some cultures
    return True


def _parse_author_string(author_str: str) -> list[str]:
    """
    Parse the PDF metadata 'author' field into a clean list.
    Handles: semicolons, 'and', comma-separated, 'Last, First' format.
    """
    if not author_str:
        return []

    # Prefer semicolons (unambiguous)
    if ";" in author_str:
        parts = author_str.split(";")
    elif " and " in author_str.lower():
        parts = re.split(r"\s+and\s+", author_str, flags=re.IGNORECASE)
    else:
        # Comma split is risky ("Last, First"), only do it if parts look like names
        parts = author_str.split(",")
        # Heuristic: if every part starts with capital, treat as separate names
        caps = [p.strip() for p in parts if p.strip()]
        if all(re.match(r"^[A-Z]", p) for p in caps) and len(caps) <= 10:
            pass   # use as-is
        else:
            # Likely "Last, First Middle" format — treat whole string as one author
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
    """
    Font+position based author extraction.

    Logic:
      1. Flatten all spans on page 1 with (y, x, size, text)
      2. Locate the bottom of the title region
      3. Scan downward for spans in the "author zone" (below title, above affiliations)
      4. Author spans are smaller than title but often larger than body,
         contain proper-name-shaped tokens
    """
    if not blocks:
        return []

    all_spans: list[tuple[float, float, float, str]] = []  # (y, x, size, text)
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

    # Locate title bottom — scan for a span containing start of title text
    title_prefix = title[:20].lower() if title else ""
    title_bottom = 0.0
    for y, x, size, text in all_spans:
        if title_prefix and title_prefix in text.lower():
            title_bottom = y + 20   # just below this line
            break
    if title_bottom == 0.0:
        # Fallback: use top 18% of page
        page_h       = max(sp[0] for sp in all_spans) or 842
        title_bottom = page_h * 0.18

    # Scan for author names in the zone below the title
    candidates: list[str] = []
    no_author_streak = 0

    for y, x, size, text in all_spans:
        if y < title_bottom:
            continue
        if y > title_bottom + 320:      # stop after 320pt below title bottom
            break
        if _STOP_AUTHOR.search(text):   # hit institution / abstract marker
            break

        # Skip title-sized text (multi-line title continuation)
        if size >= max_size * 0.87:
            continue

        # Split on common separators and try each token
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
            if no_author_streak >= 3 and len(candidates) > 0:
                break   # 3 consecutive non-author lines after finding some = stop
        else:
            no_author_streak = 0

        if len(candidates) >= 12:
            break

    return _dedupe(candidates)[:10]


def _authors_from_text(first_page: str, title: str) -> list[str]:
    """
    Text-based author extraction: scan lines after the title.
    Handles multi-author lines with degree suffixes and superscripts.
    """
    lines       = [l.strip() for l in first_page.split("\n") if l.strip()]
    candidates  : list[str] = []
    title_found = False
    title_low   = title[:30].lower() if title else ""
    no_auth_streak = 0

    for line in lines:
        if not title_found:
            if title_low and title_low in line.lower():
                title_found = True
            continue

        if _STOP_AUTHOR.search(line):
            break

        parts = re.split(
            r"[,;]|(?<=\w)\s+and\s+(?=[A-Z])",
            line, flags=re.IGNORECASE,
        )
        found_here = 0
        for part in parts:
            cleaned = _clean_author(part)
            if _is_author_name(cleaned):
                candidates.append(cleaned)
                found_here += 1
                if len(candidates) >= 12:
                    break

        if found_here == 0:
            no_auth_streak += 1
            if no_auth_streak >= 3 and len(candidates) > 0:
                break
        else:
            no_auth_streak = 0

        if len(candidates) >= 12:
            break

    return _dedupe(candidates)[:10]


def _dedupe(items: list[str]) -> list[str]:
    """Deduplicate while preserving order, case-insensitive."""
    seen:   set[str]  = set()
    result: list[str] = []
    for item in items:
        key = item.lower()
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


# ── Abstract ──────────────────────────────────────────────────────────────────

def _extract_abstract(first_page: str) -> str:
    """
    Extract the abstract from page 1 text.

    Tries in order:
      1. Explicit 'Abstract' / 'Summary' label followed by text
      2. First substantial paragraph after the author block
    """
    # Strategy 1: labelled abstract
    m = re.search(
        r"\b(?:Abstract|Summary|Overview|Synopsis)\b\s*[:—]?\s*\n?"
        r"([\s\S]{80,2500}?)"
        r"(?=\n\s*\n\s*(?:Keywords?|Key\s+words?|Index\s+terms?|"
        r"Introduction|Background|1\.|I\.|$))",
        first_page, re.IGNORECASE,
    )
    if m:
        return _clean(m.group(1))[:2000]

    # Strategy 2: first long paragraph (skip title + author block = first 2 paras)
    paragraphs = [
        p.strip() for p in re.split(r"\n\s*\n", first_page)
        if len(p.strip()) > 120
    ]
    for para in paragraphs[1:5]:
        # Skip if it looks like an affiliation block
        if _STOP_AUTHOR.search(para[:100]):
            continue
        if len(para) > 120:
            return _clean(para[:2000])

    return ""


# ── DOI ───────────────────────────────────────────────────────────────────────

def _extract_doi(text: str) -> str:
    """
    Extract DOI. Handles:
      - 10.XXXX/suffix (standard)
      - doi: 10.XXXX/suffix (labelled)
      - https://doi.org/10.XXXX/suffix (URL form)
    """
    # URL form first (most explicit)
    m = re.search(r"https?://(?:dx\.)?doi\.org/(10\.\d{4,9}/[^\s\"'<>\]\)]+)", text)
    if m:
        return m.group(1).rstrip(".,;)]")

    # Labelled form
    m = re.search(r"\bdoi\s*:?\s*(10\.\d{4,9}/[^\s\"'<>\]\)]+)", text, re.IGNORECASE)
    if m:
        return m.group(1).rstrip(".,;)]")

    # Bare form
    m = re.search(r"\b(10\.\d{4,9}/[^\s\"'<>\]\)]{3,})", text)
    if m:
        return m.group(1).rstrip(".,;)]")

    return ""


# ── ISSN ──────────────────────────────────────────────────────────────────────

def _extract_issn(text: str) -> str:
    """
    Extract ISSN (print or online). Format: XXXX-XXXX.
    Tries labelled forms first, then context-aware bare search.
    """
    # E-ISSN or P-ISSN labelled
    m = re.search(r"\b[EP]-?ISSN[:\s]*(\d{4}-\d{3}[\dXx])\b", text, re.IGNORECASE)
    if m:
        return m.group(1)
    # ISSN labelled
    m = re.search(r"\bISSN[:\s]*(\d{4}-\d{3}[\dXx])\b", text, re.IGNORECASE)
    if m:
        return m.group(1)
    # ISSN on same line as "journal" or "copyright"
    m = re.search(
        r"(?:journal|issn|copyright|published)[^\n]*\b(\d{4}-\d{3}[\dXx])\b",
        text, re.IGNORECASE,
    )
    if m:
        return m.group(1)
    # Bare ISSN in first 1500 chars (low confidence, last resort)
    m = re.search(r"\b(\d{4}-\d{3}[\dXx])\b", text[:1500])
    return m.group(1) if m else ""


# ── Publisher ─────────────────────────────────────────────────────────────────

def _extract_publisher(text: str) -> str:
    """
    Extract publisher from:
      1. Explicit "Published by" / "Publisher:" label
      2. Known publisher name in first 4000 chars
    """
    chunk = text[:4000]

    # Explicit label
    m = re.search(
        r"(?:Published\s+by|Publisher\s*:)\s*([A-Z][^\n]{3,80})",
        chunk, re.IGNORECASE,
    )
    if m:
        pub = m.group(1)
        pub = re.split(r"\s*[,.]?\s*(?:Inc\.|Ltd\.?|LLC|All rights|Copyright|\d{4})", pub)[0]
        return pub.strip()[:80]

    # Known publisher names (longest match first to avoid partial matches)
    chunk_l = chunk.lower()
    for pub in sorted(_KNOWN_PUBLISHERS, key=len, reverse=True):
        if pub.lower() in chunk_l:
            return pub

    return ""


# ── Journal ───────────────────────────────────────────────────────────────────

def _extract_journal(text: str, meta_title: str = "") -> str:
    """
    Extract journal name. Tries multiple patterns then cleans the result.
    Also checks if the PDF metadata 'title' looks like a journal name
    (e.g. Elsevier often puts the journal in the title field).
    """
    chunk = text[:5000]

    patterns = [
        # Explicit label
        r"(?:Published\s+in|Journal\s*:)\s*([A-Z][^\n]{5,100})",
        # "Journal of X" — full form
        r"((?:International\s+|European\s+|American\s+|British\s+|African\s+|"
        r"Asian\s+|Canadian\s+|Australian\s+|Indian\s+)?"
        r"(?:Journal|Review|Annals|Archives|Bulletin|Proceedings|Transactions|"
        r"Letters|Reports|Advances|Frontiers|Perspectives|Current)\s+"
        r"(?:of|for|on|in)\s+[A-Z][^\n]{3,70})",
        # "[Word]+ Journal" pattern
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,6}\s+Journal[^\n]{0,30})",
        # Specific geography prefixes
        r"((?:Nigerian|Ghanaian|Kenyan|South\s+African|Egyptian|Indian|Chinese|"
        r"Korean|Japanese|Brazilian|Mexican)\s+[A-Z][^\n]{5,60})",
    ]

    for pat in patterns:
        m = re.search(pat, chunk, re.IGNORECASE)
        if m:
            j = m.group(1).strip()
            # Trim at year / volume / issue markers
            j = re.split(r"\s+\d{4}\b|\s+[Vv]ol|\s+\d+\s*[\(,]", j)[0]
            j = j.strip().rstrip(".,;:")
            if len(j) >= 8:
                return j[:100]

    # Check if PDF metadata title field is actually a journal name
    if meta_title and re.search(
        r"\b(Journal|Review|Annals|Bulletin|Transactions|Letters)\b",
        meta_title, re.IGNORECASE,
    ):
        return meta_title[:100]

    return ""


# ── Volume / Issue / Year ─────────────────────────────────────────────────────

def _extract_volume(text: str) -> str:
    m = re.search(
        r"\bVol(?:ume)?\.?\s*(\d+)",
        text[:4000], re.IGNORECASE,
    )
    return m.group(1) if m else ""


def _extract_issue(text: str) -> str:
    """Extract issue/number from Vol X(Y) or Issue Y or No. Y patterns."""
    # Vol X(Y) — issue in parentheses right after volume
    m = re.search(r"\bVol(?:ume)?\.?\s*\d+\s*[\(,]\s*(\d+)\s*[\),]", text[:4000], re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"\b(?:Issue|No\.?|Number)\s*(\d+)", text[:4000], re.IGNORECASE)
    return m.group(1) if m else ""


def _extract_year(text: str, created: str) -> str:
    """
    Extract publication year.
    Tries explicit patterns in text, falls back to PDF creation date.
    """
    # Pattern: (2016) or , 2016. or "published 2016" in first 3 pages
    m = re.search(
        r"(?:published|accepted|received|online|copyright)\D{0,20}((?:19|20)\d{2})\b",
        text[:4000], re.IGNORECASE,
    )
    if m:
        return m.group(1)

    # Year in parentheses next to volume/journal info
    m = re.search(r"\b((?:19|20)\d{2})\b", text[:2000])
    if m:
        yr = int(m.group(1))
        if 1900 <= yr <= 2030:
            return str(yr)

    # Fall back to creation date from PDF metadata
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
    """
    Extract keywords from:
      1. 'Keywords:' / 'Key words:' / 'Index terms:' labelled block in text
      2. PDF metadata keywords field
    """
    # Strategy 1: labelled block in text
    m = re.search(
        r"(?:Keywords?|Key\s+words?|Index\s+[Tt]erms?|[Kk]ey[Pp]hrases?)"
        r"\s*[:—]\s*"
        r"([^\n]{10,600})",
        text[:10000], re.IGNORECASE,
    )
    if m:
        raw  = m.group(1).strip()
        kws  = [k.strip().strip("•·-–—") for k in re.split(r"[;,•·]", raw)]
        kws  = [k for k in kws if 2 < len(k) < 80]
        if kws:
            return kws[:15]

    # Strategy 2: PDF metadata keywords
    if meta_kw:
        kws = [k.strip() for k in re.split(r"[;,]", meta_kw) if k.strip()]
        kws = [k for k in kws if 2 < len(k) < 80]
        return kws[:15]

    return []


# ── Sentence splitter ─────────────────────────────────────────────────────────

def _split_sentences(text: str) -> list[str]:
    """
    Two-pass splitter:
      Pass 1 — protect abbreviations and decimal numbers
      Pass 2 — split on sentence-ending punctuation + capital/bracket
    """
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

    # Protect decimal numbers: 3.14, 2.1, 99.9
    protected = re.sub(r"(\d)\.(\d)", r"\1__D__\2", protected)
    # Protect single-letter initials: "J. Smith"
    protected = re.sub(r"\b([A-Z])\.\s+([A-Z])", r"\1__I__\2", protected)

    # Split on sentence-ending punctuation followed by whitespace + capital/bracket/quote
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


# ── Singleton ─────────────────────────────────────────────────────────────────
extraction_service = ExtractionService()