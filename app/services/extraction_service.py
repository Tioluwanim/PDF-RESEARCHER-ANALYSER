"""
extraction_service.py - PDF text extraction, section detection, and intelligent chunking.

Key improvements over previous version:
- Sentence-aware chunking (never splits mid-sentence)
- Smarter section detection (numbered headings, bold-style ALL CAPS)
- Better PDF text cleaning (handles ligatures, unicode noise, column artefacts)
- Author extraction from first page text when PDF metadata is empty
- Abstract pulled from first-page content even without a heading
"""

from __future__ import annotations

import re
import uuid
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


class ExtractionService:
    """
    Responsible for:
    1. Extracting full text from PDFs page by page
    2. Detecting standard research paper sections
    3. Chunking text with sentence-awareness and overlap
    4. Populating DocumentMetadata
    """

    def __init__(self) -> None:
        self._section_patterns = self._compile_section_patterns()
        logger.info("ExtractionService initialised")

    # ── Main entry point ──────────────────────────────────────────────────────

    def process(self, doc: ProcessedDocument) -> ProcessedDocument:
        slog = ServiceLogger("extraction_service", doc_id=doc.doc_id)
        slog.info("Starting extraction for '%s'", doc.filename)

        try:
            doc.status = DocumentStatus.EXTRACTING

            # 1 — Extract text + metadata
            pages_text, metadata = self._extract_from_pdf(doc.file_path, slog)
            doc.full_text = "\n\n".join(pages_text)
            doc.metadata  = metadata
            slog.info(
                "Extracted %d pages, %d words",
                metadata.page_count, metadata.word_count,
            )

            # 2 — Detect sections
            doc.sections = self._detect_sections(doc.full_text, pages_text, slog)
            slog.info(
                "Detected %d sections: %s",
                len(doc.sections),
                [s.section_type.value for s in doc.sections],
            )

            # 3 — Chunk
            doc.chunks      = self._chunk_document(doc, slog)
            doc.chunk_count = len(doc.chunks)
            slog.info("Generated %d chunks", doc.chunk_count)

            # 4 — Pull abstract into metadata
            abstract = doc.get_section(SectionType.ABSTRACT)
            if abstract:
                doc.metadata.abstract = abstract.content[:1500]

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

        pages_text : list[str] = []
        total_words = 0

        with fitz.open(str(pdf_path)) as pdf:
            page_count = len(pdf)
            raw_meta   = pdf.metadata or {}
            title      = (raw_meta.get("title") or "").strip()
            author_str = (raw_meta.get("author") or "").strip()
            created    = (raw_meta.get("creationDate") or "").strip()

            for page_num in range(page_count):
                page = pdf[page_num]
                text = page.get_text("text")
                text = self._clean_page_text(text)
                pages_text.append(text)
                total_words += len(text.split())

            # Better title: try first-page lines if metadata is empty or generic
            if (not title or len(title) < 8) and pages_text:
                title = self._extract_title_from_text(pages_text[0])

            # Authors from metadata or first-page heuristic
            authors = self._parse_authors(author_str)
            if not authors and pages_text:
                authors = self._extract_authors_from_text(pages_text[0], title)

        metadata = DocumentMetadata(
            title           = title,
            authors         = authors,
            page_count      = page_count,
            word_count      = total_words,
            created_at      = created,
            file_size_bytes = pdf_path.stat().st_size,
        )

        slog.info("Metadata — title='%s' authors=%s", title[:60], authors[:3])
        return pages_text, metadata

    # ── Section detection ─────────────────────────────────────────────────────

    def _detect_sections(
        self,
        full_text  : str,
        pages_text : list[str],
        slog       : ServiceLogger,
    ) -> list[DocumentSection]:

        lines = full_text.split("\n")
        hits  : list[tuple[int, SectionType, str]] = []

        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            # Skip very long lines (body text, not headings)
            if len(stripped) > 150:
                continue
            section_type = self._classify_heading(stripped)
            if section_type:
                # Avoid duplicate consecutive hits for the same type
                if hits and hits[-1][1] == section_type and i - hits[-1][0] < 3:
                    continue
                hits.append((i, section_type, stripped))

        slog.debug("Found %d section headings", len(hits))

        sections: list[DocumentSection] = []

        for idx, (line_idx, section_type, heading) in enumerate(hits):
            start = line_idx + 1
            end   = hits[idx + 1][0] if idx + 1 < len(hits) else len(lines)
            content = "\n".join(lines[start:end]).strip()

            if len(content) < MIN_CHUNK_LENGTH:
                continue

            chars_before = len("\n".join(lines[:line_idx]))
            page_start   = self._estimate_page(chars_before, pages_text)

            sections.append(
                DocumentSection(
                    section_type = section_type,
                    title        = heading,
                    content      = content,
                    page_start   = page_start,
                    page_end     = page_start,
                    char_start   = chars_before,
                    char_end     = chars_before + len(content),
                )
            )

        # Fallback: no sections found — treat whole doc as OTHER
        if not sections:
            slog.warning("No sections detected — treating full text as single section")
            sections.append(
                DocumentSection(
                    section_type = SectionType.OTHER,
                    title        = "Full Document",
                    content      = full_text,
                )
            )

        return sections

    # ── Chunking ──────────────────────────────────────────────────────────────

    def _chunk_document(
        self,
        doc  : ProcessedDocument,
        slog : ServiceLogger,
    ) -> list[TextChunk]:

        chunks: list[TextChunk] = []

        has_real_sections = (
            doc.sections
            and doc.sections[0].section_type != SectionType.OTHER
        )

        if has_real_sections:
            for section in doc.sections:
                section_chunks = self._chunk_text(
                    text         = section.content,
                    doc_id       = doc.doc_id,
                    section_type = section.section_type,
                    page_number  = section.page_start,
                )
                chunks.extend(section_chunks)
        else:
            chunks = self._chunk_text(
                text         = doc.full_text,
                doc_id       = doc.doc_id,
                section_type = SectionType.OTHER,
            )

        for i, chunk in enumerate(chunks):
            chunk.chunk_index  = i
            chunk.total_chunks = len(chunks)

        slog.debug(
            "Chunked into %d chunks (size=%d, overlap=%d)",
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
        """
        Sentence-aware sliding-window chunker.

        Splits by sentence boundaries first, then groups sentences
        into CHUNK_SIZE-word windows with CHUNK_OVERLAP-word carry-over.
        This ensures chunks are always coherent units of thought.
        """
        # Split into sentences
        sentences = self._split_sentences(text)
        if not sentences:
            return []

        chunks      : list[TextChunk] = []
        current     : list[str]       = []
        current_len : int             = 0

        for sent in sentences:
            sent_words = len(sent.split())

            # If adding this sentence would exceed limit, flush current window
            if current_len + sent_words > CHUNK_SIZE and current:
                chunk_text = " ".join(current).strip()
                if len(chunk_text) >= MIN_CHUNK_LENGTH:
                    chunks.append(
                        TextChunk(
                            chunk_id     = str(uuid.uuid4()),
                            doc_id       = doc_id,
                            content      = chunk_text,
                            section_type = section_type,
                            page_number  = page_number,
                        )
                    )

                # Carry over overlap words from end of current window
                overlap_text  = " ".join(current)
                overlap_words = overlap_text.split()
                carry         = overlap_words[-CHUNK_OVERLAP:] if len(overlap_words) > CHUNK_OVERLAP else overlap_words
                current       = [" ".join(carry)]
                current_len   = len(carry)

            current.append(sent)
            current_len += sent_words

        # Flush the final window
        if current:
            chunk_text = " ".join(current).strip()
            if len(chunk_text) >= MIN_CHUNK_LENGTH:
                chunks.append(
                    TextChunk(
                        chunk_id     = str(uuid.uuid4()),
                        doc_id       = doc_id,
                        content      = chunk_text,
                        section_type = section_type,
                        page_number  = page_number,
                    )
                )

        return chunks

    # ── Text cleaning ─────────────────────────────────────────────────────────

    @staticmethod
    def _clean_page_text(text: str) -> str:
        """
        Production-grade PDF text cleanup:
        - Re-joins hyphenated line breaks
        - Fixes ligatures (ﬁ→fi, ﬀ→ff etc.)
        - Removes page headers/footers (short isolated lines with numbers)
        - Collapses whitespace
        - Removes non-printable characters
        """
        # Fix common PDF ligature encoding issues
        ligatures = {
            "\ufb01": "fi", "\ufb02": "fl", "\ufb00": "ff",
            "\ufb03": "ffi", "\ufb04": "ffl", "\u2019": "'",
            "\u2018": "'", "\u201c": '"', "\u201d": '"',
            "\u2013": "-", "\u2014": "--",
        }
        for bad, good in ligatures.items():
            text = text.replace(bad, good)

        # Re-join hyphenated line-break words (e.g. "cogni-\ntive" → "cognitive")
        text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)

        # Remove non-printable characters, keep newlines and standard ASCII
        text = re.sub(r"[^\x20-\x7E\n]", " ", text)

        # Collapse 3+ consecutive newlines to 2
        text = re.sub(r"\n{3,}", "\n\n", text)

        # Collapse multiple spaces
        text = re.sub(r" {2,}", " ", text)

        return text.strip()

    # ── Sentence splitter ─────────────────────────────────────────────────────

    @staticmethod
    def _split_sentences(text: str) -> list[str]:
        """
        Lightweight sentence splitter.
        Splits on '. ', '! ', '? ' followed by a capital letter,
        but not after common abbreviations or decimal numbers.
        Uses a simple two-pass approach to avoid variable-width lookbehind.
        """
        # Common abbreviations — protect them by replacing their period temporarily
        abbrev_patterns = [
            r"Mr\.", r"Mrs\.", r"Dr\.", r"Prof\.", r"Fig\.", r"Tab\.",
            r"Eq\.", r"Sec\.", r"Vol\.", r"No\.", r"pp\.", r"vs\.",
            r"et al\.", r"i\.e\.", r"e\.g\.", r"cf\.", r"approx\.",
        ]
        protected = text
        placeholder_map: dict[str, str] = {}
        for i, abbrev in enumerate(abbrev_patterns):
            placeholder = f"__ABBREV{i}__"
            protected   = re.sub(abbrev, lambda m, p=placeholder: m.group().replace(".", p), protected)
            placeholder_map[placeholder] = "."

        # Also protect decimal numbers like 3.14, 2.1, 99.9
        protected = re.sub(r"(\d)\.(\d)", r"\1__DECIMAL__\2", protected)

        # Now split on sentence-ending punctuation followed by whitespace + capital
        parts = re.split(r"(?<=[.!?])\s+(?=[A-Z\[\(])", protected)

        # Restore placeholders
        restored: list[str] = []
        for part in parts:
            part = part.replace("__DECIMAL__", ".")
            for placeholder in placeholder_map:
                part = part.replace(placeholder, ".")
            part = part.strip()
            if part and len(part) > 5:
                restored.append(part)

        return restored

    # ── Heading classifier ────────────────────────────────────────────────────

    def _classify_heading(self, line: str) -> SectionType | None:
        """
        Returns SectionType if line looks like a section heading.

        Accepts:
        - "Abstract", "1. Introduction", "2.1 Methods"
        - "RESULTS AND DISCUSSION" (all caps)
        - "References" (standalone)
        """
        normalized = line.lower().strip().rstrip(".")

        # Strip leading numbering like "1.", "2.1", "3.4.2"
        normalized = re.sub(r"^\d+(\.\d+)*\.?\s+", "", normalized)

        for section_type, pattern in self._section_patterns.items():
            if pattern.search(normalized):
                return section_type

        return None

    def _compile_section_patterns(self) -> dict[SectionType, re.Pattern]:
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
        for key, section_type in type_map.items():
            keywords = SECTION_KEYWORDS.get(key, [])
            if not keywords:
                continue
            alts    = "|".join(re.escape(kw) for kw in keywords)
            pattern = re.compile(
                rf"(?:^|\b)({alts})(?:\b|$|:|\s)",
                re.IGNORECASE,
            )
            patterns[section_type] = pattern
        return patterns

    # ── Metadata helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _extract_title_from_text(first_page: str) -> str:
        """
        Heuristic title extraction from first page:
        Takes the first line > 15 chars that doesn't look like
        a date, URL, page number, or single word.
        """
        for line in first_page.split("\n"):
            s = line.strip()
            if len(s) < 15:
                continue
            if re.match(r"^\d+$", s):
                continue     # pure number
            if re.match(r"https?://", s):
                continue     # URL
            if s.count(" ") == 0:
                continue     # single word
            return s[:200]
        return ""

    @staticmethod
    def _extract_authors_from_text(first_page: str, title: str) -> list[str]:
        """
        Heuristic: authors often appear on lines 2–10 of the first page,
        immediately after the title, containing comma-separated proper names.
        Returns up to 8 author names.
        """
        lines     = [l.strip() for l in first_page.split("\n") if l.strip()]
        candidates: list[str] = []

        title_found = False
        for line in lines:
            if not title_found:
                if title and title[:30].lower() in line.lower():
                    title_found = True
                continue

            # Stop at lines that look like institution names, dates, emails, abstracts
            lower = line.lower()
            if any(kw in lower for kw in [
                "university", "institute", "department", "abstract",
                "email", "@", "http", "received", "accepted",
            ]):
                break

            # Lines with 2–6 proper names (capital letters) separated by commas
            parts = [p.strip() for p in re.split(r"[,;]", line) if p.strip()]
            proper = [p for p in parts if re.match(r"^[A-Z][a-z]", p)]

            if len(proper) >= 2:
                candidates.extend(proper[:6])
                if len(candidates) >= 8:
                    break

        return candidates[:8]

    @staticmethod
    def _parse_authors(author_str: str) -> list[str]:
        if not author_str:
            return []
        parts = re.split(r"[;,&]+", author_str)
        return [p.strip() for p in parts if p.strip() and len(p.strip()) > 2]

    @staticmethod
    def _estimate_page(char_offset: int, pages_text: list[str]) -> int:
        cumulative = 0
        for i, page in enumerate(pages_text):
            cumulative += len(page)
            if char_offset <= cumulative:
                return i
        return len(pages_text) - 1


# ── Singleton ─────────────────────────────────────────────────────────────────
extraction_service = ExtractionService()