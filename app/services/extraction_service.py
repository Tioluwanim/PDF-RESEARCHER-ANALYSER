"""
extraction_service.py - PDF text extraction, section detection, and intelligent chunking.
Uses PyMuPDF (fitz) for extraction and regex-based section detection.
"""

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


# ── Extraction Service ────────────────────────────────────────────────────────

class ExtractionService:
    """
    Responsible for:
    1. Extracting full text from PDFs page by page
    2. Detecting standard research paper sections
    3. Chunking text intelligently with overlap
    4. Populating DocumentMetadata
    """

    def __init__(self):
        self._section_patterns = self._compile_section_patterns()
        logger.info("ExtractionService initialised")

    # ── Main Entry Point ──────────────────────────────────────────────────────

    def process(self, doc: ProcessedDocument) -> ProcessedDocument:
        """
        Runs the full extraction pipeline on a ProcessedDocument.
        Mutates and returns the document with:
        - full_text populated
        - metadata populated
        - sections detected
        - chunks generated
        - status updated to EXTRACTED

        Args:
            doc: ProcessedDocument with UPLOADED status.

        Returns:
            Updated ProcessedDocument.
        """
        slog = ServiceLogger("extraction_service", doc_id=doc.doc_id)
        slog.info(f"Starting extraction for '{doc.filename}'")

        try:
            doc.status = DocumentStatus.EXTRACTING

            # Step 1 — Extract raw text + metadata
            pages_text, metadata = self._extract_from_pdf(doc.file_path, slog)
            doc.full_text = "\n\n".join(pages_text)
            doc.metadata  = metadata
            slog.info(
                f"Extracted {metadata.page_count} pages, "
                f"{metadata.word_count:,} words"
            )

            # Step 2 — Detect sections
            doc.sections = self._detect_sections(doc.full_text, pages_text, slog)
            slog.info(
                f"Detected {len(doc.sections)} sections: "
                f"{[s.section_type.value for s in doc.sections]}"
            )

            # Step 3 — Chunk text
            doc.chunks      = self._chunk_document(doc, slog)
            doc.chunk_count = len(doc.chunks)
            slog.info(f"Generated {doc.chunk_count} chunks")

            # Step 4 — Pull abstract into metadata if found
            abstract_section = doc.get_section(SectionType.ABSTRACT)
            if abstract_section:
                doc.metadata.abstract = abstract_section.content[:1000]

            doc.status = DocumentStatus.EXTRACTED
            slog.info("Extraction complete ✓")

        except Exception as e:
            doc.status        = DocumentStatus.FAILED
            doc.error_message = str(e)
            slog.error(f"Extraction failed: {e}", exc_info=True)

        return doc

    # ── PDF Extraction ────────────────────────────────────────────────────────

    def _extract_from_pdf(
        self,
        file_path: str,
        slog: ServiceLogger,
    ) -> tuple[list[str], DocumentMetadata]:
        """
        Opens the PDF with PyMuPDF and extracts:
        - Text per page (list of strings)
        - DocumentMetadata (title, authors, page count etc.)

        Args:
            file_path: Absolute path to the PDF.
            slog:      ServiceLogger for this document.

        Returns:
            (pages_text, DocumentMetadata)
        """
        pages_text: list[str] = []
        total_words = 0

        pdf_path = Path(file_path)
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {file_path}")

        with fitz.open(str(pdf_path)) as pdf:
            page_count = len(pdf)
            slog.debug(f"Opened PDF — {page_count} pages")

            # Extract metadata from PDF properties
            raw_meta   = pdf.metadata or {}
            title      = raw_meta.get("title", "").strip()
            author_str = raw_meta.get("author", "").strip()
            created    = raw_meta.get("creationDate", "").strip()

            # Extract text page by page
            for page_num in range(page_count):
                page = pdf[page_num]
                text = page.get_text("text")  # plain text extraction

                # Basic cleanup
                text = self._clean_page_text(text)
                pages_text.append(text)
                total_words += len(text.split())

            # Try to extract title from first page if not in metadata
            if not title and pages_text:
                title = self._extract_title_from_text(pages_text[0])

            # Parse authors
            authors = self._parse_authors(author_str)

        file_size = pdf_path.stat().st_size
        metadata  = DocumentMetadata(
            title           = title,
            authors         = authors,
            page_count      = page_count,
            word_count      = total_words,
            created_at      = created,
            file_size_bytes = file_size,
        )

        return pages_text, metadata

    # ── Section Detection ─────────────────────────────────────────────────────

    def _detect_sections(
        self,
        full_text: str,
        pages_text: list[str],
        slog: ServiceLogger,
    ) -> list[DocumentSection]:
        """
        Scans the full text for section headings using keyword patterns.
        Assigns content between consecutive headings to each section.

        Returns:
            List of DocumentSection objects, ordered by appearance.
        """
        sections: list[DocumentSection] = []
        lines    = full_text.split("\n")
        hits: list[tuple[int, SectionType, str]] = []  # (line_index, type, heading)

        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped or len(stripped) > 120:
                continue

            section_type = self._classify_heading(stripped)
            if section_type:
                hits.append((i, section_type, stripped))

        slog.debug(f"Found {len(hits)} section headings")

        # Build sections from hits — content = text between this heading and next
        for idx, (line_idx, section_type, heading) in enumerate(hits):
            start_line = line_idx + 1
            end_line   = hits[idx + 1][0] if idx + 1 < len(hits) else len(lines)

            content = "\n".join(lines[start_line:end_line]).strip()

            if len(content) < MIN_CHUNK_LENGTH:
                continue

            # Approximate page numbers
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

        # If no sections detected, treat the whole document as OTHER
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
        doc: ProcessedDocument,
        slog: ServiceLogger,
    ) -> list[TextChunk]:
        """
        Chunks the document in two passes:
        1. Section-aware: chunk each detected section separately
           so section boundaries are never crossed.
        2. Falls back to chunking full_text if no sections found.

        Each chunk has CHUNK_OVERLAP words carried over from the
        previous chunk to preserve context across boundaries.

        Returns:
            Flat list of TextChunk objects with sequential chunk_index.
        """
        chunks: list[TextChunk] = []

        if doc.sections and doc.sections[0].section_type != SectionType.OTHER:
            # Section-aware chunking
            for section in doc.sections:
                section_chunks = self._chunk_text(
                    text         = section.content,
                    doc_id       = doc.doc_id,
                    section_type = section.section_type,
                    page_number  = section.page_start,
                )
                chunks.extend(section_chunks)
        else:
            # Fallback — chunk full text
            chunks = self._chunk_text(
                text         = doc.full_text,
                doc_id       = doc.doc_id,
                section_type = SectionType.OTHER,
            )

        # Assign global sequential indices
        total = len(chunks)
        for i, chunk in enumerate(chunks):
            chunk.chunk_index  = i
            chunk.total_chunks = total

        slog.debug(f"Chunked into {total} chunks "
                   f"(size={CHUNK_SIZE}, overlap={CHUNK_OVERLAP})")
        return chunks

    def _chunk_text(
        self,
        text        : str,
        doc_id      : str,
        section_type: SectionType = SectionType.OTHER,
        page_number : int = 0,
    ) -> list[TextChunk]:
        """
        Splits text into overlapping word-based chunks.

        Args:
            text:         Text to chunk.
            doc_id:       Parent document ID.
            section_type: Section this text belongs to.
            page_number:  Approximate page number.

        Returns:
            List of TextChunk objects.
        """
        words  = text.split()
        chunks : list[TextChunk] = []
        start  = 0

        while start < len(words):
            end        = min(start + CHUNK_SIZE, len(words))
            chunk_words = words[start:end]
            chunk_text  = " ".join(chunk_words)

            if len(chunk_text.strip()) >= MIN_CHUNK_LENGTH:
                chunks.append(
                    TextChunk(
                        chunk_id     = str(uuid.uuid4()),
                        doc_id       = doc_id,
                        content      = chunk_text,
                        section_type = section_type,
                        page_number  = page_number,
                    )
                )

            if end >= len(words):
                break

            # Move forward by CHUNK_SIZE - CHUNK_OVERLAP (sliding window)
            start += max(1, CHUNK_SIZE - CHUNK_OVERLAP)

        return chunks

    # ── Text Cleaning ─────────────────────────────────────────────────────────

    @staticmethod
    def _clean_page_text(text: str) -> str:
        """
        Cleans raw text extracted from a PDF page:
        - Removes hyphenated line breaks (re-joins split words)
        - Collapses excessive whitespace
        - Removes non-printable characters
        """
        # Re-join hyphenated words at line breaks
        text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)
        # Collapse multiple spaces
        text = re.sub(r" {2,}", " ", text)
        # Remove non-printable characters (keep newlines)
        text = re.sub(r"[^\x20-\x7E\n]", " ", text)
        # Collapse more than 2 consecutive newlines
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # ── Heading Classification ────────────────────────────────────────────────

    def _classify_heading(self, line: str) -> SectionType | None:
        """
        Returns the SectionType if the line looks like a section heading,
        otherwise returns None.
        """
        normalized = line.lower().strip().rstrip(".")

        for section_type, pattern in self._section_patterns.items():
            if pattern.search(normalized):
                return section_type

        return None

    def _compile_section_patterns(self) -> dict[SectionType, re.Pattern]:
        """
        Compiles regex patterns from SECTION_KEYWORDS in config.
        Each keyword becomes a word-boundary pattern.
        """
        patterns: dict[SectionType, re.Pattern] = {}

        type_map = {
            "abstract"     : SectionType.ABSTRACT,
            "introduction" : SectionType.INTRODUCTION,
            "methods"      : SectionType.METHODS,
            "results"      : SectionType.RESULTS,
            "discussion"   : SectionType.DISCUSSION,
            "conclusion"   : SectionType.CONCLUSION,
            "references"   : SectionType.REFERENCES,
        }

        for key, section_type in type_map.items():
            keywords = SECTION_KEYWORDS.get(key, [])
            if not keywords:
                continue
            # Build alternation pattern, escape special chars
            alts    = "|".join(re.escape(kw) for kw in keywords)
            pattern = re.compile(rf"(?:^|\s)({alts})(?:\s|$|:)", re.IGNORECASE)
            patterns[section_type] = pattern

        return patterns

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_title_from_text(first_page: str) -> str:
        """
        Heuristic: the title is often the first non-empty line of the
        first page that is longer than 10 characters.
        """
        for line in first_page.split("\n"):
            stripped = line.strip()
            if len(stripped) > 10:
                return stripped[:200]
        return ""

    @staticmethod
    def _parse_authors(author_str: str) -> list[str]:
        """
        Splits an author string by common delimiters.
        e.g. "John Smith; Jane Doe, PhD" → ["John Smith", "Jane Doe", "PhD"]
        """
        if not author_str:
            return []
        parts = re.split(r"[;,&]+", author_str)
        return [p.strip() for p in parts if p.strip()]

    @staticmethod
    def _estimate_page(char_offset: int, pages_text: list[str]) -> int:
        """
        Estimates the page number for a given character offset
        by accumulating page lengths.
        """
        cumulative = 0
        for i, page_text in enumerate(pages_text):
            cumulative += len(page_text)
            if char_offset <= cumulative:
                return i
        return len(pages_text) - 1


# ── Module-level singleton ────────────────────────────────────────────────────
extraction_service = ExtractionService()