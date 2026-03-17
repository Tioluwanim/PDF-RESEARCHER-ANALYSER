"""
schemas.py - Pydantic models for PDF Research Analyzer
All data structures used across services and the Streamlit UI.
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


# ── Enums ─────────────────────────────────────────────────────────────────────

class DocumentStatus(str, Enum):
    """Lifecycle states of an uploaded PDF."""
    UPLOADED    = "uploaded"       # file saved to disk
    EXTRACTING  = "extracting"     # text extraction in progress
    EXTRACTED   = "extracted"      # text + sections ready
    EMBEDDING   = "embedding"      # generating vectors
    READY       = "ready"          # fully indexed, chat enabled
    FAILED      = "failed"         # processing error


class SectionType(str, Enum):
    """Standard research paper sections."""
    ABSTRACT     = "abstract"
    INTRODUCTION = "introduction"
    METHODS      = "methods"
    RESULTS      = "results"
    DISCUSSION   = "discussion"
    CONCLUSION   = "conclusion"
    REFERENCES   = "references"
    OTHER        = "other"


class MessageRole(str, Enum):
    """Chat message roles."""
    USER      = "user"
    ASSISTANT = "assistant"
    SYSTEM    = "system"


class LLMProvider(str, Enum):
    """Available LLM providers."""
    OPENROUTER   = "openrouter"
    HUGGINGFACE  = "huggingface"


# ── Document Section ──────────────────────────────────────────────────────────

class DocumentSection(BaseModel):
    """A detected section within a PDF document."""

    section_type : SectionType = SectionType.OTHER
    title        : str         = Field(..., description="Detected heading text")
    content      : str         = Field(..., description="Full section text")
    page_start   : int         = Field(default=0, ge=0)
    page_end     : int         = Field(default=0, ge=0)
    char_start   : int         = Field(default=0, ge=0)
    char_end     : int         = Field(default=0, ge=0)
    word_count   : int         = Field(default=0, ge=0)

    def model_post_init(self, __context) -> None:
        if self.word_count == 0 and self.content:
            self.word_count = len(self.content.split())


# ── Text Chunk ────────────────────────────────────────────────────────────────

class TextChunk(BaseModel):
    """A single chunk of text ready for embedding."""

    chunk_id     : str         = Field(..., description="Unique chunk identifier")
    doc_id       : str         = Field(..., description="Parent document ID")
    content      : str         = Field(..., description="Chunk text content")
    section_type : SectionType = SectionType.OTHER
    chunk_index  : int         = Field(default=0, ge=0)
    total_chunks : int         = Field(default=0, ge=0)
    page_number  : int         = Field(default=0, ge=0)
    word_count   : int         = Field(default=0, ge=0)
    char_count   : int         = Field(default=0, ge=0)

    def model_post_init(self, __context) -> None:
        if self.word_count == 0 and self.content:
            self.word_count = len(self.content.split())
        if self.char_count == 0 and self.content:
            self.char_count = len(self.content)


# ── Document Metadata ─────────────────────────────────────────────────────────

class DocumentMetadata(BaseModel):
    """Metadata extracted from a PDF."""

    title       : str          = Field(default="", description="Document title")
    authors     : list[str]    = Field(default_factory=list)
    abstract    : str          = Field(default="")
    keywords    : list[str]    = Field(default_factory=list)
    page_count  : int          = Field(default=0, ge=0)
    word_count  : int          = Field(default=0, ge=0)
    language    : str          = Field(default="en")
    created_at  : str          = Field(default="")
    file_size_bytes: int       = Field(default=0, ge=0)


# ── Processed Document ────────────────────────────────────────────────────────

class ProcessedDocument(BaseModel):
    """
    Full representation of a processed PDF.
    Saved to data/processed/<doc_id>.json after extraction.
    """

    doc_id       : str                    = Field(..., description="UUID for this document")
    filename     : str                    = Field(..., description="Original filename")
    file_path    : str                    = Field(..., description="Path to saved PDF")
    status       : DocumentStatus        = DocumentStatus.UPLOADED
    metadata     : DocumentMetadata      = Field(default_factory=DocumentMetadata)
    full_text    : str                    = Field(default="", description="Complete extracted text")
    sections     : list[DocumentSection] = Field(default_factory=list)
    chunks       : list[TextChunk]        = Field(default_factory=list)
    chunk_count  : int                    = Field(default=0, ge=0)
    vector_index_path: Optional[str]     = Field(default=None)
    error_message: Optional[str]         = Field(default=None)
    created_at   : datetime               = Field(default_factory=datetime.utcnow)
    updated_at   : datetime               = Field(default_factory=datetime.utcnow)

    @field_validator("filename")
    @classmethod
    def filename_must_be_pdf(cls, v: str) -> str:
        if not v.lower().endswith(".pdf"):
            raise ValueError("filename must end with .pdf")
        return v

    def get_section(self, section_type: SectionType) -> Optional[DocumentSection]:
        """Returns the first matching section or None."""
        for s in self.sections:
            if s.section_type == section_type:
                return s
        return None

    def get_section_text(self, section_type: SectionType) -> str:
        """Returns section content or empty string."""
        section = self.get_section(section_type)
        return section.content if section else ""

    def summary(self) -> dict:
        """Compact summary for UI display."""
        return {
            "doc_id"       : self.doc_id,
            "filename"     : self.filename,
            "status"       : self.status.value,
            "pages"        : self.metadata.page_count,
            "words"        : self.metadata.word_count,
            "chunks"       : self.chunk_count,
            "sections"     : [s.section_type.value for s in self.sections],
            "created_at"   : self.created_at.isoformat(),
        }


# ── Search / RAG ──────────────────────────────────────────────────────────────

class SearchResult(BaseModel):
    """A single semantic search result from FAISS."""

    chunk        : TextChunk
    score        : float  = Field(..., description="Similarity score (higher = more relevant)")
    rank         : int    = Field(default=1, ge=1)


class SearchResponse(BaseModel):
    """Full response from a semantic search query."""

    query        : str
    doc_id       : str
    results      : list[SearchResult] = Field(default_factory=list)
    total_found  : int                = Field(default=0, ge=0)
    search_time_ms: float             = Field(default=0.0)


# ── Chat ──────────────────────────────────────────────────────────────────────

class ChatMessage(BaseModel):
    """A single message in a conversation."""

    role      : MessageRole
    content   : str
    timestamp : datetime = Field(default_factory=datetime.utcnow)
    provider  : Optional[LLMProvider] = None   # which LLM answered (assistant only)
    model     : Optional[str]         = None   # model name used


class ChatRequest(BaseModel):
    """Incoming chat request from the UI."""

    doc_id    : str  = Field(..., description="Document to chat with")
    question  : str  = Field(..., min_length=1, max_length=2000)
    history   : list[ChatMessage] = Field(default_factory=list)
    top_k     : int  = Field(default=5, ge=1, le=20)
    stream    : bool = Field(default=True)

    @field_validator("question")
    @classmethod
    def question_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("question cannot be empty or whitespace")
        return v.strip()


class ChatResponse(BaseModel):
    """Full chat response returned to UI."""

    answer        : str
    doc_id        : str
    question      : str
    sources       : list[SearchResult] = Field(default_factory=list)
    provider      : LLMProvider        = LLMProvider.OPENROUTER
    model         : str                = ""
    response_time_ms: float            = Field(default=0.0)
    token_count   : Optional[int]      = None


# ── Upload ────────────────────────────────────────────────────────────────────

class UploadResponse(BaseModel):
    """Returned to UI after a PDF upload."""

    doc_id    : str
    filename  : str
    file_size : int
    status    : DocumentStatus = DocumentStatus.UPLOADED
    message   : str            = "File uploaded successfully"


# ── Analysis ─────────────────────────────────────────────────────────────────

class AnalysisRequest(BaseModel):
    """Request to run full pipeline on an uploaded PDF."""

    doc_id      : str
    reprocess   : bool = Field(
        default=False,
        description="Force reprocess even if already indexed"
    )


class AnalysisResponse(BaseModel):
    """Result of running the full analysis pipeline."""

    doc_id        : str
    status        : DocumentStatus
    message       : str
    sections_found: list[str]   = Field(default_factory=list)
    chunk_count   : int         = 0
    page_count    : int         = 0
    word_count    : int         = 0
    processing_time_ms: float   = 0.0


# ── Error ─────────────────────────────────────────────────────────────────────

class ErrorResponse(BaseModel):
    """Standardised error payload."""

    error      : str
    detail     : Optional[str] = None
    doc_id     : Optional[str] = None
    timestamp  : datetime      = Field(default_factory=datetime.utcnow)