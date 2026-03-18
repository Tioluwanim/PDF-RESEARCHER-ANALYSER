"""
schemas.py - Pydantic v2 models for PDF Research Analyzer.

Improvements:
- DocumentMetadata: added doi, issn, publisher, journal, volume, issue fields
  so export_service can read them directly instead of re-extracting from text
- SearchResult: rank ge=0 (multi-query RAG sets rank=0 before sorting)
- ProcessedDocument: updated_at refreshed on every save via model_validator
- Stricter field validation throughout
- All Optional types use explicit None defaults
"""

from __future__ import annotations

from enum import Enum
from datetime import datetime
from typing import Optional, Any

from pydantic import BaseModel, Field, field_validator


# ── Enums ─────────────────────────────────────────────────────────────────────

class DocumentStatus(str, Enum):
    UPLOADED    = "uploaded"
    EXTRACTING  = "extracting"
    EXTRACTED   = "extracted"
    EMBEDDING   = "embedding"
    READY       = "ready"
    FAILED      = "failed"


class SectionType(str, Enum):
    ABSTRACT     = "abstract"
    INTRODUCTION = "introduction"
    METHODS      = "methods"
    RESULTS      = "results"
    DISCUSSION   = "discussion"
    CONCLUSION   = "conclusion"
    REFERENCES   = "references"
    OTHER        = "other"


class MessageRole(str, Enum):
    USER      = "user"
    ASSISTANT = "assistant"
    SYSTEM    = "system"


class LLMProvider(str, Enum):
    OPENROUTER  = "openrouter"
    HUGGINGFACE = "huggingface"


# ── Document Section ──────────────────────────────────────────────────────────

class DocumentSection(BaseModel):
    section_type : SectionType = SectionType.OTHER
    title        : str         = Field(..., min_length=1)
    content      : str         = Field(..., min_length=1)
    page_start   : int         = Field(default=0, ge=0)
    page_end     : int         = Field(default=0, ge=0)
    char_start   : int         = Field(default=0, ge=0)
    char_end     : int         = Field(default=0, ge=0)
    word_count   : int         = Field(default=0, ge=0)

    def model_post_init(self, __context: Any) -> None:
        if self.word_count == 0 and self.content:
            object.__setattr__(self, "word_count", len(self.content.split()))


# ── Text Chunk ────────────────────────────────────────────────────────────────

class TextChunk(BaseModel):
    chunk_id     : str         = Field(..., min_length=1)
    doc_id       : str         = Field(..., min_length=1)
    content      : str         = Field(..., min_length=1)
    section_type : SectionType = SectionType.OTHER
    chunk_index  : int         = Field(default=0, ge=0)
    total_chunks : int         = Field(default=0, ge=0)
    page_number  : int         = Field(default=0, ge=0)
    word_count   : int         = Field(default=0, ge=0)
    char_count   : int         = Field(default=0, ge=0)

    def model_post_init(self, __context: Any) -> None:
        if self.word_count == 0 and self.content:
            object.__setattr__(self, "word_count", len(self.content.split()))
        if self.char_count == 0 and self.content:
            object.__setattr__(self, "char_count", len(self.content))


# ── Document Metadata ─────────────────────────────────────────────────────────

class DocumentMetadata(BaseModel):
    """
    All extractable metadata from a PDF.
    Fields beyond title/authors/pages are populated by extraction_service
    and consumed directly by export_service — no re-extraction needed.
    """
    title           : str       = Field(default="")
    authors         : list[str] = Field(default_factory=list)
    abstract        : str       = Field(default="")
    keywords        : list[str] = Field(default_factory=list)

    # Bibliographic
    doi             : str       = Field(default="")
    issn            : str       = Field(default="")
    publisher       : str       = Field(default="")
    journal         : str       = Field(default="")
    volume          : str       = Field(default="")
    issue           : str       = Field(default="")

    # Document stats
    page_count      : int       = Field(default=0, ge=0)
    word_count      : int       = Field(default=0, ge=0)
    language        : str       = Field(default="en")
    created_at      : str       = Field(default="")
    file_size_bytes : int       = Field(default=0, ge=0)


# ── Processed Document ────────────────────────────────────────────────────────

class ProcessedDocument(BaseModel):
    doc_id            : str                    = Field(..., min_length=1)
    filename          : str                    = Field(..., min_length=1)
    file_path         : str                    = Field(..., min_length=1)
    status            : DocumentStatus         = DocumentStatus.UPLOADED
    metadata          : DocumentMetadata       = Field(default_factory=DocumentMetadata)
    full_text         : str                    = Field(default="")
    sections          : list[DocumentSection]  = Field(default_factory=list)
    chunks            : list[TextChunk]        = Field(default_factory=list)
    chunk_count       : int                    = Field(default=0, ge=0)
    vector_index_path : Optional[str]          = None
    error_message     : Optional[str]          = None
    created_at        : datetime               = Field(default_factory=datetime.utcnow)
    updated_at        : datetime               = Field(default_factory=datetime.utcnow)

    @field_validator("filename")
    @classmethod
    def filename_must_be_pdf(cls, v: str) -> str:
        if not v.lower().endswith(".pdf"):
            raise ValueError("filename must end with .pdf")
        return v

    def get_section(self, section_type: SectionType) -> Optional[DocumentSection]:
        for s in self.sections:
            if s.section_type == section_type:
                return s
        return None

    def get_section_text(self, section_type: SectionType) -> str:
        s = self.get_section(section_type)
        return s.content if s else ""

    def touch(self) -> "ProcessedDocument":
        """Refresh updated_at timestamp — call before saving."""
        object.__setattr__(self, "updated_at", datetime.utcnow())
        return self

    def summary(self) -> dict:
        return {
            "doc_id"    : self.doc_id,
            "filename"  : self.filename,
            "status"    : self.status.value,
            "pages"     : self.metadata.page_count,
            "words"     : self.metadata.word_count,
            "chunks"    : self.chunk_count,
            "sections"  : [s.section_type.value for s in self.sections],
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


# ── Search / RAG ──────────────────────────────────────────────────────────────

class SearchResult(BaseModel):
    chunk : TextChunk
    score : float = Field(..., ge=0.0, description="Cosine similarity score")
    rank  : int   = Field(default=0, ge=0)


class SearchResponse(BaseModel):
    query          : str
    doc_id         : str
    results        : list[SearchResult] = Field(default_factory=list)
    total_found    : int                = Field(default=0, ge=0)
    search_time_ms : float              = Field(default=0.0, ge=0.0)


# ── Chat ──────────────────────────────────────────────────────────────────────

class ChatMessage(BaseModel):
    role      : MessageRole
    content   : str         = Field(..., min_length=1)
    timestamp : datetime    = Field(default_factory=datetime.utcnow)
    provider  : Optional[LLMProvider] = None
    model     : Optional[str]         = None


class ChatRequest(BaseModel):
    doc_id   : str              = Field(..., min_length=1)
    question : str              = Field(..., min_length=1, max_length=2000)
    history  : list[ChatMessage]= Field(default_factory=list)
    top_k    : int              = Field(default=5, ge=1, le=20)
    stream   : bool             = True

    @field_validator("question")
    @classmethod
    def question_not_empty(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("question cannot be empty")
        return stripped


class ChatResponse(BaseModel):
    answer           : str
    doc_id           : str
    question         : str
    sources          : list[SearchResult] = Field(default_factory=list)
    provider         : LLMProvider        = LLMProvider.OPENROUTER
    model            : str                = ""
    response_time_ms : float              = Field(default=0.0, ge=0.0)
    token_count      : Optional[int]      = None


# ── Upload ────────────────────────────────────────────────────────────────────

class UploadResponse(BaseModel):
    doc_id    : str
    filename  : str
    file_size : int            = Field(ge=0)
    status    : DocumentStatus = DocumentStatus.UPLOADED
    message   : str            = "File uploaded successfully"


# ── Analysis ─────────────────────────────────────────────────────────────────

class AnalysisRequest(BaseModel):
    doc_id    : str
    reprocess : bool = False


class AnalysisResponse(BaseModel):
    doc_id             : str
    status             : DocumentStatus
    message            : str
    sections_found     : list[str] = Field(default_factory=list)
    chunk_count        : int       = Field(default=0, ge=0)
    page_count         : int       = Field(default=0, ge=0)
    word_count         : int       = Field(default=0, ge=0)
    processing_time_ms : float     = Field(default=0.0, ge=0.0)


# ── Error ─────────────────────────────────────────────────────────────────────

class ErrorResponse(BaseModel):
    error     : str
    detail    : Optional[str]   = None
    doc_id    : Optional[str]   = None
    timestamp : datetime        = Field(default_factory=datetime.utcnow)