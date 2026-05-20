"""
app/db/models.py — SQLAlchemy ORM models.

extend_existing=True on every table tells SQLAlchemy to update the
existing table definition rather than raise an error when this module
is re-imported (which Streamlit Cloud does on every rerun).

The mapper re-registration warning ("class already exists in registry")
is suppressed by importing this module exactly once via __init__.py
before any session is opened.
"""

from __future__ import annotations

from datetime import datetime
from sqlalchemy import (
    JSON, Column, DateTime, ForeignKey,
    Integer, LargeBinary, String, Text,
)
from sqlalchemy.orm import relationship

from app.db.base import Base
from app.models.schemas import DocumentStatus, SectionType, MessageRole


class Document(Base):
    __tablename__ = "documents"
    __table_args__ = {"extend_existing": True}

    id                = Column(Integer,      primary_key=True)
    doc_id            = Column(String(36),   unique=True, index=True, nullable=False)
    drive_file_id     = Column(String(128),  index=True,  nullable=True)
    filename          = Column(String(512),  nullable=False)
    title             = Column(String(1024), default="",  nullable=False)
    mime_type         = Column(String(128),  default="application/pdf", nullable=False)
    checksum          = Column(String(64),   default="",  nullable=True)
    modified_time     = Column(DateTime,     nullable=True)
    local_path        = Column(String(1024), nullable=False)
    status            = Column(String(32),   default=DocumentStatus.UPLOADED.value, nullable=False)
    page_count        = Column(Integer,      default=0,   nullable=False)
    chunk_count       = Column(Integer,      default=0,   nullable=False)
    source_folder     = Column(String(512),  nullable=True)
    source            = Column(String(64),   default="upload", nullable=True)
    last_error        = Column(Text,         nullable=True)
    file_size_bytes   = Column(Integer,      default=0,   nullable=False)
    authors           = Column(JSON,         default=list, nullable=False)
    keywords          = Column(JSON,         default=list, nullable=False)
    abstract          = Column(Text,         default="",  nullable=True)
    doi               = Column(String(128),  default="",  nullable=True)
    issn              = Column(String(128),  default="",  nullable=True)
    publisher         = Column(String(256),  default="",  nullable=True)
    journal           = Column(String(256),  default="",  nullable=True)
    volume            = Column(String(64),   default="",  nullable=True)
    issue             = Column(String(64),   default="",  nullable=True)
    article_type      = Column(String(128),  default="",  nullable=True)
    year              = Column(String(32),   default="",  nullable=True)
    language          = Column(String(32),   default="",  nullable=True)
    full_text         = Column(Text,         default="",  nullable=True)
    metadata_json     = Column(JSON,         nullable=True)
    vector_index_path = Column(String(1024), nullable=True)
    created_at        = Column(DateTime,     default=datetime.utcnow, nullable=False)
    updated_at        = Column(DateTime,     default=datetime.utcnow,
                               onupdate=datetime.utcnow, nullable=False)

    versions       = relationship("DocumentVersion", back_populates="document",
                                  cascade="all, delete-orphan")
    sections       = relationship("DocumentSection",  back_populates="document",
                                  cascade="all, delete-orphan")
    chunks         = relationship("DocumentChunk",    back_populates="document",
                                  cascade="all, delete-orphan")
    ingestion_jobs = relationship("IngestionJob",     back_populates="document",
                                  cascade="all, delete-orphan")
    export_jobs    = relationship("ExportJob",        back_populates="document",
                                  cascade="all, delete-orphan")
    chat_sessions  = relationship("ChatSession",      back_populates="document",
                                  cascade="all, delete-orphan")


class DocumentVersion(Base):
    __tablename__ = "document_versions"
    __table_args__ = {"extend_existing": True}

    id              = Column(Integer,      primary_key=True)
    document_id     = Column(Integer,      ForeignKey("documents.id", ondelete="CASCADE"),
                             nullable=False)
    version_number  = Column(Integer,      default=1,        nullable=False)
    drive_file_id   = Column(String(128),  nullable=True)
    checksum        = Column(String(64),   nullable=True)
    modified_time   = Column(DateTime,     nullable=True)
    local_path      = Column(String(1024), nullable=False)
    file_size_bytes = Column(Integer,      default=0,        nullable=False)
    source          = Column(String(64),   default="upload", nullable=False)
    created_at      = Column(DateTime,     default=datetime.utcnow, nullable=False)

    document = relationship("Document", back_populates="versions")


class IngestionJob(Base):
    __tablename__ = "ingestion_jobs"
    __table_args__ = {"extend_existing": True}

    id            = Column(Integer,      primary_key=True)
    document_id   = Column(Integer,      ForeignKey("documents.id", ondelete="CASCADE"),
                           nullable=True)
    drive_file_id = Column(String(128),  nullable=True)
    status        = Column(String(32),   default="queued",  nullable=False)
    queued_at     = Column(DateTime,     default=datetime.utcnow, nullable=False)
    started_at    = Column(DateTime,     nullable=True)
    completed_at  = Column(DateTime,     nullable=True)
    retry_count   = Column(Integer,      default=0,         nullable=False)
    error_message = Column(Text,         nullable=True)
    source        = Column(String(64),   default="drive",   nullable=False)

    document = relationship("Document",      back_populates="ingestion_jobs")
    logs     = relationship("ProcessingLog", back_populates="ingestion_job",
                            cascade="all, delete-orphan")


class SyncRun(Base):
    __tablename__ = "sync_runs"
    __table_args__ = {"extend_existing": True}

    id            = Column(Integer,      primary_key=True)
    folder_id     = Column(String(128),  nullable=True)
    status        = Column(String(32),   default="running", nullable=False)
    total_files   = Column(Integer,      default=0,         nullable=False)
    new_files     = Column(Integer,      default=0,         nullable=False)
    skipped_files = Column(Integer,      default=0,         nullable=False)
    failed_files  = Column(Integer,      default=0,         nullable=False)
    started_at    = Column(DateTime,     default=datetime.utcnow, nullable=False)
    completed_at  = Column(DateTime,     nullable=True)
    error_message = Column(Text,         nullable=True)


class ProcessingLog(Base):
    __tablename__ = "processing_logs"
    __table_args__ = {"extend_existing": True}

    id               = Column(Integer,    primary_key=True)
    document_id      = Column(Integer,    ForeignKey("documents.id", ondelete="CASCADE"),
                              nullable=True)
    ingestion_job_id = Column(Integer,    ForeignKey("ingestion_jobs.id", ondelete="CASCADE"),
                              nullable=True)
    level            = Column(String(16), default="info", nullable=False)
    message          = Column(Text,       nullable=False)
    created_at       = Column(DateTime,   default=datetime.utcnow, nullable=False)

    ingestion_job = relationship("IngestionJob", back_populates="logs")


class DocumentSection(Base):
    __tablename__ = "document_sections"
    __table_args__ = {"extend_existing": True}

    id           = Column(Integer,     primary_key=True)
    document_id  = Column(Integer,     ForeignKey("documents.id", ondelete="CASCADE"),
                          nullable=False)
    section_type = Column(String(64),  default=SectionType.OTHER.value, nullable=False)
    title        = Column(String(512), default="",  nullable=False)
    content      = Column(Text,        nullable=False)
    page_start   = Column(Integer,     default=0,   nullable=False)
    page_end     = Column(Integer,     default=0,   nullable=False)
    char_start   = Column(Integer,     default=0,   nullable=False)
    char_end     = Column(Integer,     default=0,   nullable=False)
    word_count   = Column(Integer,     default=0,   nullable=False)
    created_at   = Column(DateTime,    default=datetime.utcnow, nullable=False)

    document = relationship("Document", back_populates="sections")


class DocumentChunk(Base):
    __tablename__ = "document_chunks"
    __table_args__ = {"extend_existing": True}

    id           = Column(String(64),   primary_key=True)
    document_id  = Column(Integer,      ForeignKey("documents.id", ondelete="CASCADE"),
                          nullable=False)
    chunk_index  = Column(Integer,      default=0,   nullable=False)
    total_chunks = Column(Integer,      default=0,   nullable=False)
    page_number  = Column(Integer,      default=0,   nullable=False)
    section_type = Column(String(64),   default=SectionType.OTHER.value, nullable=False)
    content      = Column(Text,         nullable=False)
    word_count   = Column(Integer,      default=0,   nullable=False)
    char_count   = Column(Integer,      default=0,   nullable=False)
    embedding    = Column(LargeBinary,  nullable=True)
    created_at   = Column(DateTime,     default=datetime.utcnow, nullable=False)

    document = relationship("Document", back_populates="chunks")


class ExportJob(Base):
    __tablename__ = "export_jobs"
    __table_args__ = {"extend_existing": True}

    id            = Column(Integer,     primary_key=True)
    document_id   = Column(Integer,     ForeignKey("documents.id", ondelete="SET NULL"),
                           nullable=True)
    document_ids  = Column(JSON,        nullable=True)
    export_type   = Column(String(32),  nullable=False)
    status        = Column(String(32),  default="queued", nullable=False)
    file_name     = Column(String(256), nullable=True)
    error_message = Column(Text,        nullable=True)
    created_at    = Column(DateTime,    default=datetime.utcnow, nullable=False)
    completed_at  = Column(DateTime,    nullable=True)

    document = relationship("Document", back_populates="export_jobs")


class ChatSession(Base):
    __tablename__ = "chat_sessions"
    __table_args__ = {"extend_existing": True}

    id          = Column(Integer,     primary_key=True)
    document_id = Column(Integer,     ForeignKey("documents.id", ondelete="CASCADE"),
                         nullable=True)
    name        = Column(String(128), nullable=True)
    created_at  = Column(DateTime,    default=datetime.utcnow, nullable=False)
    updated_at  = Column(DateTime,    default=datetime.utcnow,
                         onupdate=datetime.utcnow, nullable=False)

    document = relationship("Document",    back_populates="chat_sessions")
    messages = relationship("ChatMessage", back_populates="session",
                            cascade="all, delete-orphan")


class ChatMessage(Base):
    __tablename__ = "chat_messages"
    __table_args__ = {"extend_existing": True}

    id         = Column(Integer,     primary_key=True)
    session_id = Column(Integer,     ForeignKey("chat_sessions.id", ondelete="CASCADE"),
                        nullable=False)
    role       = Column(String(16),  default=MessageRole.USER.value, nullable=False)
    content    = Column(Text,        nullable=False)
    provider   = Column(String(64),  nullable=True)
    model      = Column(String(128), nullable=True)
    created_at = Column(DateTime,    default=datetime.utcnow, nullable=False)

    session = relationship("ChatSession", back_populates="messages")