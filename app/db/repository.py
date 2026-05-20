from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload

from app.db.models import (
    ChatMessage,
    ChatSession,
    Document,
    DocumentChunk,
    DocumentSection,
    DocumentVersion,
    ExportJob,
    IngestionJob,
    ProcessingLog,
    SyncRun,
)
from app.db.session import SessionLocal, engine
from app.db.base import Base
from app.models.schemas import (
    DocumentMetadata,
    DocumentSection as SchemaSection,
    DocumentStatus,
    ProcessedDocument,
    SectionType,
    TextChunk,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)


class Repository:
    def __init__(self) -> None:
        self.logger = get_logger(__name__)

    @contextmanager
    def session(self):
        session = SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def create_document(
        self,
        doc_id: str,
        filename: str,
        local_path: str,
        file_size_bytes: int,
        mime_type: str = "application/pdf",
        source_folder: str | None = None,
        drive_file_id: str | None = None,
        checksum: str | None = None,
        modified_time: datetime | None = None,
        source: str = "upload",
        status: DocumentStatus | str = DocumentStatus.UPLOADED,
    ) -> Document:
        if isinstance(status, DocumentStatus):
            status_value = status.value
        else:
            status_value = status
        with self.session() as session:
            doc = Document(
                doc_id=doc_id,
                filename=filename,
                local_path=local_path,
                file_size_bytes=file_size_bytes,
                mime_type=mime_type,
                source_folder=source_folder,
                drive_file_id=drive_file_id,
                checksum=checksum or "",
                modified_time=modified_time,
                status=status_value,
            )
            session.add(doc)
            session.flush()
            self._create_version(
                session=session,
                document=doc,
                source=source,
                checksum=checksum,
                modified_time=modified_time,
            )
            return doc

    def _create_version(
        self,
        session,
        document: Document,
        source: str = "upload",
        checksum: str | None = None,
        modified_time: datetime | None = None,
    ) -> DocumentVersion:
        version_number = (
            session.query(func.count(DocumentVersion.id))
            .filter(DocumentVersion.document_id == document.id)
            .scalar() or 0
        ) + 1
        version = DocumentVersion(
            document_id=document.id,
            version_number=version_number,
            drive_file_id=document.drive_file_id,
            checksum=checksum or document.checksum,
            modified_time=modified_time,
            local_path=document.local_path,
            file_size_bytes=document.file_size_bytes,
            source=source,
        )
        session.add(version)
        return version

    def get_document_by_doc_id(self, doc_id: str) -> Document | None:
        with self.session() as session:
            stmt = select(Document).where(Document.doc_id == doc_id)
            return session.execute(stmt).scalar_one_or_none()

    def get_document_by_drive_file_id(self, drive_file_id: str) -> Document | None:
        with self.session() as session:
            stmt = select(Document).where(Document.drive_file_id == drive_file_id)
            return session.execute(stmt).scalar_one_or_none()

    def list_documents(self) -> list[dict]:
        with self.session() as session:
            stmt = select(Document).order_by(Document.updated_at.desc())
            docs = session.execute(stmt).scalars().all()
            return [self._summary_from_record(doc) for doc in docs]

    def _summary_from_record(self, doc: Document) -> dict:
        return {
            "doc_id": doc.doc_id,
            "filename": doc.filename,
            "status": doc.status,
            "title": doc.title or doc.filename,
            "authors": doc.authors or [],
            "pages": doc.page_count,
            "chunks": doc.chunk_count,
            "source": doc.source_folder or "local",
            "updated_at": doc.updated_at.isoformat() if doc.updated_at else "",
            "created_at": doc.created_at.isoformat() if doc.created_at else "",
            "drive_file_id": doc.drive_file_id,
            "last_error": doc.last_error,
        }

    def update_document(
        self,
        doc: ProcessedDocument,
        status: DocumentStatus | None = None,
        error_message: str | None = None,
    ) -> bool:
        with self.session() as session:
            stmt = select(Document).where(Document.doc_id == doc.doc_id)
            record = session.execute(stmt).scalar_one_or_none()
            if not record:
                return False
            record.filename = doc.filename
            record.local_path = doc.file_path
            record.full_text = doc.full_text
            record.page_count = doc.metadata.page_count
            record.chunk_count = doc.chunk_count
            record.vector_index_path = doc.vector_index_path or record.vector_index_path
            record.last_error = error_message or doc.error_message or record.last_error
            record.title = doc.metadata.title or record.title
            record.authors = doc.metadata.authors or record.authors
            record.abstract = doc.metadata.abstract or record.abstract
            record.keywords = doc.metadata.keywords or record.keywords
            record.doi = getattr(doc.metadata, "doi", record.doi)
            record.issn = getattr(doc.metadata, "issn", record.issn)
            record.publisher = getattr(doc.metadata, "publisher", record.publisher)
            record.journal = getattr(doc.metadata, "journal", record.journal)
            record.volume = getattr(doc.metadata, "volume", record.volume)
            record.issue = getattr(doc.metadata, "issue", record.issue)
            record.article_type = getattr(doc.metadata, "article_type", record.article_type)
            record.year = getattr(doc.metadata, "year", record.year)
            record.language = getattr(doc.metadata, "language", record.language)
            record.metadata_json = doc.metadata.model_dump() if doc.metadata else record.metadata_json
            if status:
                record.status = status.value
            elif isinstance(doc.status, DocumentStatus):
                record.status = doc.status.value
            doc.updated_at = datetime.utcnow()
            record.updated_at = doc.updated_at
            session.add(record)
            if doc.sections:
                self.save_sections(doc.doc_id, doc.sections, session=session)
            if doc.chunks:
                self.save_chunks(doc.doc_id, doc.chunks, session=session, preserve_embeddings=True)
            return True

    def update_document_file(
        self,
        doc_id: str,
        local_path: str,
        checksum: str,
        modified_time: datetime | None,
        file_size_bytes: int,
        status: DocumentStatus,
        drive_file_id: str | None = None,
        source_folder: str | None = None,
        source: str | None = None,
    ) -> bool:
        with self.session() as session:
            record = session.execute(select(Document).where(Document.doc_id == doc_id)).scalar_one_or_none()
            if not record:
                return False
            record.local_path = local_path
            record.checksum = checksum
            record.modified_time = modified_time
            record.file_size_bytes = file_size_bytes
            record.status = status.value
            if drive_file_id is not None:
                record.drive_file_id = drive_file_id
            if source_folder is not None:
                record.source_folder = source_folder
            if source is not None:
                record.source = source
            record.updated_at = datetime.utcnow()
            self._create_version(
                session=session,
                document=record,
                source=source or "drive",
                checksum=checksum,
                modified_time=modified_time,
            )
            session.add(record)
            return True

    def save_sections(
        self,
        doc_id: str,
        sections: list[SchemaSection],
        session=None,
    ) -> None:
        own_session = session is None
        if own_session:
            session = SessionLocal()
        try:
            document = session.execute(
                select(Document).where(Document.doc_id == doc_id)
            ).scalar_one_or_none()
            if not document:
                return
            session.query(DocumentSection).filter(
                DocumentSection.document_id == document.id
            ).delete()
            for section in sections:
                record = DocumentSection(
                    document_id=document.id,
                    section_type=section.section_type.value,
                    title=section.title,
                    content=section.content,
                    page_start=section.page_start,
                    page_end=section.page_end,
                    char_start=section.char_start,
                    char_end=section.char_end,
                    word_count=section.word_count,
                )
                session.add(record)
            if own_session:
                session.commit()
        finally:
            if own_session:
                session.close()

    def save_chunks(
        self,
        doc_id: str,
        chunks: list[TextChunk],
        embeddings: np.ndarray | None = None,
        session=None,
        preserve_embeddings: bool = False,
    ) -> None:
        own_session = session is None
        if own_session:
            session = SessionLocal()
        try:
            document = session.execute(
                select(Document).where(Document.doc_id == doc_id)
            ).scalar_one_or_none()
            if not document:
                return
            existing_embeddings: dict[str, bytes] = {}
            if preserve_embeddings:
                existing = session.execute(
                    select(DocumentChunk).where(DocumentChunk.document_id == document.id)
                ).scalars().all()
                existing_embeddings = {
                    chunk.id: chunk.embedding for chunk in existing if chunk.embedding
                }
            session.query(DocumentChunk).filter(
                DocumentChunk.document_id == document.id
            ).delete()
            for idx, chunk in enumerate(chunks):
                embedding_bytes = None
                if embeddings is not None and idx < len(embeddings):
                    array = np.asarray(embeddings[idx], dtype=np.float32)
                    embedding_bytes = array.tobytes()
                elif preserve_embeddings:
                    embedding_bytes = existing_embeddings.get(chunk.chunk_id)
                record = DocumentChunk(
                    id=chunk.chunk_id,
                    document_id=document.id,
                    chunk_index=chunk.chunk_index,
                    total_chunks=chunk.total_chunks,
                    page_number=chunk.page_number,
                    section_type=chunk.section_type.value,
                    content=chunk.content,
                    word_count=chunk.word_count,
                    char_count=chunk.char_count,
                    embedding=embedding_bytes,
                )
                session.add(record)
            if own_session:
                session.commit()
        finally:
            if own_session:
                session.close()

    def load_processed_document(self, doc_id: str) -> ProcessedDocument | None:
        with self.session() as session:
            record = session.execute(
                select(Document).where(Document.doc_id == doc_id)
            ).scalar_one_or_none()
            if not record:
                return None
            sections = session.execute(
                select(DocumentSection).where(DocumentSection.document_id == record.id)
            ).scalars().all()
            chunks = session.execute(
                select(DocumentChunk).where(DocumentChunk.document_id == record.id).order_by(DocumentChunk.chunk_index)
            ).scalars().all()
            metadata = DocumentMetadata(
                title=record.title or "",
                authors=record.authors or [],
                abstract=record.abstract or "",
                keywords=record.keywords or [],
                doi=record.doi or "",
                issn=record.issn or "",
                publisher=record.publisher or "",
                journal=record.journal or "",
                volume=record.volume or "",
                issue=record.issue or "",
                article_type=record.article_type or "",
                year=record.year or "",
                language=record.language or "",
                page_count=record.page_count or 0,
                word_count=len((record.full_text or "").split()),
                file_size_bytes=record.file_size_bytes or 0,
            )
            processed = ProcessedDocument(
                doc_id=record.doc_id,
                filename=record.filename,
                file_path=record.local_path,
                status=DocumentStatus(record.status),
                metadata=metadata,
                full_text=record.full_text or "",
                sections=[
                    SchemaSection(
                        section_type=SectionType(section.section_type),
                        title=section.title,
                        content=section.content,
                        page_start=section.page_start,
                        page_end=section.page_end,
                        char_start=section.char_start,
                        char_end=section.char_end,
                        word_count=section.word_count,
                    )
                    for section in sections
                ],
                chunks=[
                    TextChunk(
                        chunk_id=chunk.id,
                        doc_id=record.doc_id,
                        content=chunk.content,
                        section_type=SectionType(chunk.section_type),
                        chunk_index=chunk.chunk_index,
                        total_chunks=chunk.total_chunks,
                        page_number=chunk.page_number,
                        word_count=chunk.word_count,
                        char_count=chunk.char_count,
                    )
                    for chunk in chunks
                ],
                chunk_count=len(chunks),
                vector_index_path=record.vector_index_path,
                error_message=record.last_error,
                created_at=record.created_at,
                updated_at=record.updated_at,
            )
            return processed

    def delete_document(self, doc_id: str) -> bool:
        with self.session() as session:
            record = session.execute(select(Document).where(Document.doc_id == doc_id)).scalar_one_or_none()
            if not record:
                return False
            session.delete(record)
            return True

    def get_documents_for_search(
        self,
        doc_ids: list[str] | None = None,
        author: str | None = None,
        year: str | None = None,
    ) -> list[str]:
        with self.session() as session:
            stmt = select(Document.doc_id).where(Document.status == DocumentStatus.READY.value)
            if doc_ids:
                stmt = stmt.where(Document.doc_id.in_(doc_ids))
            if author:
                stmt = stmt.where(Document.authors.contains([author]))
            if year:
                stmt = stmt.where(Document.year == year)
            return [row[0] for row in session.execute(stmt).all()]

    def get_library_chunks(
        self,
        doc_ids: list[str] | None = None,
        author: str | None = None,
        year: str | None = None,
        section_type: SectionType | None = None,
        page_number: int | None = None,
    ) -> list[tuple[DocumentChunk, Document]]:
        with self.session() as session:
            stmt = (
                select(DocumentChunk, Document)
                .join(Document, Document.id == DocumentChunk.document_id)
                .where(Document.status == DocumentStatus.READY.value)
            )
            if doc_ids:
                stmt = stmt.where(Document.doc_id.in_(doc_ids))
            if author:
                stmt = stmt.where(Document.authors.contains([author]))
            if year:
                stmt = stmt.where(Document.year == year)
            if section_type:
                stmt = stmt.where(DocumentChunk.section_type == section_type.value)
            if page_number is not None:
                stmt = stmt.where(DocumentChunk.page_number == page_number)
            return [(row[0], row[1]) for row in session.execute(stmt).all()]

    def get_recent_sync_runs(self, limit: int = 10) -> list[SyncRun]:
        with self.session() as session:
            stmt = select(SyncRun).order_by(SyncRun.started_at.desc()).limit(limit)
            return session.execute(stmt).scalars().all()

    def get_recent_processing_logs(self, limit: int = 50) -> list[dict]:
        with self.session() as session:
            stmt = (
                select(ProcessingLog, Document, IngestionJob)
                .outerjoin(Document, Document.id == ProcessingLog.document_id)
                .outerjoin(IngestionJob, IngestionJob.id == ProcessingLog.ingestion_job_id)
                .order_by(ProcessingLog.created_at.desc())
                .limit(limit)
            )
            rows = session.execute(stmt).all()
            return [
                {
                    "created_at": log.created_at.isoformat() if log.created_at else "",
                    "level": log.level,
                    "message": log.message,
                    "doc_id": doc.doc_id if doc else "",
                    "filename": doc.filename if doc else "",
                    "job_id": job.id if job else None,
                    "job_status": job.status if job else "",
                }
                for log, doc, job in rows
            ]

    def get_library_stats(self) -> dict:
        with self.session() as session:
            total_docs = session.scalar(select(func.count(Document.id))) or 0
            ready_docs = session.scalar(
                select(func.count(Document.id)).where(Document.status == DocumentStatus.READY.value)
            ) or 0
            failed_docs = session.scalar(
                select(func.count(Document.id)).where(Document.status == DocumentStatus.FAILED.value)
            ) or 0
            total_chunks = session.scalar(select(func.count(DocumentChunk.id))) or 0
            total_pages = session.scalar(select(func.coalesce(func.sum(Document.page_count), 0))) or 0
            pending_jobs = session.scalar(
                select(func.count(IngestionJob.id)).where(IngestionJob.status.in_(["queued", "running"]))
            ) or 0
            return {
                "total_documents": total_docs,
                "ready_documents": ready_docs,
                "failed_documents": failed_docs,
                "total_chunks": total_chunks,
                "total_pages": total_pages,
                "pending_jobs": pending_jobs,
            }

    def delete_all_documents(self) -> int:
        with self.session() as session:
            count = session.scalar(select(func.count(Document.id))) or 0
            session.query(Document).delete()
            return int(count)

    def create_sync_run(self, folder_id: str | None, total_files: int) -> SyncRun:
        with self.session() as session:
            run = SyncRun(folder_id=folder_id, total_files=total_files)
            session.add(run)
            session.flush()
            return run

    def finish_sync_run(
        self,
        sync_run: SyncRun,
        status: str,
        new_files: int,
        skipped_files: int,
        failed_files: int,
        updated_files: int = 0,
        error_message: str | None = None,
    ) -> None:
        with self.session() as session:
            record = session.execute(select(SyncRun).where(SyncRun.id == sync_run.id)).scalar_one_or_none()
            if not record:
                return
            record.status = status
            record.new_files = new_files
            record.updated_files = updated_files
            record.skipped_files = skipped_files
            record.failed_files = failed_files
            record.completed_at = datetime.utcnow()
            record.error_message = error_message
            session.add(record)

    def create_ingestion_job(
        self,
        document_id: int | None,
        drive_file_id: str | None,
        source: str = "drive",
    ) -> IngestionJob:
        with self.session() as session:
            job = IngestionJob(document_id=document_id, drive_file_id=drive_file_id, source=source)
            session.add(job)
            session.flush()
            return job

    def add_processing_log(
        self,
        document_id: int | None,
        ingestion_job_id: int | None,
        level: str,
        message: str,
    ) -> None:
        with self.session() as session:
            log = ProcessingLog(
                document_id=document_id,
                ingestion_job_id=ingestion_job_id,
                level=level,
                message=message,
            )
            session.add(log)

    def add_document_log(self, doc_id: str, level: str, message: str) -> None:
        with self.session() as session:
            document = session.execute(
                select(Document).where(Document.doc_id == doc_id)
            ).scalar_one_or_none()
            session.add(
                ProcessingLog(
                    document_id=document.id if document else None,
                    ingestion_job_id=None,
                    level=level,
                    message=message,
                )
            )

    def get_ingestion_job(self, ingestion_job_id: int) -> IngestionJob | None:
        with self.session() as session:
            return session.execute(
                select(IngestionJob)
                .options(selectinload(IngestionJob.document))
                .where(IngestionJob.id == ingestion_job_id)
            ).scalar_one_or_none()

    def get_pending_ingestion_jobs(self, limit: int = 10) -> list[IngestionJob]:
        with self.session() as session:
            stmt = (
                select(IngestionJob)
                .options(selectinload(IngestionJob.document))
                .where(IngestionJob.status == "queued")
                .order_by(IngestionJob.queued_at.asc())
                .limit(limit)
            )
            return session.execute(stmt).scalars().all()

    def start_ingestion_job(self, ingestion_job_id: int) -> bool:
        with self.session() as session:
            job = session.execute(
                select(IngestionJob).where(IngestionJob.id == ingestion_job_id)
            ).scalar_one_or_none()
            if not job:
                return False
            job.status = "running"
            job.started_at = datetime.utcnow()
            session.add(job)
            return True

    def complete_ingestion_job(
        self,
        ingestion_job_id: int,
        status: str = "completed",
        error_message: str | None = None,
    ) -> bool:
        with self.session() as session:
            job = session.execute(
                select(IngestionJob).where(IngestionJob.id == ingestion_job_id)
            ).scalar_one_or_none()
            if not job:
                return False
            job.status = status
            job.completed_at = datetime.utcnow()
            if error_message:
                job.error_message = error_message
            session.add(job)
            return True

    def fail_ingestion_job(self, ingestion_job_id: int, error_message: str) -> bool:
        return self.complete_ingestion_job(
            ingestion_job_id=ingestion_job_id,
            status="failed",
            error_message=error_message,
        )

    def get_document_chunk_embeddings(self, doc_id: str) -> list[tuple[str, np.ndarray]]:
        with self.session() as session:
            record = session.execute(select(Document).where(Document.doc_id == doc_id)).scalar_one_or_none()
            if not record:
                return []
            stmt = select(DocumentChunk).where(DocumentChunk.document_id == record.id).order_by(DocumentChunk.chunk_index)
            chunks = session.execute(stmt).scalars().all()
            result = []
            for chunk in chunks:
                if chunk.embedding:
                    result.append((chunk.id, np.frombuffer(chunk.embedding, dtype=np.float32)))
            return result

    def get_chunk_by_id(self, chunk_id: str) -> DocumentChunk | None:
        with self.session() as session:
            return session.execute(select(DocumentChunk).where(DocumentChunk.id == chunk_id)).scalar_one_or_none()

    def get_chunks_by_ids(self, chunk_ids: Iterable[str]) -> list[DocumentChunk]:
        with self.session() as session:
            stmt = select(DocumentChunk).where(DocumentChunk.id.in_(list(chunk_ids)))
            return session.execute(stmt).scalars().all()


repository = Repository()
