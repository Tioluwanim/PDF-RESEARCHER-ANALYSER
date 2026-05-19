"""
drive_service.py - Google Drive folder sync for library PDF ingestion.
Watches a shared Drive folder and downloads new/changed supported documents automatically.
Supports PDF, DOCX, DOC, TXT, XLSX, XLS, and CSV files.
"""

from __future__ import annotations

import io
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import parse_qs, urlparse

from app.config import UPLOAD_DIR, BASE_DIR
from app.db.repository import repository
from app.models.schemas import (
    DocumentMetadata,
    DocumentStatus,
    ProcessedDocument,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)

SUPPORTED_DRIVE_MIME_TYPES = [
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/msword",
    "text/plain",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
    "text/csv",
    "application/vnd.google-apps.document",
    "application/vnd.google-apps.spreadsheet",
    "application/vnd.google-apps.presentation",
]
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


class DriveService:
    """
    Syncs PDFs from a Google Drive folder into the local upload pipeline.

    Setup:
      1. Create a Google Cloud project, enable Drive API.
      2. Create a Service Account, download credentials.json.
      3. Share the Drive folder with the service account email.
      4. Set GOOGLE_DRIVE_FOLDER_ID in .env.
      5. Set GOOGLE_CREDENTIALS_PATH in .env (path to credentials.json).
    """

    def __init__(self) -> None:
        self._service = None
        # _folder_id and _creds_path are resolved lazily so that values
        # written to os.environ by config._materialize_google_credentials_file
        # (or Streamlit secrets loading) are always picked up at call time.
        self._folder_id: str | None = None   # resolved by _get_folder_id()
        self._creds_path: str = ""            # resolved by _get_creds_path()
        self.upload_dir = UPLOAD_DIR
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.last_sync: datetime | None = None
        self.last_sync_count: int = 0
        logger.info("DriveService initialised (folder ID resolved on first use)")

    # ── Lazy resolvers ──────────────────────────────────────────────────────

    def _get_folder_id(self) -> str | None:
        """Read GOOGLE_DRIVE_FOLDER_ID from env at call time (never stale)."""
        raw = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
        return self._parse_folder_id(raw or None)

    def _get_creds_path(self) -> str:
        """
        Resolve the credentials file path at call time.
        Priority:
          1. GOOGLE_CREDENTIALS_PATH env var (may have been set by config.py
             after writing secrets to disk)
          2. BASE_DIR/credentials.json  (default materialization location)
        """
        raw = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json").strip()
        if raw and os.path.isabs(raw):
            return raw
        resolved = str(BASE_DIR / raw)
        # Also check the default materialization location as a fallback
        default = str(BASE_DIR / "credentials.json")
        if not Path(resolved).exists() and Path(default).exists():
            return default
        return resolved

    # ── Public API ─────────────────────────────────────────────────────────────

    @property
    def is_configured(self) -> bool:
        """Re-evaluated on every call so secrets loaded after init are seen."""
        folder = self._get_folder_id()
        creds  = self._get_creds_path()
        return bool(folder and Path(creds).exists())

    @property
    def folder_id(self) -> str | None:
        return self._get_folder_id()

    def set_folder_id(self, folder_id: str) -> None:
        """Override folder ID at runtime (stores in env so lazy resolver sees it)."""
        parsed = self._parse_folder_id(folder_id)
        if parsed:
            os.environ["GOOGLE_DRIVE_FOLDER_ID"] = parsed

    def set_credentials_path(self, path: str) -> None:
        """Override credentials path at runtime."""
        os.environ["GOOGLE_CREDENTIALS_PATH"] = path

    def list_drive_files(self) -> list[dict]:
        """List all supported files in the configured Drive folder, including nested folders."""
        svc = self._get_service()
        folder_id = self._get_folder_id()
        if not svc or not folder_id:
            return []

        files: list[dict] = []
        folders = [folder_id]

        try:
            while folders:
                parent_id = folders.pop(0)
                page_token = None
                while True:
                    query = f"'{parent_id}' in parents and trashed=false"
                    response = (
                        svc.files()
                        .list(
                            q=query,
                            fields="nextPageToken, files(id, name, mimeType, modifiedTime, md5Checksum, size)",
                            pageSize=200,
                            pageToken=page_token,
                        )
                        .execute()
                    )
                    for item in response.get("files", []):
                        mime_type = item.get("mimeType")
                        if mime_type == FOLDER_MIME_TYPE:
                            folders.append(item["id"])
                        elif mime_type in SUPPORTED_DRIVE_MIME_TYPES or (
                            isinstance(mime_type, str) and mime_type.startswith("application/vnd.google-apps.")
                        ):
                            files.append(item)
                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break
            return files
        except Exception as e:
            logger.error("Drive list failed: %s", e)
            return []

    def sync(
        self,
        on_file_found: Optional[Callable[[str, int, int], None]] = None,
        on_file_done: Optional[Callable[[str, bytes | None, str | None], None]] = None,
    ) -> dict:
        """
        Sync Drive folder → local.

        Args:
            on_file_found: callback(filename, index, total)
            on_file_done:  callback(filename, file_bytes_or_None, error_msg_or_None)

        Returns:
            {"new": int, "skipped": int, "failed": int, "total": int}
        """
        if not self.is_configured:
            return {"error": "Drive not configured", "new": 0, "skipped": 0, "failed": 0, "total": 0}

        files = self.list_drive_files()
        result = {"new": 0, "skipped": 0, "failed": 0, "total": len(files)}
        _active_folder_id = self._get_folder_id() or ""
        sync_run = repository.create_sync_run(_active_folder_id, len(files))

        for idx, f in enumerate(files):
            file_id = f["id"]
            name = f["name"]
            md5 = f.get("md5Checksum", "")
            size = int(f.get("size", 0) or 0)
            modified_time = self._parse_drive_time(f.get("modifiedTime"))

            if on_file_found:
                on_file_found(name, idx + 1, len(files))

            existing = repository.get_document_by_drive_file_id(file_id)
            if existing and existing.checksum == md5 and existing.modified_time == modified_time:
                result["skipped"] += 1
                if on_file_done:
                    on_file_done(name, None, "skipped")
                continue

            ingestion_job = None
            try:
                file_bytes = self._download_file(file_id, f.get("mimeType"))
                file_bytes, output_name = self._ensure_pdf_for_sync(file_bytes, name, f.get("mimeType"))
                safe_name = self._sanitize_filename(output_name)
                dest_path = self.upload_dir / f"{file_id}_{safe_name}"
                dest_path.write_bytes(file_bytes)

                document_id = None
                if existing:
                    repository.update_document_file(
                        doc_id=existing.doc_id,
                        local_path=str(dest_path),
                        checksum=md5,
                        modified_time=modified_time,
                        file_size_bytes=len(file_bytes),
                        status=DocumentStatus.UPLOADED,
                    )
                    document_id = existing.id
                else:
                    created = repository.create_document(
                        doc_id=str(uuid.uuid4()),
                        filename=name,
                        local_path=str(dest_path),
                        file_size_bytes=len(file_bytes),
                        mime_type="application/pdf",
                        source_folder=self._get_folder_id(),
                        drive_file_id=file_id,
                        checksum=md5,
                        modified_time=modified_time,
                        source="drive",
                    )
                    document_id = created.id

                ingestion_job = repository.create_ingestion_job(
                    document_id=document_id,
                    drive_file_id=file_id,
                    source="drive",
                )

                result["new"] += 1
                if on_file_done:
                    on_file_done(name, file_bytes, None)
            except Exception as e:
                result["failed"] += 1
                logger.error("Failed to download %s: %s", name, e)
                repository.add_processing_log(
                    document_id=getattr(existing, "id", None),
                    ingestion_job_id=getattr(ingestion_job, "id", None),
                    level="error",
                    message=str(e),
                )
                if on_file_done:
                    on_file_done(name, None, str(e))

        repository.finish_sync_run(
            sync_run=sync_run,
            status="completed",
            new_files=result["new"],
            skipped_files=result["skipped"],
            failed_files=result["failed"],
        )

        self.last_sync = datetime.utcnow()
        self.last_sync_count = result["new"]
        return result

    def get_folder_info(self) -> dict:
        """Get metadata about the Drive folder."""
        svc = self._get_service()
        folder_id = self._get_folder_id()
        if not svc or not folder_id:
            return {}
        try:
            f = svc.files().get(fileId=folder_id, fields="name,id,modifiedTime").execute()
            return f
        except Exception as e:
            logger.error("Drive folder info failed: %s", e)
            return {}

    # ── Private ────────────────────────────────────────────────────────────────

    def _get_service(self):
        # Re-check creds path each time in case it was materialised after init.
        # Only use the cached client if the creds file hasn't changed.
        creds_path = self._get_creds_path()
        if self._service is not None:
            return self._service
        if not Path(creds_path).exists():
            logger.debug("Drive credentials file not found at %s", creds_path)
            return None
        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build

            SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
            creds = service_account.Credentials.from_service_account_file(
                creds_path, scopes=SCOPES
            )
            self._service = build("drive", "v3", credentials=creds, cache_discovery=False)
            logger.info("Google Drive service authenticated")
            return self._service
        except ImportError:
            logger.warning("google-api-python-client not installed. Run: pip install google-api-python-client google-auth")
            return None
        except Exception as e:
            logger.error("Drive auth failed: %s", e)
            return None

    def _download_file(self, file_id: str, mime_type: str | None) -> bytes:
        from googleapiclient.http import MediaIoBaseDownload

        svc = self._get_service()
        if mime_type and mime_type.startswith("application/vnd.google-apps."):
            request = svc.files().export(fileId=file_id, mimeType="application/pdf")
        else:
            request = svc.files().get_media(fileId=file_id)

        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request, chunksize=4 * 1024 * 1024)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()

    def _ensure_pdf_for_sync(
        self,
        file_bytes: bytes,
        filename: str,
        mime_type: str | None = None,
    ) -> tuple[bytes, str]:
        """Convert supported non-PDF Drive files to PDF before saving locally."""
        suffix = Path(filename).suffix.lower()
        if suffix == ".pdf":
            return file_bytes, filename

        if mime_type and mime_type.startswith("application/vnd.google-apps."):
            output_name = f"{Path(filename).stem}.pdf"
            return file_bytes, output_name

        from app.services.pdf_service import pdf_service

        try:
            return pdf_service.convert_to_pdf_bytes(file_bytes=file_bytes, filename=filename)
        except Exception as e:
            raise RuntimeError(f"Drive file conversion failed for '{filename}': {e}") from e

    @staticmethod
    def _parse_folder_id(raw_value: str | None) -> str | None:
        if not raw_value:
            return None
        value = raw_value.strip()

        try:
            parsed = urlparse(value)
            if parsed.scheme in ("http", "https") and parsed.netloc:
                # Example: https://drive.google.com/drive/folders/<id>
                path_parts = parsed.path.strip("/").split("/")
                if "folders" in path_parts:
                    idx = path_parts.index("folders")
                    if idx + 1 < len(path_parts):
                        return path_parts[idx + 1]

                # Example: https://drive.google.com/open?id=<id>
                query = parse_qs(parsed.query)
                folder_ids = query.get("id") or query.get("folderid") or query.get("folder_id")
                if folder_ids:
                    return folder_ids[0]

                # Example: shared link may include /folders/<id>?usp=sharing
                if len(path_parts) == 2 and path_parts[0] == "folders":
                    return path_parts[1]

            # Fall back to raw ID if the value looks like a Drive folder ID
            if value and all(c.isalnum() or c in ("-", "_") for c in value):
                return value
        except Exception:
            pass
        return None

    @staticmethod
    def _parse_drive_time(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            return datetime.fromisoformat(value)
        except Exception:
            return None

    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        safe = "".join(
            c if c.isalnum() or c in (".", "-", "_") else "_"
            for c in filename
        )
        while "__" in safe:
            safe = safe.replace("__", "_")
        return safe.strip("_") or "document.pdf"


drive_service = DriveService()