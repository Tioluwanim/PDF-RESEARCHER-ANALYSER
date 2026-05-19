"""
drive_service.py - Google Drive folder sync for library PDF ingestion.

OAuth-first design:
- Uses Google OAuth web application credentials.
- Automatically reads/writes OAuth client JSON from Streamlit secrets/env.
- Persists the OAuth token locally after the first sign-in.
- Falls back to service-account auth if a credentials JSON is present.

This service watches a shared Drive folder and downloads new/changed supported
documents automatically.
"""

from __future__ import annotations

import io
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import parse_qs, urlparse

from app.config import (
    BASE_DIR,
    UPLOAD_DIR,
    GOOGLE_CREDENTIALS_PATH,
    GOOGLE_OAUTH_CLIENT_PATH,
    GOOGLE_OAUTH_TOKEN_PATH,
    GOOGLE_OAUTH_REDIRECT_URI,
)
from app.db.repository import repository
from app.models.schemas import DocumentStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)

SUPPORTED_DRIVE_MIME_TYPES = {
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
}

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
GOOGLE_DRIVE_READONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"


@dataclass(frozen=True)
class DriveAuthState:
    status: str
    message: str
    folder_id: str | None = None
    auth_mode: str | None = None
    authorization_url: str | None = None


class DriveService:
    """
    Google Drive sync service.

    Auth modes:
    - oauth_user (recommended): user signs in once, token is stored locally.
    - service_account fallback: only works if the folder/shared drive is shared
      with the service account.

    Required:
    - GOOGLE_DRIVE_FOLDER_ID
    - GOOGLE_OAUTH_CLIENT_PATH (or GOOGLE_OAUTH_CLIENT_JSON in secrets/env)
    - GOOGLE_OAUTH_REDIRECT_URI
      OR
    - GOOGLE_CREDENTIALS_PATH for service-account fallback
    """

    def __init__(self) -> None:
        self._service = None
        self._service_mode: str | None = None
        self.upload_dir = UPLOAD_DIR
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.last_sync: datetime | None = None
        self.last_sync_count: int = 0
        self._last_error: str | None = None
        logger.info("DriveService initialised")

    # ────────────────────────────────────────────────────────────────────────
    # Basic config
    # ────────────────────────────────────────────────────────────────────────

    def _get_folder_id(self) -> str | None:
        raw = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
        return self._parse_drive_id(raw or None)

    def _get_oauth_client_path(self) -> Path:
        raw = os.getenv("GOOGLE_OAUTH_CLIENT_PATH", GOOGLE_OAUTH_CLIENT_PATH).strip()
        path = Path(raw)
        return path if path.is_absolute() else BASE_DIR / path

    def _get_oauth_token_path(self) -> Path:
        raw = os.getenv("GOOGLE_OAUTH_TOKEN_PATH", GOOGLE_OAUTH_TOKEN_PATH).strip()
        path = Path(raw)
        return path if path.is_absolute() else BASE_DIR / path

    def _get_oauth_redirect_uri(self) -> str:
        return os.getenv("GOOGLE_OAUTH_REDIRECT_URI", GOOGLE_OAUTH_REDIRECT_URI).strip()

    def _get_service_account_path(self) -> Path:
        raw = os.getenv("GOOGLE_CREDENTIALS_PATH", GOOGLE_CREDENTIALS_PATH).strip()
        path = Path(raw)
        return path if path.is_absolute() else BASE_DIR / path

    def _oauth_client_config_exists(self) -> bool:
        return self._get_oauth_client_path().exists()

    def _oauth_token_exists(self) -> bool:
        return self._get_oauth_token_path().exists()

    def _service_account_exists(self) -> bool:
        return self._get_service_account_path().exists()

    @property
    def folder_id(self) -> str | None:
        return self._get_folder_id()

    @property
    def is_configured(self) -> bool:
        """
        True when there is enough Drive config for the UI to consider the
        integration available.
        """
        folder = self._get_folder_id()
        if not folder:
            return False

        return any(
            (
                self._oauth_client_config_exists(),
                self._oauth_token_exists(),
                self._service_account_exists(),
            )
        )

    def get_auth_state(self) -> DriveAuthState:
        folder_id = self._get_folder_id()

        if not folder_id:
            return DriveAuthState(
                status="missing_folder",
                message="GOOGLE_DRIVE_FOLDER_ID is missing.",
            )

        if self._service is not None:
            return DriveAuthState(
                status="ready",
                message="Google Drive service authenticated.",
                folder_id=folder_id,
                auth_mode=self._service_mode,
            )

        if self._oauth_token_exists() and self._oauth_client_config_exists():
            return DriveAuthState(
                status="ready",
                message="OAuth token found and ready.",
                folder_id=folder_id,
                auth_mode="oauth_user",
            )

        if self._oauth_client_config_exists():
            auth_info = self.get_authorization_url()
            return DriveAuthState(
                status="oauth_login_required",
                message="OAuth client is configured, but the user still needs to sign in.",
                folder_id=folder_id,
                auth_mode="oauth_user",
                authorization_url=auth_info.get("authorization_url"),
            )

        if self._service_account_exists():
            return DriveAuthState(
                status="ready",
                message="Service-account credentials found and ready.",
                folder_id=folder_id,
                auth_mode="service_account",
            )

        return DriveAuthState(
            status="missing_credentials",
            message=(
                "Google Drive auth is not configured. "
                "Provide OAuth client credentials or service-account credentials."
            ),
            folder_id=folder_id,
        )

    def set_folder_id(self, folder_id: str) -> None:
        parsed = self._parse_drive_id(folder_id)
        if parsed:
            os.environ["GOOGLE_DRIVE_FOLDER_ID"] = parsed

    def set_credentials_path(self, path: str) -> None:
        os.environ["GOOGLE_CREDENTIALS_PATH"] = path

    # ────────────────────────────────────────────────────────────────────────
    # OAuth helpers
    # ────────────────────────────────────────────────────────────────────────

    def _load_oauth_client_config(self) -> dict | None:
        """
        Load OAuth client JSON from the materialised file or env.
        """
        client_path = self._get_oauth_client_path()

        if client_path.exists():
            try:
                data = json.loads(client_path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
            except Exception as e:
                logger.error("Failed to read OAuth client JSON: %s", e)

        raw = os.getenv("GOOGLE_OAUTH_CLIENT_JSON", "").strip()
        if raw:
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    return data
            except Exception as e:
                logger.error("Failed to parse GOOGLE_OAUTH_CLIENT_JSON: %s", e)

        return None

    def get_authorization_url(self) -> dict:
        """
        Return an OAuth consent URL for a one-time Google sign-in.
        """
        client_config = self._load_oauth_client_config()
        redirect_uri = self._get_oauth_redirect_uri()

        if not client_config:
            return {
                "error": (
                    "OAuth client JSON not found. "
                    "Set GOOGLE_OAUTH_CLIENT_PATH or GOOGLE_OAUTH_CLIENT_JSON."
                )
            }

        if not redirect_uri:
            return {
                "error": (
                    "GOOGLE_OAUTH_REDIRECT_URI is missing. "
                    "Set it to your local callback URL or deployed app URL."
                )
            }

        try:
            from google_auth_oauthlib.flow import Flow
        except ImportError:
            return {
                "error": (
                    "google-auth-oauthlib is not installed. "
                    "Add google-auth-oauthlib to requirements.txt."
                )
            }

        try:
            flow = Flow.from_client_config(
                client_config,
                scopes=[GOOGLE_DRIVE_READONLY_SCOPE],
                redirect_uri=redirect_uri,
            )
            authorization_url, state = flow.authorization_url(
                access_type="offline",
                include_granted_scopes="true",
                prompt="consent",
            )
            return {
                "authorization_url": authorization_url,
                "state": state,
            }
        except Exception as e:
            logger.error("Failed to build OAuth authorization URL: %s", e)
            return {"error": str(e)}

    def exchange_authorization_code(self, authorization_code: str) -> dict:
        """
        Exchange an OAuth authorization code for a token and persist it.
        """
        client_config = self._load_oauth_client_config()
        redirect_uri = self._get_oauth_redirect_uri()

        if not client_config:
            return {"error": "OAuth client JSON missing."}

        if not redirect_uri:
            return {"error": "GOOGLE_OAUTH_REDIRECT_URI missing."}

        try:
            from google_auth_oauthlib.flow import Flow
        except ImportError:
            return {"error": "google-auth-oauthlib is not installed."}

        try:
            flow = Flow.from_client_config(
                client_config,
                scopes=[GOOGLE_DRIVE_READONLY_SCOPE],
                redirect_uri=redirect_uri,
            )
            flow.fetch_token(code=authorization_code)
            creds = flow.credentials

            token_path = self._get_oauth_token_path()
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text(creds.to_json(), encoding="utf-8")

            # Reset cached client so the new token is used on next call.
            self._service = None
            self._service_mode = None

            return {
                "success": True,
                "token_path": str(token_path),
            }
        except Exception as e:
            logger.error("OAuth token exchange failed: %s", e)
            return {"error": str(e)}

    # ────────────────────────────────────────────────────────────────────────
    # Public API
    # ────────────────────────────────────────────────────────────────────────

    def list_drive_files(self) -> list[dict]:
        """
        List all supported files inside the configured Drive folder.
        If the configured ID is a file, return it as a one-item list.
        """
        svc = self._get_service()
        folder_id = self._get_folder_id()

        if not svc or not folder_id:
            return []

        resource = self._get_drive_resource_info(folder_id)
        if not resource:
            return []

        if resource.get("mimeType") != FOLDER_MIME_TYPE:
            if self._is_supported_drive_file(resource):
                return [resource]
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
                            fields=(
                                "nextPageToken, files("
                                "id, name, mimeType, modifiedTime, md5Checksum, size"
                                ")"
                            ),
                            pageSize=200,
                            pageToken=page_token,
                            includeItemsFromAllDrives=True,
                            supportsAllDrives=True,
                        )
                        .execute()
                    )

                    for item in response.get("files", []):
                        mime_type = item.get("mimeType")
                        if mime_type == FOLDER_MIME_TYPE:
                            folders.append(item["id"])
                        elif self._is_supported_drive_file(item):
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
        Sync Drive folder -> local.

        Returns:
            {
                "new": int,
                "updated": int,
                "skipped": int,
                "failed": int,
                "total": int,
                "auth_mode": str | None
            }
        """
        auth_state = self.get_auth_state()
        if auth_state.status not in {"ready", "oauth_login_required"}:
            return {
                "error": auth_state.message,
                "auth_mode": auth_state.auth_mode,
                "authorization_url": auth_state.authorization_url,
                "new": 0,
                "updated": 0,
                "skipped": 0,
                "failed": 0,
                "total": 0,
            }

        # If OAuth is configured but not yet authorised, stop here with a
        # helpful response rather than pretending config is missing.
        if auth_state.status == "oauth_login_required":
            return {
                "error": auth_state.message,
                "auth_mode": auth_state.auth_mode,
                "authorization_url": auth_state.authorization_url,
                "new": 0,
                "updated": 0,
                "skipped": 0,
                "failed": 0,
                "total": 0,
            }

        files = self.list_drive_files()
        folder_id = self._get_folder_id() or ""
        sync_run = repository.create_sync_run(folder_id, len(files))

        result = {
            "new": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "total": len(files),
            "auth_mode": auth_state.auth_mode,
        }

        try:
            for idx, item in enumerate(files):
                file_id = item["id"]
                name = item.get("name", file_id)
                md5 = item.get("md5Checksum", "")
                size = int(item.get("size", 0) or 0)
                modified_time = self._parse_drive_time(item.get("modifiedTime"))
                mime_type = item.get("mimeType")

                if on_file_found:
                    on_file_found(name, idx + 1, len(files))

                existing = repository.get_document_by_drive_file_id(file_id)

                existing_doc_id = getattr(existing, "doc_id", None) if existing is not None else None
                existing_pk = getattr(existing, "id", None) if existing is not None else None
                existing_checksum = getattr(existing, "checksum", None) if existing is not None else None
                existing_modified = getattr(existing, "modified_time", None) if existing is not None else None

                if (
                    existing is not None
                    and existing_checksum == md5
                    and existing_modified == modified_time
                ):
                    result["skipped"] += 1
                    if on_file_done:
                        on_file_done(name, None, "skipped")
                    continue

                ingestion_job = None
                try:
                    file_bytes = self._download_file(file_id, mime_type)
                    file_bytes, output_name = self._ensure_pdf_for_sync(
                        file_bytes=file_bytes,
                        filename=name,
                        mime_type=mime_type,
                    )

                    safe_name = self._sanitize_filename(output_name)
                    dest_path = self.upload_dir / f"{file_id}_{safe_name}"
                    dest_path.write_bytes(file_bytes)

                    if existing is not None and existing_doc_id:
                        repository.update_document_file(
                            doc_id=existing_doc_id,
                            local_path=str(dest_path),
                            checksum=md5,
                            modified_time=modified_time,
                            file_size_bytes=size if size > 0 else len(file_bytes),
                            status=DocumentStatus.UPLOADED,
                            source="drive",
                            source_folder=folder_id,
                            drive_file_id=file_id,
                        )
                        result["updated"] += 1
                    else:
                        created = repository.create_document(
                            doc_id=str(uuid.uuid4()),
                            filename=name,
                            local_path=str(dest_path),
                            file_size_bytes=size if size > 0 else len(file_bytes),
                            mime_type="application/pdf",
                            source_folder=folder_id,
                            drive_file_id=file_id,
                            checksum=md5,
                            modified_time=modified_time,
                            source="drive",
                            status=DocumentStatus.UPLOADED,
                        )
                        existing_pk = getattr(created, "id", None)
                        result["new"] += 1

                    ingestion_job = repository.create_ingestion_job(
                        document_id=existing_pk,
                        drive_file_id=file_id,
                        source="drive",
                    )

                    if on_file_done:
                        on_file_done(name, file_bytes, None)

                except Exception as e:
                    result["failed"] += 1
                    logger.error("Failed to sync %s: %s", name, e)
                    repository.add_processing_log(
                        document_id=existing_pk,
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

        except Exception as e:
            logger.error("Sync run failed: %s", e)
            repository.finish_sync_run(
                sync_run=sync_run,
                status="failed",
                new_files=result["new"],
                skipped_files=result["skipped"],
                failed_files=result["failed"] + 1,
            )
            raise

        self.last_sync = datetime.now(timezone.utc)
        self.last_sync_count = result["new"] + result["updated"]
        return result

    def get_folder_info(self) -> dict:
        """
        Get metadata about the configured Drive folder or file.
        """
        svc = self._get_service()
        folder_id = self._get_folder_id()

        if not svc or not folder_id:
            message = "Drive service is not configured."
            logger.warning(message)
            return {"error": message}

        try:
            info = svc.files().get(
                fileId=folder_id,
                fields="name,id,mimeType,modifiedTime",
                supportsAllDrives=True,
            ).execute()
            return info

        except Exception as e:
            error_message = str(e)
            status_code = None

            try:
                from googleapiclient.errors import HttpError
                if isinstance(e, HttpError):
                    status_code = getattr(e.resp, "status", None)
            except Exception:
                pass

            if status_code == 404:
                error_message = (
                    "Resource not found. If you are using a service account, "
                    "share the folder/shared drive with it, or use OAuth user auth."
                )

            logger.warning("Drive folder info failed: %s", error_message)
            return {"error": error_message, "status_code": status_code}

    # ────────────────────────────────────────────────────────────────────────
    # Internal auth / file helpers
    # ────────────────────────────────────────────────────────────────────────

    def _get_service(self):
        if self._service is not None:
            return self._service

        try:
            from google.auth.transport.requests import Request
            from google.oauth2.credentials import Credentials as UserCredentials
        except ImportError:
            Request = None  # type: ignore[assignment]
            UserCredentials = None  # type: ignore[assignment]

        oauth_token_path = self._get_oauth_token_path()
        oauth_client_path = self._get_oauth_client_path()
        service_account_path = self._get_service_account_path()

        try:
            # 1) OAuth user token
            if UserCredentials is not None and oauth_token_path.exists() and oauth_client_path.exists():
                creds = UserCredentials.from_authorized_user_file(
                    str(oauth_token_path),
                    scopes=[GOOGLE_DRIVE_READONLY_SCOPE],
                )

                if creds and creds.expired and creds.refresh_token and Request is not None:
                    creds.refresh(Request())
                    oauth_token_path.write_text(creds.to_json(), encoding="utf-8")

                self._service = self._build_drive_client(creds)
                self._service_mode = "oauth_user"
                logger.info("Google Drive service authenticated via OAuth user token")
                return self._service

            # 2) Service account fallback
            if service_account_path.exists():
                from google.oauth2 import service_account

                creds = service_account.Credentials.from_service_account_file(
                    str(service_account_path),
                    scopes=[GOOGLE_DRIVE_READONLY_SCOPE],
                )
                self._service = self._build_drive_client(creds)
                self._service_mode = "service_account"
                logger.info("Google Drive service authenticated via service account")
                return self._service

            # 3) OAuth client present but no token yet
            if oauth_client_path.exists():
                self._last_error = "OAuth login required"
                logger.debug("OAuth client exists but token is missing")
                return None

            self._last_error = "No valid Drive credentials found"
            logger.debug("No Drive credentials found")
            return None

        except ImportError as e:
            self._last_error = str(e)
            logger.warning("Drive auth imports missing: %s", e)
            return None
        except Exception as e:
            self._last_error = str(e)
            logger.error("Drive auth failed: %s", e)
            return None

    def _build_drive_client(self, creds):
        from googleapiclient.discovery import build

        return build("drive", "v3", credentials=creds, cache_discovery=False)

    def _get_drive_resource_info(self, file_id: str) -> dict | None:
        svc = self._get_service()
        if not svc:
            return None

        try:
            return svc.files().get(
                fileId=file_id,
                fields="id,name,mimeType,modifiedTime,md5Checksum,size",
                supportsAllDrives=True,
            ).execute()

        except Exception as e:
            logger.error("Drive resource info failed for %s: %s", file_id, e)
            return None

    def _is_supported_drive_file(self, item: dict) -> bool:
        mime_type = item.get("mimeType")
        return bool(
            mime_type in SUPPORTED_DRIVE_MIME_TYPES
            or (
                isinstance(mime_type, str)
                and mime_type.startswith("application/vnd.google-apps.")
            )
        )

    def get_service_account_email(self) -> str | None:
        """
        Return the email address from the service-account credentials JSON, if present.
        """
        creds_path = self._get_service_account_path()
        if not creds_path.exists():
            return None

        try:
            data = json.loads(creds_path.read_text(encoding="utf-8"))
            return data.get("client_email")
        except Exception as e:
            logger.debug("Failed to read service account email: %s", e)
            return None

    def _download_file(self, file_id: str, mime_type: str | None) -> bytes:
        from googleapiclient.http import MediaIoBaseDownload

        svc = self._get_service()
        if svc is None:
            raise RuntimeError("Google Drive client is not available.")

        if mime_type and mime_type.startswith("application/vnd.google-apps."):
            request = svc.files().export(
                fileId=file_id,
                mimeType="application/pdf",
                supportsAllDrives=True,
            )
        else:
            request = svc.files().get_media(
                fileId=file_id,
                supportsAllDrives=True,
            )

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
        """
        Convert supported non-PDF Drive files to PDF before saving locally.
        """
        suffix = Path(filename).suffix.lower()
        if suffix == ".pdf":
            return file_bytes, filename

        if mime_type and mime_type.startswith("application/vnd.google-apps."):
            return file_bytes, f"{Path(filename).stem}.pdf"

        from app.services.pdf_service import pdf_service

        try:
            return pdf_service.convert_to_pdf_bytes(
                file_bytes=file_bytes,
                filename=filename,
            )
        except Exception as e:
            raise RuntimeError(
                f"Drive file conversion failed for '{filename}': {e}"
            ) from e

    @staticmethod
    def _parse_drive_id(raw_value: str | None) -> str | None:
        if not raw_value:
            return None

        value = raw_value.strip()

        try:
            parsed = urlparse(value)

            if parsed.scheme in ("http", "https") and parsed.netloc:
                path_parts = parsed.path.strip("/").split("/")
                query = parse_qs(parsed.query)

                if "folders" in path_parts:
                    idx = path_parts.index("folders")
                    if idx + 1 < len(path_parts):
                        return path_parts[idx + 1]

                ids = query.get("id") or query.get("folderid") or query.get("folder_id")
                if ids:
                    return ids[0]

                if len(path_parts) == 2 and path_parts[0] == "folders":
                    return path_parts[1]

                if "file" in path_parts and "d" in path_parts:
                    idx = path_parts.index("d")
                    if idx + 1 < len(path_parts):
                        return path_parts[idx + 1]

                if len(path_parts) >= 3 and path_parts[1] == "d":
                    return path_parts[2]

            if all(c.isalnum() or c in ("-", "_") for c in value):
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