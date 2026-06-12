"""
multi_drive_service.py — Multi-account, parallel Google Drive sync.

Improvements over the single-account drive_service.py:
  - Manages N independent Drive accounts, each with its own OAuth token and
    optional folder ID.  Accounts are stored in session_state + disk.
  - Parallel download pool: downloads up to DRIVE_DOWNLOAD_WORKERS files
    simultaneously using concurrent.futures.ThreadPoolExecutor, cutting sync
    time on large folders by ~N× where N = worker count.
  - Chunked streaming download with configurable chunk size (default 8 MB)
    so memory stays flat even for 100-file batches.
  - Per-account auth state: each account independently tracks OAuth flow,
    token expiry, and service-account fallback.
  - Aggregate sync across all configured accounts in one call, deduplicating
    by Drive file-ID so the same file in two folders isn't double-processed.
  - Retry logic: each download retried up to DRIVE_DOWNLOAD_RETRIES times
    with exponential back-off before being counted as a failure.
  - Graceful degradation: if one account fails to authenticate or a folder
    is unreachable, the others continue and the error is surfaced per-account.
"""

from __future__ import annotations

import io
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import parse_qs, urlparse

from app.config import BASE_DIR, UPLOAD_DIR
from app.db.repository import repository
from app.models.schemas import DocumentStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Tuning constants ──────────────────────────────────────────────────────────
DRIVE_DOWNLOAD_WORKERS  = int(os.getenv("DRIVE_DOWNLOAD_WORKERS",  "6"))
DRIVE_DOWNLOAD_CHUNK_MB = int(os.getenv("DRIVE_DOWNLOAD_CHUNK_MB", "8"))
DRIVE_DOWNLOAD_RETRIES  = int(os.getenv("DRIVE_DOWNLOAD_RETRIES",  "3"))
DRIVE_ACCOUNTS_FILE     = BASE_DIR / "data" / "drive_accounts.json"

SUPPORTED_MIME_TYPES = {
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
FOLDER_MIME  = "application/vnd.google-apps.folder"
DRIVE_SCOPE  = "https://www.googleapis.com/auth/drive.readonly"


# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class DriveAccount:
    """Represents a single connected Google Drive account / folder."""
    account_id  : str           # stable UUID assigned at registration
    label       : str           # human-readable name set by user
    folder_id   : str           # Drive folder (or file) ID to sync
    token_path  : str           # path to persisted OAuth token JSON
    client_path : str           # path to OAuth client JSON
    sa_path     : str           # path to service-account JSON (fallback)
    enabled     : bool = True

    def to_dict(self) -> dict:
        return {
            "account_id"  : self.account_id,
            "label"       : self.label,
            "folder_id"   : self.folder_id,
            "token_path"  : self.token_path,
            "client_path" : self.client_path,
            "sa_path"     : self.sa_path,
            "enabled"     : self.enabled,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "DriveAccount":
        return cls(
            account_id  = d["account_id"],
            label       = d.get("label", d["account_id"]),
            folder_id   = d.get("folder_id", ""),
            token_path  = d.get("token_path", ""),
            client_path = d.get("client_path", ""),
            sa_path     = d.get("sa_path", ""),
            enabled     = d.get("enabled", True),
        )


@dataclass
class AccountSyncResult:
    account_id : str
    label      : str
    new        : int = 0
    updated    : int = 0
    skipped    : int = 0
    failed     : int = 0
    total      : int = 0
    error      : str = ""
    auth_mode  : str = ""
    duration_s : float = 0.0


@dataclass
class MultiSyncResult:
    accounts       : list[AccountSyncResult] = field(default_factory=list)
    total_new      : int = 0
    total_updated  : int = 0
    total_skipped  : int = 0
    total_failed   : int = 0
    total_files    : int = 0
    duration_s     : float = 0.0

    def add(self, r: AccountSyncResult) -> None:
        self.accounts.append(r)
        self.total_new     += r.new
        self.total_updated += r.updated
        self.total_skipped += r.skipped
        self.total_failed  += r.failed
        self.total_files   += r.total


# ═══════════════════════════════════════════════════════════════════════════════
class _AccountClient:
    """
    Wraps the Google Drive API client for a single DriveAccount.
    Handles auth, listing, and streaming download with retry.
    """

    def __init__(self, account: DriveAccount) -> None:
        self.account  = account
        self._svc     = None
        self._mode    : str = ""

    # ── Auth ──────────────────────────────────────────────────────────────────

    def get_service(self):
        if self._svc is not None:
            return self._svc

        token_path  = Path(self.account.token_path)
        client_path = Path(self.account.client_path)
        sa_path     = Path(self.account.sa_path)

        try:
            from google.auth.transport.requests import Request
            from google.oauth2.credentials import Credentials

            # 1 — OAuth user token
            if token_path.exists() and client_path.exists():
                creds = Credentials.from_authorized_user_file(
                    str(token_path), scopes=[DRIVE_SCOPE],
                )
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                    token_path.write_text(creds.to_json(), encoding="utf-8")
                self._svc  = self._build(creds)
                self._mode = "oauth_user"
                return self._svc

            # 2 — Service account
            if sa_path.exists():
                from google.oauth2 import service_account
                creds = service_account.Credentials.from_service_account_file(
                    str(sa_path), scopes=[DRIVE_SCOPE],
                )
                self._svc  = self._build(creds)
                self._mode = "service_account"
                return self._svc

        except Exception as e:
            logger.error("[%s] Auth failed: %s", self.account.label, e)

        return None

    @staticmethod
    def _build(creds):
        from googleapiclient.discovery import build
        return build("drive", "v3", credentials=creds, cache_discovery=False)

    @property
    def auth_mode(self) -> str:
        return self._mode

    def get_auth_url(self) -> str | None:
        """Return OAuth consent URL if client JSON exists but no token yet."""
        client_path = Path(self.account.client_path)
        if not client_path.exists():
            return None

        try:
            from requests_oauthlib import OAuth2Session
            cfg        = json.loads(client_path.read_text())
            info       = cfg.get("web") or cfg.get("installed") or cfg
            client_id  = info["client_id"]
            auth_uri   = info.get("auth_uri", "https://accounts.google.com/o/oauth2/auth")
            redirect   = self._redirect_uri()
            oauth      = OAuth2Session(
                client_id=client_id,
                redirect_uri=redirect,
                scope=[DRIVE_SCOPE],
            )
            url, _ = oauth.authorization_url(
                auth_uri, access_type="offline", prompt="consent",
            )
            return url
        except Exception as e:
            logger.warning("[%s] Could not build auth URL: %s", self.account.label, e)
            return None

    def exchange_code(self, code: str) -> bool:
        """Exchange OAuth code for token, persist to token_path. Returns success."""
        client_path = Path(self.account.client_path)
        if not client_path.exists():
            return False
        try:
            from requests_oauthlib import OAuth2Session
            from google.oauth2.credentials import Credentials

            cfg        = json.loads(client_path.read_text())
            info       = cfg.get("web") or cfg.get("installed") or cfg
            client_id  = info["client_id"]
            client_sec = info["client_secret"]
            token_uri  = info.get("token_uri", "https://oauth2.googleapis.com/token")
            redirect   = self._redirect_uri()

            oauth = OAuth2Session(
                client_id=client_id, redirect_uri=redirect, scope=[DRIVE_SCOPE],
            )
            token = oauth.fetch_token(
                token_uri, code=code,
                client_secret=client_sec, include_client_id=True,
            )
            creds = Credentials(
                token=token.get("access_token"),
                refresh_token=token.get("refresh_token"),
                token_uri=token_uri, client_id=client_id,
                client_secret=client_sec, scopes=[DRIVE_SCOPE],
            )
            token_p = Path(self.account.token_path)
            token_p.parent.mkdir(parents=True, exist_ok=True)
            token_p.write_text(creds.to_json(), encoding="utf-8")
            self._svc  = None   # force re-auth with new token
            self._mode = ""
            return True
        except Exception as e:
            logger.error("[%s] Code exchange failed: %s", self.account.label, e)
            return False

    @staticmethod
    def _redirect_uri() -> str:
        explicit = os.getenv("GOOGLE_OAUTH_REDIRECT_URI", "").strip()
        if explicit:
            return explicit
        try:
            import streamlit as st
            ctx_url = getattr(getattr(st, "context", None), "url", None)
            if ctx_url:
                p = urlparse(ctx_url)
                return f"{p.scheme}://{p.netloc}/"
        except Exception:
            pass
        return "http://localhost:8501/"

    # ── Listing ───────────────────────────────────────────────────────────────

    def list_files(self) -> list[dict]:
        svc = self.get_service()
        if not svc or not self.account.folder_id:
            return []

        try:
            root_info = svc.files().get(
                fileId=self.account.folder_id,
                fields="id,name,mimeType",
                supportsAllDrives=True,
            ).execute()
        except Exception as e:
            logger.error("[%s] list_files root lookup failed: %s", self.account.label, e)
            return []

        if root_info.get("mimeType") != FOLDER_MIME:
            # Single file
            if self._is_supported(root_info):
                return [self._enrich(root_info)]
            return []

        files: list[dict] = []
        queue = [self.account.folder_id]

        while queue:
            parent = queue.pop(0)
            page_token = None
            while True:
                try:
                    resp = svc.files().list(
                        q=f"'{parent}' in parents and trashed=false",
                        fields=(
                            "nextPageToken, files("
                            "id, name, mimeType, modifiedTime, md5Checksum, size)"
                        ),
                        pageSize=1000,   # max allowed by API
                        pageToken=page_token,
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True,
                        orderBy="name",
                    ).execute()
                except Exception as e:
                    logger.error("[%s] list_files page failed: %s", self.account.label, e)
                    break

                for item in resp.get("files", []):
                    if item.get("mimeType") == FOLDER_MIME:
                        queue.append(item["id"])
                    elif self._is_supported(item):
                        files.append(self._enrich(item))

                page_token = resp.get("nextPageToken")
                if not page_token:
                    break

        return files

    # ── Download ──────────────────────────────────────────────────────────────

    def download(self, file_id: str, mime_type: str | None) -> bytes:
        """
        Download a single file with retries and chunked streaming.
        Raises RuntimeError after DRIVE_DOWNLOAD_RETRIES failed attempts.
        """
        last_err: Exception | None = None
        chunk_bytes = DRIVE_DOWNLOAD_CHUNK_MB * 1024 * 1024

        for attempt in range(1, DRIVE_DOWNLOAD_RETRIES + 1):
            try:
                return self._download_once(file_id, mime_type, chunk_bytes)
            except Exception as e:
                last_err = e
                wait = 2 ** (attempt - 1)
                logger.warning(
                    "[%s] Download %s attempt %d/%d failed: %s — retrying in %ds",
                    self.account.label, file_id, attempt, DRIVE_DOWNLOAD_RETRIES, e, wait,
                )
                if attempt < DRIVE_DOWNLOAD_RETRIES:
                    time.sleep(wait)

        raise RuntimeError(
            f"Download failed after {DRIVE_DOWNLOAD_RETRIES} attempts: {last_err}"
        )

    def _download_once(self, file_id: str, mime_type: str | None, chunk_bytes: int) -> bytes:
        from googleapiclient.http import MediaIoBaseDownload
        svc = self.get_service()
        if svc is None:
            raise RuntimeError("Drive service not available")

        if mime_type and mime_type.startswith("application/vnd.google-apps."):
            request = svc.files().export(
                fileId=file_id, mimeType="application/pdf",
                supportsAllDrives=True,
            )
        else:
            request = svc.files().get_media(
                fileId=file_id, supportsAllDrives=True,
            )

        buf = io.BytesIO()
        dl  = MediaIoBaseDownload(buf, request, chunksize=chunk_bytes)
        done = False
        while not done:
            _, done = dl.next_chunk()
        return buf.getvalue()

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _is_supported(item: dict) -> bool:
        mime = item.get("mimeType", "")
        return bool(
            mime in SUPPORTED_MIME_TYPES
            or (isinstance(mime, str) and mime.startswith("application/vnd.google-apps."))
        )

    def _enrich(self, item: dict) -> dict:
        """Tag each file with which account it came from."""
        item = dict(item)
        item["_account_id"] = self.account.account_id
        item["_label"]      = self.account.label
        return item


# ═══════════════════════════════════════════════════════════════════════════════
class MultiDriveService:
    """
    Manages multiple Google Drive accounts and syncs them in parallel.

    Public API
    ----------
    add_account(label, folder_id, client_path, sa_path) -> DriveAccount
    remove_account(account_id)
    list_accounts() -> list[DriveAccount]
    get_auth_url(account_id) -> str | None
    exchange_code(account_id, code) -> bool
    sync_all(on_progress, on_file_done) -> MultiSyncResult
    sync_account(account_id, on_progress, on_file_done) -> AccountSyncResult
    """

    def __init__(self) -> None:
        self._accounts   : dict[str, DriveAccount] = {}
        self._clients    : dict[str, _AccountClient] = {}
        self.upload_dir  = UPLOAD_DIR
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self._load_accounts()
        logger.info("MultiDriveService initialised — %d accounts", len(self._accounts))

    # ── Account management ────────────────────────────────────────────────────

    def add_account(
        self,
        label       : str,
        folder_id   : str,
        client_path : str = "",
        sa_path     : str = "",
    ) -> DriveAccount:
        """
        Register a new Drive account.
        A unique token path is auto-assigned under data/ so tokens never collide.
        """
        account_id = str(uuid.uuid4())
        data_dir   = BASE_DIR / "data"
        token_path = str(data_dir / f"drive_token_{account_id[:8]}.json")

        account = DriveAccount(
            account_id  = account_id,
            label       = label.strip() or f"Drive {len(self._accounts) + 1}",
            folder_id   = self._parse_folder_id(folder_id),
            token_path  = token_path,
            client_path = client_path or str(data_dir / "google_oauth_client.json"),
            sa_path     = sa_path or str(BASE_DIR / "credentials.json"),
            enabled     = True,
        )
        self._accounts[account_id] = account
        self._clients[account_id]  = _AccountClient(account)
        self._save_accounts()
        logger.info("Added Drive account '%s' (%s)", account.label, account_id[:8])
        return account

    def remove_account(self, account_id: str) -> bool:
        if account_id not in self._accounts:
            return False
        label = self._accounts[account_id].label
        del self._accounts[account_id]
        self._clients.pop(account_id, None)
        self._save_accounts()
        logger.info("Removed Drive account '%s'", label)
        return True

    def update_account(self, account_id: str, **kwargs) -> bool:
        if account_id not in self._accounts:
            return False
        acc = self._accounts[account_id]
        for k, v in kwargs.items():
            if hasattr(acc, k):
                if k == "folder_id":
                    v = self._parse_folder_id(v)
                object.__setattr__(acc, k, v)
        self._save_accounts()
        # Rebuild client with updated config
        self._clients[account_id] = _AccountClient(acc)
        return True

    def set_account_enabled(self, account_id: str, enabled: bool) -> None:
        if account_id in self._accounts:
            self._accounts[account_id].enabled = enabled
            self._save_accounts()

    def list_accounts(self) -> list[DriveAccount]:
        return list(self._accounts.values())

    def get_account(self, account_id: str) -> DriveAccount | None:
        return self._accounts.get(account_id)

    # ── OAuth helpers ─────────────────────────────────────────────────────────

    def get_auth_url(self, account_id: str) -> str | None:
        client = self._clients.get(account_id)
        return client.get_auth_url() if client else None

    def exchange_code(self, account_id: str, code: str) -> bool:
        client = self._clients.get(account_id)
        return client.exchange_code(code) if client else False

    def get_account_status(self, account_id: str) -> dict:
        """Return auth/config status for a single account."""
        acc    = self._accounts.get(account_id)
        client = self._clients.get(account_id)
        if not acc or not client:
            return {"status": "not_found"}

        if not acc.folder_id:
            return {"status": "missing_folder", "label": acc.label}

        token_exists  = Path(acc.token_path).exists()
        client_exists = Path(acc.client_path).exists()
        sa_exists     = Path(acc.sa_path).exists()

        if token_exists and client_exists:
            return {"status": "ready", "auth_mode": "oauth_user",  "label": acc.label}
        if sa_exists:
            return {"status": "ready", "auth_mode": "service_account", "label": acc.label}
        if client_exists:
            auth_url = client.get_auth_url()
            return {
                "status"           : "oauth_login_required",
                "auth_mode"        : "oauth_user",
                "label"            : acc.label,
                "authorization_url": auth_url,
            }
        return {"status": "missing_credentials", "label": acc.label}

    # ── Sync ──────────────────────────────────────────────────────────────────

    def sync_all(
        self,
        on_progress  : Optional[Callable[[str, str, int, int], None]] = None,
        on_file_done : Optional[Callable[[str, str, str | None], None]] = None,
    ) -> MultiSyncResult:
        """
        Sync all enabled accounts.

        on_progress(account_label, filename, current_idx, total)
        on_file_done(account_label, filename, error_or_None)
        """
        t0     = time.monotonic()
        result = MultiSyncResult()

        enabled = [a for a in self._accounts.values() if a.enabled]
        if not enabled:
            logger.warning("MultiDriveService.sync_all: no enabled accounts")
            return result

        for account in enabled:
            logger.info("Syncing account '%s' …", account.label)
            acc_result = self.sync_account(
                account_id   = account.account_id,
                on_progress  = (
                    (lambda lbl: lambda fn, i, t: on_progress(lbl, fn, i, t))(account.label)
                    if on_progress else None
                ),
                on_file_done = (
                    (lambda lbl: lambda fn, err: on_file_done(lbl, fn, err))(account.label)
                    if on_file_done else None
                ),
            )
            result.add(acc_result)

        result.duration_s = round(time.monotonic() - t0, 2)
        logger.info(
            "Multi-sync done — new=%d updated=%d skipped=%d failed=%d in %.1fs",
            result.total_new, result.total_updated,
            result.total_skipped, result.total_failed, result.duration_s,
        )
        return result

    def sync_account(
        self,
        account_id   : str,
        on_progress  : Optional[Callable[[str, int, int], None]] = None,
        on_file_done : Optional[Callable[[str, str | None], None]] = None,
    ) -> AccountSyncResult:
        """
        Sync a single account using a parallel download pool.
        Downloads up to DRIVE_DOWNLOAD_WORKERS files concurrently.
        """
        acc    = self._accounts.get(account_id)
        client = self._clients.get(account_id)
        t0     = time.monotonic()

        r = AccountSyncResult(
            account_id = account_id,
            label      = acc.label if acc else account_id,
        )

        if not acc or not client:
            r.error = "Account not found"
            return r

        status = self.get_account_status(account_id)
        if status["status"] not in ("ready",):
            r.error    = f"Account not ready: {status['status']}"
            r.auth_mode = status.get("auth_mode", "")
            return r

        # 1 — List all files
        try:
            files = client.list_files()
        except Exception as e:
            r.error = str(e)
            logger.error("[%s] list_files failed: %s", acc.label, e)
            return r

        r.total    = len(files)
        r.auth_mode = client.auth_mode

        if not files:
            logger.info("[%s] No files found", acc.label)
            return r

        # 2 — Classify: skip unchanged, queue new/updated
        to_download: list[dict] = []
        for item in files:
            fid      = item["id"]
            existing = repository.get_document_by_drive_file_id(fid)
            if (
                existing is not None
                and existing.checksum == item.get("md5Checksum", "")
                and existing.modified_time == _parse_time(item.get("modifiedTime"))
            ):
                r.skipped += 1
            else:
                to_download.append(item)

        logger.info(
            "[%s] %d files: %d to download, %d skipped",
            acc.label, r.total, len(to_download), r.skipped,
        )

        if not to_download:
            return r

        # 3 — Parallel download + persist
        sync_run = repository.create_sync_run(acc.folder_id, r.total)

        def _do_one(item: dict) -> tuple[dict, bytes | None, str | None]:
            """Download one file. Returns (item, bytes_or_None, error_or_None)."""
            try:
                data = client.download(item["id"], item.get("mimeType"))
                return item, data, None
            except Exception as e:
                return item, None, str(e)

        workers = min(DRIVE_DOWNLOAD_WORKERS, len(to_download))
        done_count = 0

        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="drive_dl") as pool:
            futures = {pool.submit(_do_one, item): item for item in to_download}

            for future in as_completed(futures):
                item, file_bytes, dl_error = future.result()
                done_count += 1
                name      = item.get("name", item["id"])
                fid       = item["id"]
                mime_type = item.get("mimeType", "")
                md5       = item.get("md5Checksum", "")
                size      = int(item.get("size", 0) or 0)
                mod_time  = _parse_time(item.get("modifiedTime"))

                if on_progress:
                    try:
                        on_progress(name, done_count, len(to_download))
                    except Exception:
                        pass

                if dl_error:
                    r.failed += 1
                    logger.error("[%s] Download failed %s: %s", acc.label, name, dl_error)
                    if on_file_done:
                        try:
                            on_file_done(name, dl_error)
                        except Exception:
                            pass
                    continue

                # Convert non-PDF if needed
                try:
                    file_bytes, output_name = _ensure_pdf(file_bytes, name, mime_type)
                except Exception as e:
                    r.failed += 1
                    logger.error("[%s] Conversion failed %s: %s", acc.label, name, e)
                    if on_file_done:
                        try:
                            on_file_done(name, str(e))
                        except Exception:
                            pass
                    continue

                # Persist to disk
                safe  = _sanitize(output_name)
                dest  = self.upload_dir / f"{fid}_{safe}"
                try:
                    dest.write_bytes(file_bytes)
                except OSError as e:
                    r.failed += 1
                    logger.error("[%s] Write failed %s: %s", acc.label, name, e)
                    if on_file_done:
                        try:
                            on_file_done(name, str(e))
                        except Exception:
                            pass
                    continue

                # DB upsert
                existing    = repository.get_document_by_drive_file_id(fid)
                existing_pk = getattr(existing, "id",     None)
                existing_id = getattr(existing, "doc_id", None)

                try:
                    if existing is not None and existing_id:
                        repository.update_document_file(
                            doc_id         = existing_id,
                            local_path     = str(dest),
                            checksum       = md5,
                            modified_time  = mod_time,
                            file_size_bytes= size or len(file_bytes),
                            status         = DocumentStatus.UPLOADED,
                            source         = "drive",
                            source_folder  = acc.folder_id,
                            drive_file_id  = fid,
                        )
                        r.updated += 1
                    else:
                        doc_id  = str(uuid.uuid4())
                        created = repository.create_document(
                            doc_id          = doc_id,
                            filename        = name,
                            local_path      = str(dest),
                            file_size_bytes = size or len(file_bytes),
                            mime_type       = "application/pdf",
                            source_folder   = acc.folder_id,
                            drive_file_id   = fid,
                            checksum        = md5,
                            modified_time   = mod_time,
                            source          = "drive",
                            status          = DocumentStatus.UPLOADED,
                        )
                        existing_pk = getattr(created, "id", None)
                        r.new += 1

                    repository.create_ingestion_job(
                        document_id   = existing_pk,
                        drive_file_id = fid,
                        source        = "drive",
                    )
                except Exception as e:
                    r.failed += 1
                    logger.error("[%s] DB upsert failed %s: %s", acc.label, name, e)
                    if on_file_done:
                        try:
                            on_file_done(name, str(e))
                        except Exception:
                            pass
                    continue

                if on_file_done:
                    try:
                        on_file_done(name, None)
                    except Exception:
                        pass

        repository.finish_sync_run(
            sync_run      = sync_run,
            status        = "completed",
            new_files     = r.new,
            updated_files = r.updated,
            skipped_files = r.skipped,
            failed_files  = r.failed,
        )

        r.duration_s = round(time.monotonic() - t0, 2)
        logger.info(
            "[%s] Sync done — new=%d updated=%d skipped=%d failed=%d in %.1fs",
            acc.label, r.new, r.updated, r.skipped, r.failed, r.duration_s,
        )
        return r

    # ── Persist accounts ──────────────────────────────────────────────────────

    def _save_accounts(self) -> None:
        DRIVE_ACCOUNTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = [a.to_dict() for a in self._accounts.values()]
        DRIVE_ACCOUNTS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def _load_accounts(self) -> None:
        if not DRIVE_ACCOUNTS_FILE.exists():
            # Migrate single-account config from env if present
            self._migrate_legacy()
            return
        try:
            data = json.loads(DRIVE_ACCOUNTS_FILE.read_text(encoding="utf-8"))
            for d in data:
                acc = DriveAccount.from_dict(d)
                self._accounts[acc.account_id] = acc
                self._clients[acc.account_id]  = _AccountClient(acc)
        except Exception as e:
            logger.error("Failed to load drive accounts: %s", e)

    def _migrate_legacy(self) -> None:
        """
        If the old single-account env vars are set, import them as account #1
        so existing deployments don't break.
        """
        folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
        if not folder_id:
            return
        data_dir = BASE_DIR / "data"
        self.add_account(
            label       = "Default Drive",
            folder_id   = folder_id,
            client_path = str(data_dir / "google_oauth_client.json"),
            sa_path     = str(BASE_DIR / "credentials.json"),
        )
        logger.info("Migrated legacy single-account Drive config")

    # ── Utilities ─────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_folder_id(raw: str) -> str:
        if not raw:
            return ""
        raw = raw.strip()
        try:
            parsed = urlparse(raw)
            if parsed.scheme in ("http", "https"):
                parts = parsed.path.strip("/").split("/")
                qs    = parse_qs(parsed.query)
                if "folders" in parts:
                    idx = parts.index("folders")
                    if idx + 1 < len(parts):
                        return parts[idx + 1]
                for key in ("id", "folderid", "folder_id"):
                    if qs.get(key):
                        return qs[key][0]
        except Exception:
            pass
        if all(c.isalnum() or c in ("-", "_") for c in raw):
            return raw
        return raw


# ── Module-level helpers ──────────────────────────────────────────────────────

def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _ensure_pdf(file_bytes: bytes, filename: str, mime_type: str | None) -> tuple[bytes, str]:
    """Convert supported non-PDF formats to PDF bytes."""
    suffix = Path(filename).suffix.lower()
    if suffix == ".pdf":
        return file_bytes, filename
    if mime_type and mime_type.startswith("application/vnd.google-apps."):
        return file_bytes, f"{Path(filename).stem}.pdf"
    try:
        from app.services.pdf_service import pdf_service
        return pdf_service.convert_to_pdf_bytes(file_bytes=file_bytes, filename=filename)
    except Exception as e:
        raise RuntimeError(f"Conversion failed for '{filename}': {e}") from e


def _sanitize(filename: str) -> str:
    safe = "".join(c if c.isalnum() or c in (".", "-", "_") else "_" for c in filename)
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.strip("_") or "document.pdf"


# ── Singleton ─────────────────────────────────────────────────────────────────
multi_drive_service = MultiDriveService()
