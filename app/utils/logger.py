"""
logger.py - Structured logging for PDF Research Analyzer
Provides a consistent logger across all services and modules.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

# ── Import log level from config ──────────────────────────────────────────────
# We import only what we need to avoid circular imports
try:
    from app.config import LOG_LEVEL, DEBUG, BASE_DIR
except ImportError:
    LOG_LEVEL = "INFO"
    DEBUG = False
    BASE_DIR = Path(__file__).resolve().parent.parent.parent


# ── Log File Setup ────────────────────────────────────────────────────────────
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_DIR / f"app_{datetime.now().strftime('%Y-%m-%d')}.log"


# ── Custom Formatter ──────────────────────────────────────────────────────────
class CustomFormatter(logging.Formatter):
    """
    Colored console formatter + clean file formatter.
    Colors help distinguish log levels at a glance in the terminal.
    """

    # ANSI color codes
    GREY = "\x1b[38;20m"
    CYAN = "\x1b[36;20m"
    YELLOW = "\x1b[33;20m"
    RED = "\x1b[31;20m"
    BOLD_RED = "\x1b[31;1m"
    RESET = "\x1b[0m"

    BASE_FORMAT = (
        "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s"
    )
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    LEVEL_COLORS = {
        logging.DEBUG: CYAN,
        logging.INFO: GREY,
        logging.WARNING: YELLOW,
        logging.ERROR: RED,
        logging.CRITICAL: BOLD_RED,
    }

    def format(self, record: logging.LogRecord) -> str:
        color = self.LEVEL_COLORS.get(record.levelno, self.GREY)
        formatter = logging.Formatter(
            fmt=f"{color}{self.BASE_FORMAT}{self.RESET}",
            datefmt=self.DATE_FORMAT,
        )
        return formatter.format(record)


class PlainFormatter(logging.Formatter):
    """Plain formatter for file output — no ANSI codes."""

    BASE_FORMAT = (
        "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s"
    )
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def format(self, record: logging.LogRecord) -> str:
        formatter = logging.Formatter(
            fmt=self.BASE_FORMAT,
            datefmt=self.DATE_FORMAT,
        )
        return formatter.format(record)


# ── Logger Factory ────────────────────────────────────────────────────────────
def get_logger(name: str) -> logging.Logger:
    """
    Returns a named logger with:
    - Colored output to stdout
    - Plain text output to daily rotating log file
    - Level controlled by LOG_LEVEL env var

    Usage:
        from app.utils.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Processing started")
        logger.error("Something went wrong", exc_info=True)

    Args:
        name: Typically __name__ of the calling module.

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if logger already configured
    if logger.handlers:
        return logger

    numeric_level = getattr(logging, LOG_LEVEL, logging.INFO)
    logger.setLevel(logging.DEBUG if DEBUG else numeric_level)

    # ── Console Handler (colored) ─────────────────────────────────────────────
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if DEBUG else numeric_level)
    console_handler.setFormatter(CustomFormatter())

    # ── File Handler (plain text, daily log) ──────────────────────────────────
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)  # always write DEBUG+ to file
    file_handler.setFormatter(PlainFormatter())

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # Prevent log messages from bubbling up to root logger
    logger.propagate = False

    return logger


# ── Convenience Loggers ───────────────────────────────────────────────────────
# Pre-built loggers for each service — import these directly if preferred

pdf_logger        = get_logger("pdf_service")
extraction_logger = get_logger("extraction_service")
embedding_logger  = get_logger("embedding_service")
rag_logger        = get_logger("rag_service")
analysis_logger   = get_logger("analysis_service")
ai_router_logger  = get_logger("ai_router")
ui_logger         = get_logger("streamlit_ui")


# ── Startup Logger ────────────────────────────────────────────────────────────
def log_startup(config_summary: dict, warnings: list[str]) -> None:
    """
    Logs application startup info and any config warnings.
    Call this once from main.py at app start.

    Args:
        config_summary: dict from config.get_config_summary()
        warnings: list from config.validate_config()
    """
    startup_logger = get_logger("startup")

    startup_logger.info("=" * 60)
    startup_logger.info(f"  {config_summary.get('app_title')} v{config_summary.get('version')} starting up")
    startup_logger.info("=" * 60)

    for key, value in config_summary.items():
        startup_logger.info(f"  {key:<30} {value}")

    startup_logger.info("-" * 60)

    if warnings:
        for warning in warnings:
            startup_logger.warning(f"  ⚠  {warning}")
    else:
        startup_logger.info("  ✓  Config validation passed — no warnings")

    startup_logger.info(f"  ✓  Log file: {LOG_FILE}")
    startup_logger.info("=" * 60)


# ── Context Logger ────────────────────────────────────────────────────────────
class ServiceLogger:
    """
    Thin wrapper that prefixes every log message with a doc_id or
    session_id for easy log tracing across services.

    Usage:
        slog = ServiceLogger("pdf_service", doc_id="abc123")
        slog.info("Extraction complete")
        # logs → [abc123] Extraction complete
    """

    def __init__(self, service_name: str, doc_id: str = ""):
        self._logger = get_logger(service_name)
        self._prefix = f"[{doc_id}] " if doc_id else ""

    def _fmt(self, msg: str) -> str:
        return f"{self._prefix}{msg}"

    def debug(self, msg: str, *args, **kwargs) -> None:
        self._logger.debug(self._fmt(msg), *args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        self._logger.info(self._fmt(msg), *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        self._logger.warning(self._fmt(msg), *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        self._logger.error(self._fmt(msg), *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs) -> None:
        self._logger.critical(self._fmt(msg), *args, **kwargs)