# kronicle/utils/dev_logs.py
import logging
import sys
import traceback
from contextlib import contextmanager
from logging import CRITICAL, DEBUG, ERROR, INFO, WARNING, Formatter, LogRecord, getLogger
from logging.handlers import RotatingFileHandler, SysLogHandler
from os import getenv, makedirs
from pathlib import Path
from sys import base_prefix, prefix
from time import time
from unittest.mock import MagicMock

from rich.console import Console
from rich.logging import RichHandler
from rich.markup import escape
from rich.text import Text
from rich.traceback import Traceback

from kronicle.utils.str_utils import enforce_length

# ------------------------------------------------------
# Configuration
# ------------------------------------------------------
LOG_LEVEL = int(getenv("KRONICLE_LOG_LEVEL", 3))
print("LOG_LEVEL:", LOG_LEVEL)


PROJECT_ROOT = Path(__file__).parent.parent.resolve()
VENV_PATH_PART = ".venv"  # adjust if your virtual env path differs

LEVEL_SHORT = {
    DEBUG: "D",
    INFO: "I",
    WARNING: "W",
    ERROR: "E",
    CRITICAL: "C",
}

HERE_LEN = 15
LOG_LINE_LEN = 140

_logging_initialized = False


class OneLetterRichHandler(RichHandler):
    """RichHandler that filters .venv frames and shows one-letter levels."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Add virtualenv path to suppressed frames
        venv_root = Path(prefix).resolve()
        if venv_root != Path(base_prefix).resolve():
            suppress_paths = list(getattr(self, "tracebacks_suppress", []))
            suppress_paths.extend([str(venv_root), "starlette", "asyncpg"])
            self.tracebacks_suppress = suppress_paths

    def get_level_text(self, record):
        # Don't pad with spaces; keep the levelname as-is
        return Text.styled(record.levelname, f"logging.level.{record.levelname.lower()}")

    def emit(self, record: LogRecord) -> None:
        """Emit log with rich traceback safely."""
        message = self.format(record)
        traceback_renderable = None

        if self.rich_tracebacks and record.exc_info not in (None, (None, None, None)):
            exc_type, exc_value, exc_tb = record.exc_info
            assert exc_type is not None
            assert exc_value is not None
            # Pyright-safe: always pass correct types
            traceback_renderable = Traceback.from_exception(
                exc_type,
                exc_value,
                exc_tb,
                width=self.tracebacks_width,
                code_width=self.tracebacks_code_width,
                extra_lines=self.tracebacks_extra_lines,
                theme=self.tracebacks_theme,
                word_wrap=self.tracebacks_word_wrap,
                show_locals=self.tracebacks_show_locals,
                locals_max_length=self.locals_max_length,
                locals_max_string=self.locals_max_string,
                suppress=self.tracebacks_suppress,
                max_frames=self.tracebacks_max_frames,
            )

        # Render message with optional traceback
        message_renderable = self.render_message(record, message)
        log_renderable = self.render(
            record=record,
            traceback=traceback_renderable,
            message_renderable=message_renderable,
        )

        try:
            self.console.print(log_renderable)
        except Exception:
            self.handleError(record)


class SingleLetterFormatter(Formatter):
    """Formatter that replaces levelname with a single letter."""

    def format(self, record):
        record.levelname = LEVEL_SHORT.get(record.levelno, record.levelname)
        return super().format(record)


class StripPrefixFormatter(Formatter):
    """Formatter that strips a known base path from record.pathname."""

    def __init__(self, fmt, datefmt, base_path):
        super().__init__(fmt, datefmt)
        self.base = str(Path(base_path).resolve()) + "/"

    def format(self, record):
        path = record.pathname
        if path.startswith(self.base):
            record.pathname = "/" + path[len(self.base) :]
        return super().format(record)


# ------------------------------------------------------
# Loggers
# ------------------------------------------------------
basic_logger = getLogger("kronicle_basic_logger")
file_logger = getLogger("kronicle_file_log")
request_logger = getLogger("kronicle_request_logger")


def setup_logging():
    """Initialize application logging with RichHandler and file logging."""
    global _logging_initialized

    if _logging_initialized:
        return

    # Clear root handlers
    root_logger = getLogger()
    root_logger.handlers.clear()
    root_logger.propagate = False

    DATE_FORMAT = "%y-%m-%d %H:%M:%S"

    # ---------------------
    # Basic logger
    # ---------------------
    basic_formatter = SingleLetterFormatter("%(message)s")
    basic_formatter.datefmt = DATE_FORMAT

    basic_handler = OneLetterRichHandler(
        console=Console(width=LOG_LINE_LEN),
        show_time=True,
        rich_tracebacks=True,
        markup=True,
        tracebacks_width=LOG_LINE_LEN,
        tracebacks_code_width=LOG_LINE_LEN,
    )
    basic_handler.setFormatter(basic_formatter)

    basic_logger.handlers.clear()
    basic_logger.setLevel(DEBUG)
    basic_logger.propagate = False
    basic_logger.addHandler(basic_handler)

    # ---------------------
    # File logger — captures raw messages (no Rich markup) to ./logs/
    # ---------------------
    log_file = "unknown"
    try:
        from datetime import datetime

        log_dir = Path(__file__).resolve().parent.parent.parent.parent / "logs"
        makedirs(str(log_dir), exist_ok=True)
        log_file = log_dir / f'{datetime.now().strftime("%Y%m%d_%H%M%S")}_kronicle_app.log'

        kronicle_base = Path(__file__).resolve().parent.parent
        file_formatter = StripPrefixFormatter(
            "%(asctime)s [%(levelname)s] %(message)s\tf:/%(pathname)s:%(lineno)d",
            DATE_FORMAT,
            kronicle_base,
        )
        file_handler = RotatingFileHandler(
            str(log_file),
            maxBytes=10 * 1024 * 1024,
            backupCount=3,
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(DEBUG)
        file_handler.terminator = "\n"
        file_handler.flush()

        file_logger.handlers.clear()
        file_logger.setLevel(DEBUG)
        file_logger.propagate = False
        file_logger.addHandler(file_handler)

        request_logger.addHandler(file_handler)
    except Exception:
        print(f"[WARN] Failed to set up file logger at {log_file}", file=sys.stderr)
        traceback.print_exc()
    # ---------------------
    # Request logger (file only; uvicorn handles console)
    # ---------------------
    request_logger.setLevel(INFO)
    request_logger.propagate = False

    # ---------------------
    # Syslog
    # ---------------------
    syslog_handler = SysLogHandler(address="/dev/log")
    syslog_formatter = Formatter(
        "%(asctime)s %(name)s %(levelname)s: %(message)s",
        datefmt="%b %d %H:%M:%S",
    )
    syslog_handler.setFormatter(syslog_formatter)

    syslog = getLogger("kronicle_syslog")
    syslog.addHandler(syslog_handler)
    syslog.setLevel(DEBUG)

    _logging_initialized = True


def _ensure_logging():
    if not _logging_initialized:
        setup_logging()


# ------------------------------------------------------
# Utility functions
# ------------------------------------------------------
def format_input(here: str, *args, **kwargs) -> str:
    """Format log content and escape brackets for Rich."""
    parts = []
    if args:
        if len(args) > 1:
            parts.append(f"{args[0]}: {' '.join(str(a) for a in args[1:])}")
        else:
            parts.append(" ".join([str(a) for a in args]))
    if kwargs:
        parts.append(" ".join(f"{k}={v}" for k, v in kwargs.items()))

    here_str = f"[{enforce_length(here, HERE_LEN)}]"
    content = " | ".join(parts) if parts else ""
    raw = f"{here_str} {content}" if content else f"{here_str} <"
    return raw


def _log(level_func, color: str, here: str, *args, stacklevel=2, **kwargs):
    """Internal helper to log colored messages according to LOG_LEVEL."""
    _ensure_logging()
    exc_info = kwargs.pop("exc_info", None)
    msg = format_input(here, *args, **kwargs)
    try:
        level_name = getattr(level_func, "__name__", "debug").upper()
        file_logger.log(getattr(logging, level_name, logging.DEBUG), msg, exc_info=exc_info, stacklevel=stacklevel)
    except Exception as e:
        print(e)
    level_func(f"[{color}]{escape(msg)}[/{color}]", stacklevel=stacklevel, exc_info=exc_info)


def log_e(here, *args, stacklevel=3, **kwargs):
    if LOG_LEVEL > -1:
        _log(basic_logger.error, "bold red", here, *args, stacklevel=stacklevel, **kwargs)


def log_w(here, *args, stacklevel=3, **kwargs):
    if LOG_LEVEL > 0:
        _log(basic_logger.warning, "yellow", here, *args, stacklevel=stacklevel, **kwargs)


def log_i(here, *args, stacklevel=3, **kwargs):
    if LOG_LEVEL > 1:
        _log(basic_logger.info, "blue", here, *args, stacklevel=stacklevel, **kwargs)


def log_d(here, *args, stacklevel=3, **kwargs):
    if LOG_LEVEL > 2:
        _log(basic_logger.debug, "white", here, *args, stacklevel=stacklevel, **kwargs)
        # basic_logger.debug(format_input(here, *args, **kwargs), stacklevel=stacklevel)


def log_t(here, *args, stacklevel=3, **kwargs):
    if LOG_LEVEL > 3:
        _log(basic_logger.debug, "white", here, *args, stacklevel=stacklevel, **kwargs)
        # basic_logger.debug(format_input(here, *args, **kwargs), stacklevel=stacklevel)


def log_d_if(here, should_print: bool = False, *args, stacklevel=3, **kwargs):
    if LOG_LEVEL > 2 and should_print:
        _log(basic_logger.debug, "white", here, *args, stacklevel=stacklevel, **kwargs)
        # basic_logger.debug(format_input(here, *args, **kwargs), stacklevel=2)


def decorator_timer(func):
    """Measure average execution time of a function over 50 runs."""

    def _wrap(*args, **kwargs):
        multiplier = 50
        start = time()
        result = None
        for _ in range(multiplier):
            result = func(*args, **kwargs)
        duration = (time() - start) / multiplier
        log_d(func.__name__, "duration", duration)
        return result, duration

    return _wrap


def log_assert(cond: bool, ok_tag: str = "OK", ko_tag: str = "!! KO !!") -> str:
    return ok_tag if cond else ko_tag


# ------------------------------------------------------
# Context manager
# ------------------------------------------------------
@contextmanager
def log_block(here, message):
    stacklevel = 5
    log_d(here, f"Starting {message}...", stacklevel=stacklevel)
    start_time = time()
    try:
        yield
    except Exception as e:
        elapsed = time() - start_time
        log_e(here, f"  >{message}: FAILED after {elapsed:.3f}s ({e})", stacklevel=stacklevel)
        raise
    else:
        elapsed = time() - start_time
        log_d(here, f"  >{message}: done in {elapsed:.3f}s", stacklevel=stacklevel)


# ------------------------------------------------------
# Example usage
# ------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover
    here = "Log"
    log_d("Test log")
    log_d("Log", "Test")
    log_d("Log", "Main", "test")
    log_i("Log", "Main", "info")
    log_w("Log", "Main", "warn")
    log_e("Log", "Main", "error")
    formatter = SingleLetterFormatter("%(levelname)s")

    record = MagicMock()
    record.levelno = 20  # INFO
    record.levelname = "INFO"

    output = formatter.format(record)
    log_d(here, "output", output)

    assert "I" in output  # INFO -> I
