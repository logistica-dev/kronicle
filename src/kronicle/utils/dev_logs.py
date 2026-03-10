# kronicle/utils/dev_logs.py
from contextlib import contextmanager
from logging import CRITICAL, DEBUG, ERROR, INFO, WARNING, Formatter, StreamHandler, getLogger
from os import getenv
from time import time

from rich.logging import RichHandler
from rich.markup import escape
from rich.text import Text

from kronicle.utils.str_utils import enforce_length

# ------------------------------------------------------
# Configuration
# ------------------------------------------------------
LOG_LEVEL = int(getenv("KRONICLE_LOG_LEVEL", 3))
print("LOG_LEVEL:", LOG_LEVEL)

LEVEL_SHORT = {
    DEBUG: "D",
    INFO: "I",
    WARNING: "W",
    ERROR: "E",
    CRITICAL: "C",
}


# Subclass RichHandler to remove the padding
class OneLetterRichHandler(RichHandler):
    def get_level_text(self, record):
        # Don't pad with spaces; keep the levelname as-is
        return Text.styled(record.levelname, f"logging.level.{record.levelname.lower()}")


class SingleLetterFormatter(Formatter):
    """Formatter that replaces levelname with a single letter."""

    def format(self, record):
        record.levelname = LEVEL_SHORT.get(record.levelno, record.levelname)
        return super().format(record)


# ------------------------------------------------------
# Loggers
# ------------------------------------------------------
basic_logger = getLogger("kronicle_basic_logger")
request_logger = getLogger("kronicle_request_logger")


def setup_logging():
    """Initialize application logging with RichHandler."""
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
        show_time=True,
        rich_tracebacks=True,
        markup=True,
        tracebacks_width=120,
        tracebacks_code_width=120,
    )
    basic_handler.setFormatter(basic_formatter)

    basic_logger.handlers.clear()
    basic_logger.setLevel(DEBUG)
    basic_logger.propagate = False
    basic_logger.addHandler(basic_handler)

    # ---------------------
    # Request logger
    # ---------------------
    request_formatter = Formatter("%(asctime)s [%(levelname)s] %(message)s", DATE_FORMAT)
    request_handler = StreamHandler()
    request_handler.setFormatter(request_formatter)

    request_logger.setLevel(INFO)
    request_logger.propagate = False
    request_logger.addHandler(request_handler)


# ------------------------------------------------------
# Utility functions
# ------------------------------------------------------
def format_input(here: str, *args, **kwargs) -> str:
    """Format log content and escape brackets for Rich."""
    parts = []
    if args:
        parts.append(" ".join(str(a) for a in args))
    if kwargs:
        parts.append(" ".join(f"{k}={v}" for k, v in kwargs.items()))

    here_str = f"[{enforce_length(here, 10)}]"
    content = " | ".join(parts) if parts else ""
    raw = f"{here_str} {content}" if content else f"{here_str} <"
    return escape(raw)


def _log(level_func, color: str, here: str, *args, stacklevel=2, **kwargs):
    """Internal helper to log colored messages according to LOG_LEVEL."""
    msg = format_input(here, *args, **kwargs)
    level_func(f"[{color}]{msg}[/{color}]", stacklevel=stacklevel)


def log_e(here, *args, stacklevel=2, **kwargs):
    if LOG_LEVEL > -1:
        _log(basic_logger.error, "bold red", here, *args, stacklevel=stacklevel, **kwargs)


def log_w(here, *args, stacklevel=2, **kwargs):
    if LOG_LEVEL > 0:
        _log(basic_logger.warning, "yellow", here, *args, stacklevel=stacklevel, **kwargs)


def log_i(here, *args, stacklevel=2, **kwargs):
    if LOG_LEVEL > 1:
        _log(basic_logger.info, "blue", here, *args, stacklevel=stacklevel, **kwargs)


def log_d(here, *args, stacklevel=2, **kwargs):
    if LOG_LEVEL > 2:
        basic_logger.debug(format_input(here, *args, **kwargs), stacklevel=stacklevel)


def log_d_if(here, should_print: bool = False, *args, **kwargs):
    if LOG_LEVEL > 2 and should_print:
        basic_logger.debug(format_input(here, *args, **kwargs), stacklevel=2)


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
    log_d(here, f"Starting {message}...", stacklevel=4)
    start_time = time()
    try:
        yield
    finally:
        elapsed = time() - start_time
        log_d(here, f"  >{message} init in {elapsed:.3f}s", stacklevel=4)


# ------------------------------------------------------
# Example usage
# ------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover
    setup_logging()
    log_d("Test log")
    log_d("Log", "Test")
    log_d("Log", "Main", "test")
    log_i("Log", "Main", "info")
    log_w("Log", "Main", "warn")
    log_e("Log", "Main", "error")
