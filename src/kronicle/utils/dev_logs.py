# kronicle/utils/dev_logs.py
from contextlib import contextmanager
from logging import DEBUG, INFO, Formatter, StreamHandler, getLogger
from os import getenv
from time import time

from rich.logging import RichHandler
from rich.markup import escape

from kronicle.utils.str_utils import enforce_length

# LOGGING_CONFIG = {
#     "version": 1,
#     "disable_existing_loggers": False,
#     "formatters": {
#         "colored": {
#             "()": "uvicorn.logging.DefaultFormatter",
#             "fmt": "%(levelprefix)s %(asctime)s [%(name)s] %(message)s",
#             "use_colors": True,
#         },
#         "plain": {
#             "format": "%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
#         },
#     },
#     "handlers": {
#         "default": {
#             "formatter": "colored",
#             "class": "logging.StreamHandler",
#             "stream": "ext://sys.stdout",
#         },
#     },
#     "loggers": {
#         "": {"handlers": ["default"], "level": "DEBUG"},
#         "uvicorn": {"handlers": ["default"], "level": "INFO"},
#         "uvicorn.error": {"handlers": ["default"], "level": "INFO"},
#         "uvicorn.access": {"handlers": ["default"], "level": "INFO"},
#     },
# }

# dictConfig(LOGGING_CONFIG)
LOG_LEVEL = int(getenv("KRONICLE_LOG_LEVEL") or 3)
print("LOG_LEVEL:", LOG_LEVEL)


# ------------------------------------------------------
# Create loggers
# ------------------------------------------------------
basic_logger = getLogger("kronicle_basic_logger")
request_logger = getLogger("kronicle_request_logger")


def setup_logging():
    # ----------------------------------------------
    # Disable root logger handler auto-config
    # ----------------------------------------------
    getLogger().handlers.clear()
    getLogger().propagate = False

    # FULL_LOG = False  #
    # LOG_FORMAT = "%(asctime)s -%(levelname)s- %(message)s" if FULL_LOG else "%(message)s"
    # rich_handler.setFormatter(formatter)

    # ----------------------------------------------
    # Basic logger for app internals
    # ----------------------------------------------
    DATE_FORMAT = "%y-%m-%d %H:%M:%S"
    basic_formatter = Formatter("%(message)s")  # RichHandler already adds date & level
    basic_formatter.datefmt = DATE_FORMAT

    basic_handler = RichHandler(show_time=True, rich_tracebacks=True, markup=True)
    basic_handler.setFormatter(basic_formatter)

    basic_logger.handlers.clear()
    basic_logger.setLevel(DEBUG)
    basic_logger.propagate = False
    basic_logger.addHandler(basic_handler)

    # ----------------------------------------------
    # Request logger (clean, simple)
    # ----------------------------------------------
    request_formatter = Formatter("%(asctime)s [%(levelname)s] %(message)s")
    request_formatter.datefmt = DATE_FORMAT

    request_handler = StreamHandler()
    request_handler.setFormatter(request_formatter)

    request_logger.setLevel(INFO)
    request_logger.propagate = False
    request_logger.addHandler(request_handler)


def format_input(here: str, *args, **kwargs) -> str:  # pragma: no cover
    """
    Returns a formatted string for logs.
    Escapes brackets for Rich if markup=True.
    """
    # right_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    parts = []
    if args:
        parts.append(" ".join(str(a) for a in args))
    if kwargs:
        kv = " ".join(f"{k}={v}" for k, v in kwargs.items())
        parts.append(kv)

    # Escape brackets for Rich
    here_str = f"[{(enforce_length(here, 10))}]"
    content = " | ".join(parts) if parts else ""
    raw = f"{here_str} {content}" if content else f"{here_str} <"
    return escape(raw)


def log_e(here, *args, stacklevel=2, **kwargs):  # pragma: no cover
    if LOG_LEVEL > -1:
        msg = format_input(here, *args, **kwargs)
        basic_logger.error(f"[bold red]{msg}[/bold red]", stacklevel=stacklevel)


def log_w(here, *args, stacklevel=2, **kwargs):  # pragma: no cover
    if LOG_LEVEL > 0:
        msg = format_input(here, *args, **kwargs)
        basic_logger.warning(f"[yellow]{msg}[/yellow]", stacklevel=stacklevel)


def log_i(here, *args, stacklevel=2, **kwargs):  # pragma: no cover
    if LOG_LEVEL > 1:
        msg = format_input(here, *args, **kwargs)
        basic_logger.info(f"[blue]{msg}[/blue]", stacklevel=stacklevel)


def log_d(here, *args, stacklevel=2, **kwargs):  # pragma: no cover
    if LOG_LEVEL > 2:
        basic_logger.debug(format_input(here, *args, **kwargs), stacklevel=stacklevel)


def log_d_if(here, should_print: bool = False, *args, **kwargs):  # pragma: no cover
    if LOG_LEVEL > 2 and should_print:
        basic_logger.debug(format_input(here, *args, **kwargs), stacklevel=2)


def decorator_timer(some_function):  # pragma: no cover
    def _wrap(*args, **kwargs):
        multiplier = 50
        begin = time()
        result = None
        for _ in range(multiplier):
            result = some_function(*args, **kwargs)
        duration = (time() - begin) / multiplier
        log_d(some_function.__name__, "duration", duration)
        return result, duration

    return _wrap


def log_assert(cond: bool, ok_tag: str = "OK", ko_tag: str = "!! KO !!"):  # pragma: no cover
    return ok_tag if cond else ko_tag


# Context manager for logging
@contextmanager
def log_block(here, message):
    log_d(here, f"Starting {message}...", stacklevel=4)
    start_time = time()
    try:
        yield
    finally:
        elapsed = time() - start_time
        log_d(here, f"  >{message} init in {elapsed:.3f}s", stacklevel=4)


if __name__ == "__main__":  # pragma: no cover
    log_d("Test log")
    log_d("Log", "Test")
    log_d("Log", "Main", "test")
    log_i("Log", "Main", "info")
    log_w("Log", "Main", "warn")
    log_e("Log", "Main", "error")
