from logging import DEBUG, INFO, Formatter, StreamHandler, getLogger
from os import getenv
from time import time

from rich.logging import RichHandler

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
LOG_LEVEL = int(getenv("DTS_LOG_LEVEL") or 3)
print("LOG_LEVEL:", LOG_LEVEL)


# ------------------------------------------------------
# Create loggers
# ------------------------------------------------------
basic_logger = getLogger("basic_logger")
request_logger = getLogger("request_logger")


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

    basic_handler = RichHandler(show_time=True, rich_tracebacks=True)
    basic_handler.setFormatter(basic_formatter)  # RichHandler already adds date & level

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


setup_logging()


def format_input(here: str, *args) -> str:  # pragma: no cover
    # right_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if len(args) == 0:
        return f"[{here}] <"
    if len(args) == 1:
        return f"[{here}] {args[0]}"
    else:
        try:
            return f"[{here}] {args[0]}: {' '.join(args[1:])}"
        except (UnicodeDecodeError, TypeError):
            return f"[{here}] {args[0]}: {' '.join(str(a) for a in args[1:])}"


def log_e(here, *args):  # pragma: no cover
    if LOG_LEVEL > -1:
        basic_logger.error("[bold red]" + format_input(here, *args) + "[/]", extra={"markup": True})


def log_w(here, *args):  # pragma: no cover
    if LOG_LEVEL > 0:
        basic_logger.warning("[yellow]" + format_input(here, *args) + "[/]", extra={"markup": True})


def log_i(here, *args):  # pragma: no cover
    if LOG_LEVEL > 1:
        basic_logger.info("[blue]" + format_input(here, *args) + "[/]", extra={"markup": True})


def log_d(here, *args):  # pragma: no cover
    if LOG_LEVEL > 2:
        basic_logger.debug(format_input(here, *args))


def log_d_if(here, should_print: bool = False, *args):  # pragma: no cover
    if LOG_LEVEL > 2 and should_print:
        basic_logger.debug(format_input(here, *args))


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


if __name__ == "__main__":  # pragma: no cover
    log_d("Test log")
    log_d("Log", "Test")
    log_d("Log", "Main", "test")
    log_i("Log", "Main", "info")
    log_w("Log", "Main", "warn")
    log_e("Log", "Main", "error")
