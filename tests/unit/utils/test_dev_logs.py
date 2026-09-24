# tests/unit/utils/test_dev_logs.py
from logging import INFO, LogRecord
from unittest.mock import patch

from kronicle.utils.dev_logs import (
    OneLetterRichHandler,
    SingleLetterFormatter,
    attach_request_console_logger,
    basic_logger,
    decorator_timer,
    format_input,
    log_assert,
    log_block,
    log_d_if,
    request_logger,
)


# ------------------------------------------------------
# format_input
# ------------------------------------------------------
def test_format_input_basic():
    result = format_input("here", "msg")
    assert "[here" in result
    assert "msg" in result


def test_format_input_multiple_args():
    result = format_input("test", "key", "val1", "val2")
    assert "key:" in result
    assert "val1 val2" in result


def test_format_input_kwargs():
    result = format_input("test", a=1, b=2)
    assert "a=1" in result
    assert "b=2" in result


def test_format_input_no_content():
    result = format_input("test")
    assert "<" in result  # empty marker


# ------------------------------------------------------
# log_assert
# ------------------------------------------------------
def test_log_assert_true():
    assert log_assert(True) == "OK"


def test_log_assert_false():
    assert log_assert(False) == "!! KO !!"


def test_log_assert_custom_tags():
    assert log_assert(True, ok_tag="YES") == "YES"
    assert log_assert(False, ko_tag="NO") == "NO"


# ------------------------------------------------------
# decorator_timer
# ------------------------------------------------------
def test_decorator_timer_returns_result_and_duration():
    @decorator_timer
    def add(a, b):
        return a + b

    result, duration = add(1, 2)

    assert result == 3
    assert isinstance(duration, float)


def test_decorator_timer_calls_function_multiple_times():
    calls = []

    def func():
        calls.append(1)
        return 42

    wrapped = decorator_timer(func)
    result, _ = wrapped()

    assert result == 42
    assert len(calls) == 50


# ------------------------------------------------------
# log_block
# ------------------------------------------------------
def test_log_block_calls_log_start_and_end():
    with (
        patch("kronicle.utils.dev_logs.log_i") as mock_start,
        patch("kronicle.utils.dev_logs.log_d") as mock_end,
    ):
        with log_block("test", "something"):
            pass

        # start is logged at info level, end at debug level
        mock_start.assert_called_once_with("test", "Starting something...", stacklevel=5)
        mock_end.assert_called_once()


# ------------------------------------------------------
# log_d_if
# ------------------------------------------------------
def test_log_d_if_logs_when_true():
    with patch("kronicle.utils.dev_logs.basic_logger.debug") as mock_debug:
        with patch("kronicle.utils.dev_logs.LOG_LEVEL", 3):
            log_d_if("here", True, "msg")

        mock_debug.assert_called_once()


def test_log_d_if_does_not_log_when_false():
    with patch("kronicle.utils.dev_logs.basic_logger.debug") as mock_debug:
        with patch("kronicle.utils.dev_logs.LOG_LEVEL", 3):
            log_d_if("here", False, "msg")

        mock_debug.assert_not_called()


# ------------------------------------------------------
# attach_request_console_logger
# ------------------------------------------------------
class TestAttachRequestConsoleLogger:
    def teardown_method(self):
        request_logger.handlers = [h for h in request_logger.handlers if not isinstance(h, OneLetterRichHandler)]

    @patch("kronicle.utils.dev_logs._ensure_logging")
    def test_attaches_rich_console_handler_when_enabled(self, mock_ensure):
        mock_ensure.side_effect = None
        h = OneLetterRichHandler()
        basic_logger.addHandler(h)
        try:
            attach_request_console_logger(enabled=True)
            assert any(isinstance(x, OneLetterRichHandler) for x in request_logger.handlers)
        finally:
            basic_logger.removeHandler(h)
            request_logger.handlers = [x for x in request_logger.handlers if not isinstance(x, OneLetterRichHandler)]

    def test_does_not_add_duplicate_handler(self):
        h = OneLetterRichHandler()
        basic_logger.addHandler(h)
        try:
            attach_request_console_logger(True)
            before = [x for x in request_logger.handlers if isinstance(x, OneLetterRichHandler)]
            attach_request_console_logger(True)
            after = [x for x in request_logger.handlers if isinstance(x, OneLetterRichHandler)]
            assert len(after) == len(before)
        finally:
            basic_logger.removeHandler(h)
            request_logger.handlers = [x for x in request_logger.handlers if not isinstance(x, OneLetterRichHandler)]

    @patch("kronicle.utils.dev_logs._ensure_logging")
    def test_noop_when_disabled(self, mock_ensure):
        attach_request_console_logger(enabled=False)
        assert not any(isinstance(h, OneLetterRichHandler) for h in request_logger.handlers)


# ------------------------------------------------------
# SingleLetterFormatter
# ------------------------------------------------------


def test_single_letter_formatter_changes_levelname():
    formatter = SingleLetterFormatter("%(levelname)s")

    record = LogRecord(
        name="test",
        level=INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )

    output = formatter.format(record)

    assert output == "I"
