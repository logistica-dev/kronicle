# tests/unit/utils/test_singleton.py
import threading
import time
from unittest.mock import patch

from pytest import fixture, raises

from kronicle.utils.singleton import Singleton

# --------------------------------------------------------------------------------------
# Test subjects
# --------------------------------------------------------------------------------------


class Widget(metaclass=Singleton):
    """Records every ``__init__`` call so tests can prove it runs exactly once."""

    init_calls: list[str] = []

    def __init__(self, name: str = "default", **extras):
        Widget.init_calls.append(name)
        self.name = name
        self.extras = extras


class Gadget(metaclass=Singleton):
    def __init__(self, value=None):
        self.value = value


class Falsy(metaclass=Singleton):
    """A singleton whose instance is falsy (guards against ``if not instance`` bugs)."""

    def __bool__(self) -> bool:
        return False


class SubWidget(Widget):
    """A subclass is a distinct registry key, not a re-use of the parent instance."""


# --------------------------------------------------------------------------------------
# Isolation: Singleton._instances is shared, class-level state
# --------------------------------------------------------------------------------------


@fixture(autouse=True)
def _clean_registry():
    saved = dict(Singleton._instances)
    Singleton._instances.clear()
    Widget.init_calls.clear()
    yield
    Singleton._instances.clear()
    Singleton._instances.update(saved)
    Widget.init_calls.clear()


# --------------------------------------------------------------------------------------
# Fetch before initialize
# --------------------------------------------------------------------------------------


def test_fetch_before_initialize_raises():
    with raises(RuntimeError, match="Widget is not initialized"):
        Widget()


def test_pre_init_error_points_at_initialize():
    with raises(RuntimeError, match=r"call Widget\.initialize\(\.\.\.\)"):
        Widget()


def test_registry_is_empty_before_initialize():
    assert Widget not in Singleton._instances


# --------------------------------------------------------------------------------------
# initialize
# --------------------------------------------------------------------------------------


def test_initialize_returns_and_registers_the_instance():
    widget = Widget.initialize(name="first")
    assert isinstance(widget, Widget)
    assert Singleton._instances[Widget] is widget


def test_fetch_returns_the_initialized_instance():
    widget = Widget.initialize(name="first")
    assert Widget() is widget
    assert Widget() is Widget()


def test_init_runs_exactly_once():
    Widget.initialize(name="first")
    Widget()
    Widget()
    assert Widget.init_calls == ["first"]


def test_initialize_forwards_positional_and_keyword_args():
    widget = Widget.initialize("positional", size=3, colour="red")
    assert widget.name == "positional"
    assert widget.extras == {"size": 3, "colour": "red"}


def test_initialize_without_args_uses_init_defaults():
    assert Gadget.initialize().value is None


def test_double_initialize_raises_and_keeps_the_first_instance():
    first = Widget.initialize(name="first")
    with raises(RuntimeError, match="Widget is already initialized"):
        Widget.initialize(name="second")
    assert Widget() is first
    assert Widget().name == "first"
    assert Widget.init_calls == ["first"]


# --------------------------------------------------------------------------------------
# Arguments are rejected at fetch time, before and after initialize
# --------------------------------------------------------------------------------------


def test_arguments_rejected_before_initialize():
    with raises(RuntimeError, match=r"Widget\(\) takes no arguments"):
        Widget(name="first")
    assert Widget not in Singleton._instances  # rejected call must not initialize


def test_arguments_rejected_after_initialize():
    Widget.initialize(name="first")
    with raises(RuntimeError, match=r"Widget\(\) takes no arguments"):
        Widget(name="second")
    assert Widget().name == "first"


# --------------------------------------------------------------------------------------
# reset
# --------------------------------------------------------------------------------------


def test_reset_allows_reinitialization():
    Widget.initialize(name="first")
    Widget.reset()
    assert Widget not in Singleton._instances
    with raises(RuntimeError, match="Widget is not initialized"):
        Widget()
    assert Widget.initialize(name="second").name == "second"
    assert Widget.init_calls == ["first", "second"]


def test_reset_on_uninitialized_class_is_a_noop():
    Widget.reset()
    Widget.reset()
    assert Widget not in Singleton._instances


# --------------------------------------------------------------------------------------
# One registry slot per class
# --------------------------------------------------------------------------------------


def test_registry_is_keyed_by_class_not_shared():
    """Regression: one `_instances` dict must not collapse every class into one instance."""
    widget = Widget.initialize(name="w")
    gadget = Gadget.initialize(value=42)
    assert widget is not gadget
    assert len(Singleton._instances) == 2
    assert Singleton._instances[Widget] is not Singleton._instances[Gadget]


def test_each_class_fetches_its_own_instance():
    widget = Widget.initialize(name="w")
    gadget = Gadget.initialize(value=42)
    assert Widget() is widget
    assert Gadget() is gadget
    assert Gadget().value == 42


def test_initializing_one_class_does_not_initialize_another():
    Widget.initialize(name="w")
    with raises(RuntimeError, match="Gadget is not initialized"):
        Gadget()


def test_reset_only_affects_its_own_class():
    Widget.initialize(name="w")
    Gadget.initialize(value=42)
    Widget.reset()
    with raises(RuntimeError, match="Widget is not initialized"):
        Widget()
    assert Gadget().value == 42


def test_subclass_is_its_own_singleton():
    Widget.initialize(name="w")
    SubWidget.initialize(name="sub")
    assert Widget() is not SubWidget()
    assert Widget().name == "w"
    assert SubWidget().name == "sub"
    assert set(Singleton._instances) == {Widget, SubWidget}


def test_falsy_instance_is_still_returned():
    falsy = Falsy.initialize()
    assert bool(falsy) is False
    assert Falsy() is falsy


# --------------------------------------------------------------------------------------
# metaclass-provided methods stay patchable (test_main.py patches PasswordManager.initialize)
# --------------------------------------------------------------------------------------


def test_initialize_is_patchable_via_patch_object():
    with patch.object(Widget, "initialize") as mock_initialize:
        Widget.initialize(name="patched")
        mock_initialize.assert_called_once_with(name="patched")
    assert Widget not in Singleton._instances


def test_initialize_is_patchable_via_dotted_path():
    with patch(f"{__name__}.Gadget.initialize") as mock_initialize:
        Gadget.initialize(value=1)
        mock_initialize.assert_called_once_with(value=1)
    assert Gadget not in Singleton._instances


# --------------------------------------------------------------------------------------
# Thread safety of initialize
# --------------------------------------------------------------------------------------


def test_concurrent_initialize_creates_exactly_one_instance():
    """N threads released together must produce one instance and N-1 'already initialized'.

    The sleep inside ``__init__`` widens the check-then-create window; without the lock in
    ``Singleton.initialize`` every thread passes the membership check and constructs its own
    instance, so ``init_count`` becomes N and this test fails.
    """
    n = 8
    start_together = threading.Barrier(n)
    init_count: list[int] = []
    successes: list[object] = []
    failures: list[BaseException] = []

    class Contended(metaclass=Singleton):
        def __init__(self):
            init_count.append(1)
            time.sleep(0.05)

    def worker():
        start_together.wait()
        try:
            successes.append(Contended.initialize())
        except Exception as exc:  # noqa: BLE001 - collected so the thread cannot die silently
            failures.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(successes) == 1
    assert len(failures) == n - 1
    assert all(isinstance(exc, RuntimeError) for exc in failures)
    assert len(init_count) == 1, f"__init__ ran {len(init_count)} times"
    assert Contended() is successes[0]
