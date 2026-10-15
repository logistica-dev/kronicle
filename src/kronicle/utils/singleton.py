# kronicle/utils/singleton.py
from __future__ import annotations

import threading
from typing import Any, TypeVar

_T = TypeVar("_T")


class Singleton(type):
    """
    Metaclass for singletons that are configured once and then fetched.

    Contract:
        Cls.initialize(**params)   create the one instance (raises if already done)
        Cls()                      return it (raises if not initialized)
        Cls(**params)              raises: parameters only go through ``initialize``

    The class writes an ordinary ``__init__`` to configure itself; it runs exactly once,
    inside ``initialize()``. ``Cls()`` is therefore always a *fetch*, never a construction,
    so a bare ``Cls()`` can never silently produce a second, default-configured instance.

    ``initialize()`` is thread-safe: concurrent callers serialize on a lock and all but the
    first are told the class is already initialized. ``Cls()`` stays lock-free — an entry is
    only published after ``__init__`` has finished, so a reader sees either nothing or a
    fully configured instance.

    Locking scope — one lock for the whole registry:
        ``_lock`` is an attribute of this metaclass, so it is shared by *every* class that
        uses ``Singleton``. It is deliberately not one lock per class, and only a single
        ``initialize()`` can therefore be in flight process-wide: initializing two *different*
        singletons concurrently serializes instead of running in parallel. That is acceptable
        because ``initialize()`` runs once, at startup, where the contention is irrelevant and
        a single lock cannot be mis-keyed. If per-class parallelism is ever needed, replace
        ``_lock`` with locks keyed by class (``dict[type, threading.RLock]``). ``reset()`` is
        not locked either: it is a single-threaded test hook.
    """

    _instances: dict[type, Any] = {}
    _lock = threading.RLock()

    def __call__(cls: type[_T], *args: Any, **kwargs: Any) -> _T:
        if args or kwargs:
            raise RuntimeError(f"{cls.__name__}() takes no arguments; use {cls.__name__}.initialize(...)")
        instance = Singleton._instances.get(cls)
        if instance is None:
            raise RuntimeError(f"{cls.__name__} is not initialized; call {cls.__name__}.initialize(...) first")
        return instance

    def initialize(cls: type[_T], *args: Any, **kwargs: Any) -> _T:
        """Create and register the instance. Raises if it already exists."""
        with Singleton._lock:
            if cls in Singleton._instances:
                raise RuntimeError(f"{cls.__name__} is already initialized")
            instance = super().__call__(*args, **kwargs)
            Singleton._instances[cls] = instance
            return instance

    def reset(cls) -> None:
        """Forget the instance, allowing one more ``initialize()`` (test hook)."""
        Singleton._instances.pop(cls, None)
