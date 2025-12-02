"""Lightweight fallback implementation of a tiny subset of :mod:`psutil`.

The testing environment for this kata may not provide the external
``psutil`` dependency. Only the :class:`Process` class with a
:py:meth:`memory_info` method is required by the tests, so this module
implements a minimal compatible surface backed by the standard library.
"""
from __future__ import annotations

import dataclasses
import os
import resource


@dataclasses.dataclass
class _MemoryInfo:
    """Container mimicking ``psutil.Process().memory_info()`` result."""

    rss: int


class Process:
    """Minimal stand-in for :class:`psutil.Process`.

    Only :py:meth:`memory_info` is implemented, returning an object with an
    ``rss`` attribute measured in bytes.
    """

    def __init__(self, _pid: int | None = None) -> None:
        self._pid = _pid or os.getpid()

    def memory_info(self) -> _MemoryInfo:
        """Return RSS of the current process in bytes."""

        try:
            usage_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # ``ru_maxrss`` is reported in KiB on Linux, convert to bytes.
            rss = int(usage_kib) * 1024
        except Exception:
            rss = 0
        return _MemoryInfo(rss=rss)


__all__ = ["Process"]
