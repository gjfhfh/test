"""Minimal psutil stub for test environments without the real package."""
from __future__ import annotations

import importlib
import os
from collections import namedtuple
from typing import Any

try:  # pragma: no cover - platform-dependent import
    import resource  # type: ignore
except Exception:  # pragma: no cover
    resource = None  # type: ignore

try:  # pragma: no cover - if real psutil is installed we delegate to it
    _real_psutil = importlib.import_module("psutil")
except Exception:  # pragma: no cover
    _real_psutil = None

_MemoryInfo = namedtuple("_MemoryInfo", "rss")


class Process:  # pragma: no cover - tiny compatibility shim
    """Lightweight stand-in for :mod:`psutil.Process` used in tests."""

    def __init__(self, pid: int | None = None) -> None:
        self.pid = pid or os.getpid()
        self._delegate = None
        if _real_psutil is not None:
            try:
                self._delegate = _real_psutil.Process(self.pid)
            except Exception:
                self._delegate = None

    def memory_info(self) -> _MemoryInfo:
        if self._delegate is not None:
            info: Any = self._delegate.memory_info()
            return _MemoryInfo(rss=int(getattr(info, "rss", 0)))

        if resource is not None:
            usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            rss_bytes = int(usage) * 1024 if os.name != "nt" else int(usage)
            return _MemoryInfo(rss=rss_bytes)

        try:
            with open(f"/proc/{self.pid}/statm", "r", encoding="utf-8") as statm:
                pages = int(statm.readline().split()[0])
            page_size = os.sysconf("SC_PAGE_SIZE")
            return _MemoryInfo(rss=pages * page_size)
        except Exception:
            return _MemoryInfo(rss=0)


__all__ = ["Process"]
