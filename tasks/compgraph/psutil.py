import os
import resource
from collections import namedtuple

MemoryInfo = namedtuple('MemoryInfo', 'rss')

class Process:
    def __init__(self, pid: int | None = None) -> None:
        self.pid = pid or os.getpid()

    def memory_info(self) -> MemoryInfo:  # pragma: no cover - used in memory tests
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # ru_maxrss is kilobytes on Linux
        return MemoryInfo(rss=usage * 1024)
