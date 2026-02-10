import threading
import time
from dataclasses import dataclass
from dataclasses import field


@dataclass
class BaseStats:
    total: int = 0
    skipped: int = 0
    failed: int = 0
    api_requests: int = 0
    cache_hits: int = 0
    start_time: float = field(default_factory=time.time)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def inc_skipped(self, count: int = 1):
        with self._lock:
            self.skipped += count

    def inc_failed(self, count: int = 1):
        with self._lock:
            self.failed += count

    def inc_api_requests(self, count: int = 1):
        with self._lock:
            self.api_requests += count

    def inc_cache_hits(self, count: int = 1):
        with self._lock:
            self.cache_hits += count

    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time
