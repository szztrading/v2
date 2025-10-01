import time
from functools import lru_cache

@lru_cache(maxsize=2048)
def memo(key: str, value: str):
    # 占位：用于 requests 级缓存可替换为 requests-cache
    return value

class SimpleRateLimiter:
    def __init__(self, qps: float = 2.0):
        self.gap = 1.0 / qps
        self.last = 0.0
    def wait(self):
        now = time.time()
        delta = now - self.last
        if delta < self.gap:
            time.sleep(self.gap - delta)
        self.last = time.time()
