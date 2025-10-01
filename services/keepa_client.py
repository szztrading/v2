import time, requests
from typing import List, Tuple, Optional, Dict
from core.cache import SimpleRateLimiter

class KeepaClient:
    def __init__(self, api_key: str | None, domain_map: Dict[str,int], timeout=25, retries=2):
        self.key = api_key or ""
        self.domain_map = domain_map
        self.timeout = timeout
        self.retries = retries
        self.rl = SimpleRateLimiter(qps=2.0)

    def product_related(self, asin: str, domain_name: str, history: int = 0) -> Tuple[List[str], Optional[str]]:
        """返回 related ASIN 列表（alsoBought/alsoViewed/frequentlyBoughtTogether/related 合并去重）"""
        if not self.key:
            return [], "No Keepa API Key"
        dom = self.domain_map.get(domain_name, 2)
        url = "https://api.keepa.com/product"
        params = {"key": self.key, "domain": dom, "asin": asin, "history": history}

        last_err = None
        for _ in range(self.retries + 1):
            try:
                self.rl.wait()
                r = requests.get(url, params=params, timeout=self.timeout)
                r.raise_for_status()
                data = r.json()
                if "error" in data and data["error"]:
                    return [], f"Keepa API error: {data['error']}"
                products = data.get("products") or []
                if not products:
                    return [], "No products returned"
                p = products[0]
                related = set()
                for k in ("alsoBought","alsoViewed","frequentlyBoughtTogether","related"):
                    for x in (p.get(k) or []):
                        if isinstance(x, str) and len(x) == 10:
                            related.add(x.upper())
                related.discard(asin.upper())
                return list(sorted(related)), None
            except Exception as e:
                last_err = str(e)
                time.sleep(0.6)
        return [], f"Keepa request failed: {last_err}"
    # ---- 新增：BSR 历史序列（简版） ----
    def product_bsr_series(self, asin: str, domain_name: str | None = None):
        """
        返回 [(timestamp_ms, bsr), ...] 的简版列表。
        Keepa product 接口返回 salesRanks 历史数组，需要解析。
        说明：为保证速率，这里单次请求只取该 ASIN，按 cfg.history 参数控制是否带历史。
        """
        if not self.key:
            return []
        dom = self.domain_map.get(domain_name, 2) if domain_name else 2
        url = "https://api.keepa.com/product"
        params = {"key": self.key, "domain": dom, "asin": asin, "history": 1}
        last_err = None
        for _ in range(self.retries + 1):
            try:
                self.rl.wait()
                r = requests.get(url, params=params, timeout=self.timeout)
                r.raise_for_status()
                data = r.json()
                if "error" in data and data["error"]:
                    return []
                products = data.get("products") or []
                if not products:
                    return []
                p = products[0]
                # Keepa 历史：salesRanks 是 {categoryId: [time0, val0, time1, val1, ...]}
                ranks = p.get("salesRanks") or {}
                if not ranks:
                    return []
                # 选第一条类目的 ranks
                first = next(iter(ranks.values()))
                # Keepa 时间戳是相对时间（minutes since 1970-01-01? Keepa docs），这里只作相对序列返回
                series = []
                for i in range(0, len(first), 2):
                    t = first[i]
                    v = first[i+1] if i+1 < len(first) else None
                    series.append((t, v))
                return series
            except Exception as e:
                last_err = str(e)
                time.sleep(0.6)
        return []
