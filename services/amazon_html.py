# -*- coding: utf-8 -*-
"""
Amazon HTML fallback utilities:
- search_related_html(seed_asin, domain_name) -> List[str]
- enrich_product_info(asins, domain_name) -> DataFrame[asin,title,price,rating,reviews,url,brand,category]
- scrape_listing_text(asins, domain_name) -> Dict[asin] = {title, bullets, aplus, brand}

Notes:
- Mobile page (/gp/aw/d/<ASIN>) is simpler to parse; desktop (/dp/<ASIN>) is used as fallback.
- This code is lightweight and best-effort; Amazon may rate limit or show bot checks.
- For production, consider adding proxy/rotating headers and stricter error handling.
"""

from __future__ import annotations
import re
import time
import random
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
import pandas as pd

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]

HEADERS_BASE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

DP_URL = "https://{domain}/dp/{asin}?th=1&psc=1"
MOBILE_URL = "https://{domain}/gp/aw/d/{asin}"

ASIN_RE = re.compile(r"/dp/([A-Z0-9]{10})")
STAR_RE = re.compile(r"([0-9.]+)\s+out of 5")
INT_RE = re.compile(r"([0-9][0-9,\.]*)")

# ---------------------------
# A+ UI noise blacklist
# ---------------------------
APLUIS_PATTERNS = [
    r"\bproduct description\b",
    r"\bbrief content\b",
    r"\bread (brief|full) content\b",
    r"\btap to read\b",
    r"\bdouble tap\b",
    r"\bcontent visible\b",
    r"\bvisit the store\b",
    r"\bread more\b",
    r"\bsee more\b",
]
APLUIS_RE = re.compile("|".join(APLUIS_PATTERNS), flags=re.I)

def _strip_ui_lines(txt: str) -> str:
    if not txt:
        return ""
    # split by sentence or pipe and drop UI noise sentences
    parts = re.split(r"(?<=[.!?])\s+|\s*\|\s*", txt)
    clean = [p.strip() for p in parts if p and not APLUIS_RE.search(p)]
    return " | ".join(clean)

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS_BASE.copy())
    return s

def _get(session: requests.Session, url: str, timeout: int = 20, retries: int = 2, qps: float = 1.2) -> Optional[str]:
    last_err = None
    gap = 1.0 / max(qps, 0.
