# -*- coding: utf-8 -*-
"""
Amazon HTML fallback utilities:
- search_related_html(seed_asin, domain_name) -> List[str]
- enrich_product_info(asins, domain_name) -> DataFrame[asin,title,price,rating,reviews,url,brand,category]
- scrape_listing_text(asins, domain_name) -> Dict[asin] = {title, bullets, aplus, brand}
Notes:
- Mobile page (/gp/aw/d/<ASIN>) is simpler to parse; desktop (/dp/<ASIN>) used as fallback.
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
    # A few common desktop and mobile UAs (rotate randomly per request)
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

def _make_session() -> requests.Session:
    s = requests.Session()
    # Per-request we will also randomize UA again.
    s.headers.update(HEADERS_BASE.copy())
    return s

def _get(session: requests.Session, url: str, timeout: int = 20, retries: int = 2, qps: float = 1.2) -> Optional[str]:
    last_err = None
    gap = 1.0 / max(qps, 0.1)
    for i in range(retries + 1):
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            resp = session.get(url, headers=headers, timeout=timeout)
            txt = resp.text or ""
            if resp.status_code == 200 and ("Robot Check" not in txt and "captcha" not in txt.lower()):
                return txt
            last_err = f"status {resp.status_code}"
        except Exception as e:
            last_err = str(e)
        time.sleep(gap * (1.0 + 0.5 * i))
    return None

def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")

# ---------------------------
# Parsing helpers
# ---------------------------
def _extract_title(soup: BeautifulSoup) -> Optional[str]:
    # Desktop
    t = soup.select_one("#productTitle")
    if t and t.get_text(strip=True):
        return t.get_text(strip=True)
    # Mobile
    t = soup.select_one("#title") or soup.select_one("h1")
    if t and t.get_text(strip=True):
        return t.get_text(strip=True)
    return None

def _extract_bullets(soup: BeautifulSoup) -> str:
    # Desktop feature bullets
    blocks = []
    ul = soup.select_one("#feature-bullets")
    if ul:
        items = ul.select("li")
        for li in items:
            txt = li.get_text(" ", strip=True)
            if txt:
                blocks.append(txt)
    # Mobile bullets (best-effort)
    if not blocks:
        # Many mobile templates render bullet lists under divs containing 'feature' in id
        for div in soup.select("div[id*='feature'] ul, div[id*='feature'] li"):
            txt = div.get_text(" ", strip=True)
            if txt and len(txt) > 3:
                blocks.append(txt)
    # Fallback: important info list
    if not blocks:
        for li in soup.select("li.a-list-item"):
            txt = li.get_text(" ", strip=True)
            if txt and len(txt) > 10:
                blocks.append(txt)
                if len(blocks) >= 6:
                    break
    return " | ".join(dict.fromkeys(blocks))[:2000]

def _extract_aplus(soup: BeautifulSoup) -> str:
    # Desktop A+ often in #aplus / #aplus_feature_div blocks
    blocks = []
    for sel in ["#aplus", "#aplus_feature_div", "div[id*='aplus']"]:
        for div in soup.select(sel):
            txt = div.get_text(" ", strip=True)
            if txt and len(txt) > 30:
                blocks.append(txt)
    # Mobile: some aplus-like sections are expander blocks
    if not blocks:
        for div in soup.select("div[data-a-expander-name]"):
            if "aplus" in (div.get("data-a-expander-name") or "").lower():
                txt = div.get_text(" ", strip=True)
                if txt and len(txt) > 30:
                    blocks.append(txt)
    return " | ".join(blocks)[:3000]

def _extract_brand(soup: BeautifulSoup) -> Optional[str]:
    # Byline
    a = soup.select_one("#bylineInfo")
    if a and a.get_text(strip=True):
        return a.get_text(strip=True).replace("Brand: ", "").replace("Visit the ", "").replace(" Store", "").strip()
    # Product details table (desktop)
    rows = soup.select("#productDetails_techSpec_section_1 tr, #productDetails_detailBullets_sections1 tr")
    for tr in rows:
        th = tr.select_one("th")
        td = tr.select_one("td")
        if th and td and "brand" in th.get_text(" ", strip=True).lower():
            return td.get_text(" ", strip=True)
    # Detail bullets (mobile/desktop)
    for li in soup.select("#detailBullets_feature_div li"):
        txt = li.get_text(" ", strip=True)
        if ":" in txt and "brand" in txt.lower():
            return txt.split(":", 1)[1].strip()
    return None

def _extract_price(soup: BeautifulSoup) -> Optional[float]:
    # Desktop price
    for sel in ["#priceblock_ourprice", "#priceblock_dealprice", "span.a-price > span.a-offscreen", "span.a-price-whole"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            txt = el.get_text(strip=True)
            val = _parse_price(txt)
            if val:
                return val
    # Mobile price
    el = soup.select_one("span.a-color-price")
    if el and el.get_text(strip=True):
        val = _parse_price(el.get_text(strip=True))
        if val:
            return val
    return None

def _parse_price(txt: str) -> Optional[float]:
    # Remove currency and separators
    t = txt.replace(",", "").replace("£", "").replace("$", "").replace("€", "").strip()
    m = re.search(r"([0-9]+(\.[0-9]+)?)", t)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None

def _extract_rating(soup: BeautifulSoup) -> Optional[float]:
    # Desktop and mobile
    el = soup.select_one("#acrPopover") or soup.select_one("span.a-icon-alt")
    if el and el.get_text(strip=True):
        m = STAR_RE.search(el.get_text(strip=True))
        if m:
            try:
                return float(m.group(1))
            except Exception:
                return None
    return None

def _extract_reviews_count(soup: BeautifulSoup) -> Optional[int]:
    el = soup.select_one("#acrCustomerReviewText") or soup.select_one("span[data-hook='total-review-count']")
    if el and el.get_text(strip=True):
        m = INT_RE.search(el.get_text(strip=True).replace(",", ""))
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except Exception:
                return None
    # Mobile alternative
    el = soup.select_one("a[href*='#customerReviews'] span")
    if el and el.get_text(strip=True):
        m = INT_RE.search(el.get_text(strip=True).replace(",", ""))
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except Exception:
                return None
    return None

def _extract_related_asins(soup: BeautifulSoup) -> List[str]:
    asins = set()
    # data-asin attributes in carousels
    for el in soup.select("[data-asin]"):
        a = (el.get("data-asin") or "").strip().upper()
        if len(a) == 10 and a.startswith("B0"):
            asins.add(a)
    # links containing /dp/ASIN
    for a in soup.select("a[href*='/dp/']"):
        href = a.get("href") or ""
        m = ASIN_RE.search(href)
        if m:
            asins.add(m.group(1).upper())
    return list(asins)

# ---------------------------
# Public API
# ---------------------------
def search_related_html(seed_asin: str, domain_name: str) -> List[str]:
    """
    Fetch related ASINs using desktop dp page (more modules) and fallback to mobile.
    """
    s = _make_session()
    urls = [
        DP_URL.format(domain=domain_name, asin=seed_asin),
        MOBILE_URL.format(domain=domain_name, asin=seed_asin),
    ]
    for url in urls:
        html = _get(s, url, timeout=20, retries=2, qps=1.2)
        if not html:
            continue
        soup = _soup(html)
        rel = _extract_related_asins(soup)
        if rel:
            return sorted(set(rel))
    return []

def enrich_product_info(asins: List[str], domain_name: str) -> pd.DataFrame:
    """
    For each ASIN, fetch title, price, rating, reviews, brand. Desktop first, then mobile fallback.
    Returns DataFrame with columns:
      asin, title, price, rating, reviews, url, brand, category
    """
    s = _make_session()
    rows = []
    for a in asins:
        title = price = rating = reviews = brand = None
        url_used = ""
        # Try desktop first
        url = DP_URL.format(domain=domain_name, asin=a)
        html = _get(s, url, timeout=20, retries=2, qps=1.2)
        if html:
            soup = _soup(html)
            title = _extract_title(soup)
            price = _extract_price(soup)
            rating = _extract_rating(soup)
            reviews = _extract_reviews_count(soup)
            brand = _extract_brand(soup)
            url_used = url
        # Fallback to mobile if still missing crucial fields
        if not title or rating is None:
            url2 = MOBILE_URL.format(domain=domain_name, asin=a)
            html2 = _get(s, url2, timeout=20, retries=2, qps=1.2)
            if html2:
                soup2 = _soup(html2)
                title = title or _extract_title(soup2)
                price = price or _extract_price(soup2)
                rating = rating if rating is not None else _extract_rating(soup2)
                reviews = reviews if reviews is not None else _extract_reviews_count(soup2)
                brand = brand or _extract_brand(soup2)
                if not url_used:
                    url_used = url2

        rows.append({
            "asin": a,
            "title": title,
            "price": price,
            "rating": rating,
            "reviews": reviews,
            "url": url_used or DP_URL.format(domain=domain_name, asin=a),
            "brand": brand,
            "category": None,
        })
        time.sleep(0.5)  # be gentle
    return pd.DataFrame(rows)

def scrape_listing_text(asins: List[str], domain_name: str) -> Dict[str, Dict[str, str]]:
    """
    Return {asin: {"title": str, "bullets": str, "aplus": str, "brand": str}}.
    We fetch mobile first (lighter), then desktop as fallback, and merge fields.
    """
    s = _make_session()
    out: Dict[str, Dict[str, str]] = {}
    for a in asins:
        title = bullets = aplus = brand = None

        # Mobile first
        url_m = MOBILE_URL.format(domain=domain_name, asin=a)
        html_m = _get(s, url_m, timeout=20, retries=2, qps=1.2)
        if html_m:
            sm = _soup(html_m)
            title = _extract_title(sm) or title
            bullets = _extract_bullets(sm) or bullets
            aplus = _extract_aplus(sm) or aplus
            brand = _extract_brand(sm) or brand

        # Desktop fallback / merge
        url_d = DP_URL.format(domain=domain_name, asin=a)
        html_d = _get(s, url_d, timeout=20, retries=2, qps=1.2)
        if html_d:
            sd = _soup(html_d)
            title = title or _extract_title(sd)
            # Prefer desktop bullets if mobile empty
            if not bullets:
                bullets = _extract_bullets(sd)
            # Append aplus if desktop found more
            apl2 = _extract_aplus(sd)
            if apl2:
                if aplus:
                    if apl2 not in aplus:
                        aplus = (aplus + " | " + apl2)[:4000]
                else:
                    aplus = apl2
            brand = brand or _extract_brand(sd)

        out[a] = {
            "title": title or "",
            "bullets": bullets or "",
            "aplus": aplus or "",
            "brand": brand or "",
        }
        time.sleep(0.5)
    return out
