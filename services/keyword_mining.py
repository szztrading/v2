# -*- coding: utf-8 -*-
import re
import math
from collections import Counter, defaultdict
from typing import Dict, List, Tuple
import pandas as pd

# ---------------------------
# Built-in stopwords and noise filters
# ---------------------------
EN_STOPWORDS = set("""
a an the and or but if while as than then so very more most such each per
i you he she it we they me him her us them my your his her its our their
this that these those who whom whose which what where when why how
be am is are was were being been do does did doing have has had having
can could may might must shall should will would
for to from with without within into onto upon of in on at by over under
up down out off across through between among along around behind beyond
again further also only same other another any all some no nor not
there here above below before after during until against about
own once ever never always sometimes often usually
""".split())

# Domain-generic words you likely do not want to rank high
DOMAIN_NOISE = set("""
set kit stainless steel 304 home brewing brewer brewers brews beer
plastic glass rubber silver black white
""".split())

# Measurement / unit tokens and patterns
UNIT_TOKENS = set("l ml cl dl oz floz inch in cm mm m kg g lb lbs pack packs pcs piece pieces pair".split())
SIZE_PAT = re.compile(r"""
^(
    \d+([./-]\d+)?([./-]\d+)?     # 5, 5/16, 3-8, 10.5
    ([a-z]{1,4})?                 # optional unit suffix
  |
    [a-z]{1,4}\d+                 # unit prefix e.g. m6
)$
""", re.IGNORECASE | re.VERBOSE)

# UI noise blacklist (captions like "tap to read", "product description", etc.)
UI_NOISE_PATTERNS = [
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
UI_NOISE_RE = re.compile("|".join(UI_NOISE_PATTERNS), flags=re.I)

# ---------------------------
# Text cleaning / tokenizing
# ---------------------------
def _clean_text(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s\-\+./]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _tokenize(text: str, stop: set) -> List[str]:
    toks = _clean_text(text).split()
    toks = [t for t in toks if len(t) > 1]
    return toks

def _ngrams(tokens: List[str], n_min: int, n_max: int) -> List[List[str]]:
    res: List[List[str]] = []
    for n in range(n_min, n_max + 1):
        for i in range(0, len(tokens) - n + 1):
            res.append(tokens[i:i + n])
    return res

def _is_unit_token(t: str) -> bool:
    if t in UNIT_TOKENS:
        return True
    if SIZE_PAT.match(t):
        return True
    if re.match(r"^\d+(?:[./-]\d+)?(l|ml|cl|dl|oz|in|cm|mm|m|kg|g|lb|lbs)$", t):
        return True
    if re.match(r"^(in|cm|mm|m)\d+$", t):
        return True
    return False

def _ngram_to_text(ng: List[str]) -> str:
    return " ".join(ng)

def _is_noise_ngram(ng: List[str], stop_all: set) -> bool:
    """
    Decide whether an n-gram is noise:
    - matches UI noise
    - pure size/unit
    - leading/trailing stopwords, high stopword ratio
    - no alphabetic chars, too short after trim, punctuation-only chunks
    - domain-generic unigrams (e.g., 'set', 'kit', etc.)
    """
    if not ng:
        return True

    text = _ngram_to_text(ng)
    if UI_NOISE_RE.search(text):
        return True

    # strip size/unit tokens
    core = [t for t in ng if not _is_unit_token(t)]
    if not core:
        return True

    # unigram low-value terms
    if len(core) == 1:
        t = core[0]
        if t in stop_all or t.isdigit() or t in DOMAIN_NOISE:
            return True

    # starts or ends with stopword
    if core[0] in stop_all or core[-1] in stop_all:
        return True

    # too many stopwords inside
    sw_ratio = sum(1 for t in core if t in stop_all) / float(len(core))
    if sw_ratio >= 0.5:
        return True

    # must contain alpha
    if not re.search(r"[a-z]", _ngram_to_text(core)):
        return True

    # over-short after trimming
    if len(_ngram_to_text(core)) <= 2:
        return True

    # excessive punctuation artifacts
    if any(t in ("--", "-", "+") for t in core):
        return True

    return False

def _empty_kw_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "keyword",
            "score",
            "df",
            "tf_weighted",
            "title_hits",
            "bullet_hits",
            "aplus_hits",
            "sample_asins",
        ]
    )

# ---------------------------
# Main keyword mining
# ---------------------------
def mine_keywords_from_cluster(
    texts_by_asin: Dict[str, Dict[str, str]],
    cfg
) -> Tuple[pd.DataFrame, List[dict]]:
    """
    Inputs:
      texts_by_asin: {asin: {"title": str, "bullets": str, "aplus": str, "brand": str}}
      cfg: config.keyword_mining (pydantic model with defaults)
    Returns:
      kw_table: DataFrame columns:
        keyword, score, df, tf_weighted, title_hits, bullet_hits, aplus_hits, sample_asins
      debug_rows: list of small dicts for traceability
    """
    # merge built-in stopwords with user config
    user_sw = set(getattr(cfg, "stopwords", []) or [])
    stop_all = EN_STOPWORDS | user_sw

    # weights
    weight = getattr(cfg, "weight", None)
    if weight is None:
        w_title, w_bul, w_apl = 1.0, 0.7, 0.4
    else:
        w_title = float(getattr(weight, "title", 1.0))
        w_bul = float(getattr(weight, "bullets", 0.7))
        w_apl = float(getattr(weight, "aplus", 0.4))

    # n-gram range and thresholds
    ngram_range = getattr(cfg, "ngram_range", [1, 3]) or [1, 3]
    n_min = int(ngram_range[0]) if len(ngram_range) > 0 else 1
    n_max = int(ngram_range[1]) if len(ngram_range) > 1 else 3

    min_df_cfg = int(getattr(cfg, "min_df", 2))
    max_top = int(getattr(cfg, "max_top", 200))

    # counters
    df_count: Counter = Counter()
    tf_weighted: Counter = Counter()
    per_kw_asins: defaultdict = defaultdict(set)
    title_hits: Counter = Counter()
    bullet_hits: Counter = Counter()
    aplus_hits: Counter = Counter()
    debug_rows: List[dict] = []

    # iterate samples
    for asin, parts in (texts_by_asin or {}).items():
        title = (parts.get("title") or "").strip()
        bullets = (parts.get("bullets") or "").strip()
        aplus = (parts.get("aplus") or "").strip()

        toks_t = _tokenize(title, stop_all)
        toks_b = _tokenize(bullets, stop_all)
        toks_a = _tokenize(aplus, stop_all)

        # generate and filter n-grams
        grams_t = []
        for g in _ngrams(toks_t, n_min, n_max):
            if not _is_noise_ngram(g, stop_all):
                grams_t.append(tuple(g))

        grams_b = []
        for g in _ngrams(toks_b, n_min, n_max):
            if not _is_noise_ngram(g, stop_all):
                grams_b.append(tuple(g))

        grams_a = []
        for g in _ngrams(toks_a, n_min, n_max):
            if not _is_noise_ngram(g, stop_all):
                grams_a.append(tuple(g))

        seen_in_asin = set()

        # title
        for g in set(grams_t):
            key = " ".join(g)
            tf_weighted[key] += w_title
            title_hits[key] += 1
            per_kw_asins[key].add(asin)
            seen_in_asin.add(key)

        # bullets
        for g in set(grams_b):
            key = " ".join(g)
            tf_weighted[key] += w_bul
            bullet_hits[key] += 1
            per_kw_asins[key].add(asin)
            seen_in_asin.add(key)

        # aplus
        for g in set(grams_a):
            key = " ".join(g)
            tf_weighted[key] += w_apl
            aplus_hits[key] += 1
            per_kw_asins[key].add(asin)
            seen_in_asin.add(key)

        for key in seen_in_asin:
            df_count[key] += 1

        debug_rows.append({
            "asin": asin,
            "title": title[:200],
            "bullets": bullets[:200],
            "aplus": aplus[:200],
        })

    # builder with UI-noise and A+-only cleanup
    def _build_df(min_df_value: int) -> pd.DataFrame:
        rows: List[dict] = []
        for kw, dfv in df_count.items():
            if dfv < min_df_value:
                continue
            rows.append({
                "keyword": kw,
                "score": round(float(tf_weighted[kw] * (1.0 + math.log1p(dfv))), 4),
                "df": int(dfv),
                "tf_weighted": round(float(tf_weighted[kw]), 4),
                "title_hits": int(title_hits[kw]),
                "bullet_hits": int(bullet_hits[kw]),
                "aplus_hits": int(aplus_hits[kw]),
                "sample_asins": ",".join(list(per_kw_asins[kw])[:8]),
            })
        if not rows:
            return _empty_kw_df()

        df = pd.DataFrame(rows)

        # First-pass UI noise filter on the table
        # IMPORTANT: do NOT pass 'case' when using a compiled regex; just use na=False
        df = df[~df["keyword"].astype(str).str.contains(UI_NOISE_RE, na=False)]

        # Drop phrases that only appear in A+ but never in title/bullets (likely UI/helper text)
        if {"title_hits", "bullet_hits", "aplus_hits"}.issubset(df.columns):
            mask_ui_like = (df["title_hits"] == 0) & (df["bullet_hits"] == 0) & (df["aplus_hits"] > 0)
            df = df[~mask_ui_like]

        # Sort by score/df/tf_weighted
        sort_cols = [c for c in ["score", "df", "tf_weighted"] if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols, ascending=[False] * len(sort_cols))
        df = df.reset_index(drop=True)

        if len(df) > max_top:
            df = df.head(max_top).copy()
        return df

    # Build with configured min_df
    kw_table = _build_df(min_df_cfg)

    # Fallback: if empty but grams exist, retry with min_df=1
    if kw_table.empty and len(df_count) > 0 and min_df_cfg > 1:
        kw_table = _build_df(1)

    # Final fallback: structure even when completely empty
    if kw_table.empty and len(df_count) == 0:
        kw_table = _empty_kw_df()

    return kw_table, debug_rows

# ---------------------------
# Optional: BSR sync proxy
# ---------------------------
def attach_bsr_signal(
    kw_table: pd.DataFrame,
    candidate_asins: List[str],
    keepa_client,
    window: int = 5,
    domain_name: str | None = None
) -> pd.DataFrame:
    """
    Compute a coarse BSR sync proxy from candidate_asins using Keepa BSR series.
    This is not causal; it is a lightweight market heat proxy.
    """
    deltas: List[float] = []
    for a in candidate_asins:
        try:
            series = keepa_client.product_bsr_series(a, domain_name=domain_name)
            if not series:
                continue
            series = sorted(series, key=lambda x: x[0])
            if len(series) < 2:
                continue
            last = series[-1][1]
            prev = series[-min(len(series), window)][1]
            if last and prev:
                deltas.append(prev - last)  # positive => BSR down (rank improved)
        except Exception:
            # ignore per-asin errors
            pass

    avg_delta = (sum(deltas) / len(deltas)) if deltas else 0.0
    label = "Neutral"
    if avg_delta > 0:
        label = "Positive"
    elif avg_delta < 0:
        label = "Negative"

    kw_table = kw_table.copy()
    kw_table["bsr_sync_signal"] = label
    kw_table["bsr_avg_delta_sample"] = round(avg_delta, 2)
    return kw_table
