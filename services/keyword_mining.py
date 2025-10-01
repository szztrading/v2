# -*- coding: utf-8 -*-
import re
import math
from collections import Counter, defaultdict
from typing import Dict, List, Tuple
import pandas as pd

# ---------------------------
# Text cleaning and tokenizing
# ---------------------------
def _clean_text(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    # keep letters, digits, spaces, and a few separators
    s = re.sub(r"[^a-z0-9\s\-\+./]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _tokenize(text: str, stop: set) -> List[str]:
    toks = _clean_text(text).split()
    toks = [t for t in toks if t not in stop and len(t) > 1]
    return toks

def _ngrams(tokens: List[str], n_min: int, n_max: int) -> List[str]:
    res: List[str] = []
    for n in range(n_min, n_max + 1):
        for i in range(0, len(tokens) - n + 1):
            res.append(" ".join(tokens[i:i + n]))
    return res

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
    stop = set(getattr(cfg, "stopwords", []) or [])

    weight = getattr(cfg, "weight", None)
    if weight is None:
        # safety defaults
        w_title, w_bul, w_apl = 1.0, 0.7, 0.4
    else:
        w_title = float(getattr(weight, "title", 1.0))
        w_bul = float(getattr(weight, "bullets", 0.7))
        w_apl = float(getattr(weight, "aplus", 0.4))

    ngram_range = getattr(cfg, "ngram_range", [1, 3]) or [1, 3]
    n_min = int(ngram_range[0]) if len(ngram_range) > 0 else 1
    n_max = int(ngram_range[1]) if len(ngram_range) > 1 else 3

    min_df_cfg = int(getattr(cfg, "min_df", 2))
    max_top = int(getattr(cfg, "max_top", 200))

    df_count: Counter = Counter()
    tf_weighted: Counter = Counter()
    per_kw_asins: defaultdict = defaultdict(set)
    title_hits: Counter = Counter()
    bullet_hits: Counter = Counter()
    aplus_hits: Counter = Counter()
    debug_rows: List[dict] = []

    # build stats
    for asin, parts in (texts_by_asin or {}).items():
        title = (parts.get("title") or "").strip()
        bullets = (parts.get("bullets") or "").strip()
        aplus = (parts.get("aplus") or "").strip()

        toks_t = _tokenize(title, stop)
        toks_b = _tokenize(bullets, stop)
        toks_a = _tokenize(aplus, stop)

        grams_t = set(_ngrams(toks_t, n_min, n_max))
        grams_b = set(_ngrams(toks_b, n_min, n_max))
        grams_a = set(_ngrams(toks_a, n_min, n_max))

        seen_in_asin = set()

        # title
        for g in grams_t:
            tf_weighted[g] += w_title
            title_hits[g] += 1
            per_kw_asins[g].add(asin)
            seen_in_asin.add(g)

        # bullets
        for g in grams_b:
            tf_weighted[g] += w_bul
            bullet_hits[g] += 1
            per_kw_asins[g].add(asin)
            seen_in_asin.add(g)

        # aplus
        for g in grams_a:
            tf_weighted[g] += w_apl
            aplus_hits[g] += 1
            per_kw_asins[g].add(asin)
            seen_in_asin.add(g)

        for g in seen_in_asin:
            df_count[g] += 1

        debug_rows.append({
            "asin": asin,
            "title": title[:200],
            "bullets": bullets[:200],
            "aplus": aplus[:200],
        })

    # helper to build dataframe from current counters with a given min_df
    def _build_df(min_df_value: int) -> pd.DataFrame:
        rows: List[dict] = []
        for kw, dfv in df_count.items():
            if dfv < min_df_value:
                continue
            score = tf_weighted[kw] * (1.0 + math.log1p(dfv))
            rows.append({
                "keyword": kw,
                "score": round(float(score), 4),
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
        # safe sort: only sort by columns that exist in df
        sort_cols = [c for c in ["score", "df", "tf_weighted"] if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols, ascending=[False] * len(sort_cols))
        df = df.reset_index(drop=True)
        if len(df) > max_top:
            df = df.head(max_top).copy()
        return df

    # first attempt with configured min_df
    kw_table = _build_df(min_df_cfg)

    # fallback: if empty but we actually have any grams counted, retry with min_df = 1
    if kw_table.empty and len(df_count) > 0 and min_df_cfg > 1:
        kw_table = _build_df(1)

    # final fallback: ensure structure even if completely empty (no grams)
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
    This is not a causal metric; it is a lightweight market heat proxy.
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
            # ignore errors per asin
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
