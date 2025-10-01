import re
import math
from collections import Counter, defaultdict
from typing import Dict, List, Tuple
import pandas as pd

# --------- 文本清洗 & token 化 ----------
def _clean_text(s: str) -> str:
    if not s: return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s\-+./]", " ", s)   # 保留常见分隔符
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _ngrams(tokens: List[str], n_min: int, n_max: int) -> List[str]:
    res = []
    for n in range(n_min, n_max+1):
        for i in range(len(tokens)-n+1):
            g = " ".join(tokens[i:i+n])
            res.append(g)
    return res

def _tokenize(text: str, stop: set) -> List[str]:
    toks = _clean_text(text).split()
    toks = [t for t in toks if t not in stop and len(t) > 1]
    return toks

# --------- 主流程 ----------
def mine_keywords_from_cluster(
    texts_by_asin: Dict[str, Dict[str, str]],
    cfg
) -> Tuple[pd.DataFrame, List[dict]]:
    """
    输入：
      texts_by_asin: {asin: {"title": str, "bullets": str, "aplus": str, "brand": str}}
      cfg: config.keyword_mining
    输出：
      kw_table: DataFrame[ keyword, score, df(出现产品数), tf(频次), title_hits, bullet_hits, aplus_hits, sample_asins ]
      debug_rows: 采样可追溯
    """
    stop = set((cfg.stopwords or []))
    w_title = float(cfg.weight["title"])
    w_bul = float(cfg.weight["bullets"])
    w_apl = float(cfg.weight["aplus"])

    n_min, n_max = int(cfg.ngram_range[0]), int(cfg.ngram_range[1])
    min_df = int(cfg.min_df)
    max_top = int(cfg.max_top)

    # 统计
    df_count = Counter()
    tf_weighted = Counter()
    per_kw_asins = defaultdict(set)
    title_hits = Counter()
    bullet_hits = Counter()
    aplus_hits = Counter()
    debug_rows = []

    for asin, parts in texts_by_asin.items():
        title = parts.get("title") or ""
        bullets = parts.get("bullets") or ""
        aplus = parts.get("aplus") or ""

        toks_t = _tokenize(title, stop)
        toks_b = _tokenize(bullets, stop)
        toks_a = _tokenize(aplus, stop)

        grams = []
        grams += _ngrams(toks_t, n_min, n_max)
        grams += _ngrams(toks_b, n_min, n_max)
        grams += _ngrams(toks_a, n_min, n_max)

        # 分来源累加加权 TF
        seen_in_asin = set()
        # title
        for g in set(_ngrams(toks_t, n_min, n_max)):
            tf_weighted[g] += w_title
            title_hits[g] += 1
            per_kw_asins[g].add(asin)
            seen_in_asin.add(g)
        # bullets
        for g in set(_ngrams(toks_b, n_min, n_max)):
            tf_weighted[g] += w_bul
            bullet_hits[g] += 1
            per_kw_asins[g].add(asin)
            seen_in_asin.add(g)
        # aplus
        for g in set(_ngrams(toks_a, n_min, n_max)):
            tf_weighted[g] += w_apl
            aplus_hits[g] += 1
            per_kw_asins[g].add(asin)
            seen_in_asin.add(g)

        for g in seen_in_asin:
            df_count[g] += 1

        debug_rows.append({
            "asin": asin,
            "title": title[:160],
            "bullets": bullets[:160],
            "aplus": aplus[:160],
        })

    rows = []
    for kw, dfv in df_count.items():
        if dfv < min_df:  # 过滤低覆盖词
            continue
        score = tf_weighted[kw] * (1.0 + math.log1p(dfv))  # 简单 TF * log(DF) 打分
        rows.append({
            "keyword": kw,
            "score": round(score, 4),
            "df": dfv,
            "tf_weighted": round(tf_weighted[kw], 3),
            "title_hits": title_hits[kw],
            "bullet_hits": bullet_hits[kw],
            "aplus_hits": aplus_hits[kw],
            "sample_asins": ",".join(list(per_kw_asins[kw])[:8]),
        })

    kw_table = pd.DataFrame(rows).sort_values(["score","df","tf_weighted"], ascending=[False, False, False]).reset_index(drop=True)
    if len(kw_table) > max_top:
        kw_table = kw_table.head(max_top).copy()
    return kw_table, debug_rows

# --------- BSR 同步信号（可选） ----------
def attach_bsr_signal(
    kw_table: pd.DataFrame,
    candidate_asins: List[str],
    keepa_client,
    window: int = 5,
    domain_name: str | None = None
) -> pd.DataFrame:
    """
    对关键词打“BSR 同步信号”标签：
    简化实现：对候选 ASIN 取最近 window 段 BSR 变化（下降=销量信号），
    作为整体市场热度 proxy（非严格因果，仅做佐证）。
    """
    deltas = []
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
                deltas.append(prev - last)  # 正数=BSR下降（排名提升）
        except Exception:
            pass
    avg_delta = (sum(deltas)/len(deltas)) if deltas else 0.0
    kw_table["bsr_sync_signal"] = "Neutral" if avg_delta == 0 else ("Positive" if avg_delta > 0 else "Negative")
    kw_table["bsr_avg_delta_sample"] = round(avg_delta, 2)
    return kw_table
