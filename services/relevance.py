import re
import pandas as pd

def _contains_any(text: str, terms: list[str]) -> bool:
    if not text: return False
    t = text.lower()
    return any(re.search(rf"\b{re.escape(w.lower())}\b", t) for w in terms)

def score_and_filter(df: pd.DataFrame, cfg) -> tuple[pd.DataFrame, pd.DataFrame]:
    ns = cfg.negatives_scan
    df = df.copy()
    for col in ["title"]:
        df[col] = df[col].fillna("")

    # brand 列可能为空，先创建
    if "brand" not in df.columns: df["brand"] = None
    if "category" not in df.columns: df["category"] = None

    # 1) 黑名单 & 白名单（白名单优先通过）
    def brand_flag(b):
        if not b: return "unknown"
        b_l = str(b).lower()
        if any(b_l == x.lower() for x in ns.brand_whitelist): return "whitelist"
        if any(b_l == x.lower() for x in ns.brand_blacklist): return "blacklist"
        return "ok"

    df["brand_flag"] = df["brand"].apply(brand_flag)

    # 2) exclude_terms 直接剔除（除非 brand 为白名单）
    df["exclude_hit"] = df["title"].apply(lambda t: _contains_any(t, ns.exclude_terms))

    # 3) include_terms 打分
    def include_score(t: str) -> int:
        return sum(1 for w in ns.include_terms if re.search(rf"\b{re.escape(w.lower())}\b", t.lower()))

    df["include_score"] = df["title"].apply(include_score)

    # 4) 数值阈值
    def ok_price(x):
        try:
            return ns.price_min <= float(x) <= ns.price_max
        except: return False

    def ok_rating(x):
        try:
            return float(x) >= ns.rating_min
        except: return False

    def ok_reviews(x):
        try:
            return int(x) >= ns.reviews_min
        except: return False

    df["price_ok"] = df["price"].apply(ok_price)
    df["rating_ok"] = df["rating"].apply(ok_rating)
    df["reviews_ok"] = df["reviews"].apply(ok_reviews)

    # whitelist 直接通过；blacklist 或 exclude_hit 剔除
    def pass_filter(row):
        if row["brand_flag"] == "whitelist":
            return True
        if row["brand_flag"] == "blacklist": return False
        if row["exclude_hit"]: return False
        return row["price_ok"] and (row["rating_ok"] or row["reviews_ok"])

    df["pass"] = df.apply(pass_filter, axis=1)

    # 5) 计算 RelevanceScore
    # include_score + (rating_ok) + (reviews_ok)
    df["RelevanceScore"] = df["include_score"] + df["rating_ok"].astype(int) + df["reviews_ok"].astype(int)

    kept = df[df["pass"]].copy()
    kept = kept.sort_values(by=["RelevanceScore","reviews","rating"], ascending=[False, False, False])
    dropped = df[~df["pass"]].copy()

    return kept.reset_index(drop=True), dropped.reset_index(drop=True)
