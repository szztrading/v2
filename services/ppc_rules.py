import pandas as pd

def make_recommendations(df: pd.DataFrame, cfg) -> dict[str, pd.DataFrame]:
    """
    输入：标准化后的 Search Term DataFrame，需包含：
    clicks, impressions, orders, spend, sales, keyword, match_type, campaign, ad_group
    """
    df = df.copy()
    df["ctr"] = df["impressions"].replace(0, 1)
    df["ctr"] = df["clicks"] / df["impressions"].clip(lower=1)
    df["cvr"] = df["orders"] / df["clicks"].clip(lower=1)
    df["acos"] = df["spend"] / df["sales"].clip(lower=1e-9)

    t = cfg.thresholds
    target_acos = cfg.target_acos
    up_pct = cfg.bid_steps.up_pct
    down_pct = cfg.bid_steps.down_pct

    # Scale Up
    up = df[
        (df["cvr"] >= t.harvest_cvr) &
        (df["acos"] <= target_acos) &
        (df["orders"] >= t.min_conversions)
    ].copy()
    up["action"] = f"Increase bid by {int(up_pct*100)}%"

    # Bid Down
    down = df[
        (df["clicks"] >= t.min_clicks) &
        ((df["orders"] < t.min_conversions) | (df["acos"] > target_acos*1.2))
    ].copy()
    down["action"] = f"Decrease bid by {int(down_pct*100)}%"

    # Negatives：点击多、无转化
    neg = df[
        (df["clicks"] >= t.min_clicks) &
        (df["orders"] == 0)
    ].copy()
    neg["neg_type"] = neg["match_type"].map(lambda m: "Negative Exact" if str(m).lower()=="exact" else "Negative Phrase")
    neg = neg[["campaign","ad_group","keyword","neg_type","clicks","spend"]].copy()

    # Harvest（高 cvr 但展示/覆盖有限）
    harv = df[
        (df["cvr"] >= t.harvest_cvr) &
        (df["impressions"] < df["impressions"].median())
    ].copy()
    harv["plan"] = "Create SKAG + add neg-exact in origin"

    return {
        "bid_up_recommendations": up.sort_values(by="cvr", ascending=False),
        "bid_down_recommendations": down.sort_values(by="acos", ascending=False),
        "negatives_upload": neg.sort_values(by="clicks", ascending=False),
        "harvest_plan": harv.sort_values(by="cvr", ascending=False)
    }
