import pandas as pd

def load_search_terms(file) -> pd.DataFrame:
    # 兼容 CSV/XLSX
    try:
        df = pd.read_csv(file)
    except Exception:
        file.seek(0)
        df = pd.read_excel(file)

    # 尽量标准化常见字段名（你可根据自己的导出模板补充映射）
    rename_map = {
        "Customer Search Term":"keyword",
        "Search term":"keyword",
        "Keyword text":"keyword",
        "Match Type":"match_type",
        "Match type":"match_type",
        "Campaign Name":"campaign",
        "Ad Group Name":"ad_group",
        "Impressions":"impressions",
        "Clicks":"clicks",
        "Orders (Total)":"orders",
        "7 Day Total Orders (#)":"orders",
        "Spend":"spend",
        "7 Day Total Sales ":"sales",
        "7 Day Total Sales ($)":"sales",
        "Sales":"sales",
    }
    for col in list(rename_map):
        if col in df.columns:
            df.rename(columns={col: rename_map[col]}, inplace=True)

    # 缺失列填充
    for col in ["keyword","match_type","campaign","ad_group","impressions","clicks","orders","spend","sales"]:
        if col not in df.columns:
            df[col] = 0 if col in ("impressions","clicks","orders","spend","sales") else ""

    # 类型转换
    num_cols = ["impressions","clicks","orders","spend","sales"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["keyword"] = df["keyword"].astype(str)
    df["match_type"] = df["match_type"].astype(str)
    df["campaign"] = df["campaign"].astype(str)
    df["ad_group"] = df["ad_group"].astype(str)

    return df
