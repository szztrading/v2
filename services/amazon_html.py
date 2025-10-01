"""
HTML 回退：占位实现。
根据你 v1 的解析逻辑，抓取 dp 页/移动页，抽取 related ASIN 与基本信息。
这里给出最小可运行骨架（返回空/伪数据），你可替换为真实解析。
"""
from typing import List
import pandas as pd

def search_related_html(seed_asin: str, domain_name: str) -> List[str]:
    # TODO: 用 requests + bs4 抓取 /dp/ 与移动页 /gp/aw/d/ASIN 提取相关 ASIN
    return []  # 骨架先返回空，保证流程可跑

def enrich_product_info(asins: list[str], domain_name: str) -> pd.DataFrame:
    # TODO: HTML 抓取 title/price/rating/reviews（你已有代码可迁移）
    rows = []
    for a in asins:
        rows.append({
            "asin": a,
            "title": None,
            "price": None,
            "rating": None,
            "reviews": None,
            "url": f"https://{domain_name}/dp/{a}"
        })
    return pd.DataFrame(rows)
