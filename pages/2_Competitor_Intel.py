import pandas as pd
import streamlit as st
from core.config import load_config
from services.keepa_client import KeepaClient
from services.amazon_html import search_related_html, enrich_product_info
from services.relevance import score_and_filter
from core.exporters import to_csv_bytes

st.set_page_config(page_title="Competitor Intelligence", page_icon="🕵️", layout="wide")
st.title("🕵️ Competitor Intelligence")

cfg = load_config("config.yaml")
domain_name = st.selectbox("站点", list(cfg.keepa.domain_map.keys()), index=0)
asin = st.text_input("输入种子 ASIN（如 B08CH9HFSC）").strip().upper()
max_n = st.slider("最大抓取数量（最终显示前 N 条按相关性排序）", 20, 300, 120, 10)

if st.button("开始抓取", disabled=not asin):
    with st.spinner("Keepa 抓取中..."):
        kc = KeepaClient(cfg.keepa.api_key, cfg.keepa.domain_map, cfg.keepa.timeout, cfg.keepa.retries)
        related, err = kc.product_related(asin, domain_name, cfg.keepa.history)
    if err:
        st.warning(f"Keepa 提示：{err}")

    # HTML 回退补充
    with st.spinner("HTML 回退抓取中..."):
        html_related = search_related_html(asin, domain_name)

    all_asins = sorted(set([x for x in (related or [])] + [y for y in (html_related or [])]))
    if asin in all_asins:
        all_asins.remove(asin)
    st.write(f"合并去重后 ASIN 数：{len(all_asins)}")

    with st.spinner("补全商品信息..."):
        df = enrich_product_info(all_asins, domain_name)
    st.dataframe(df.head(50), use_container_width=True)

    with st.spinner("相关性打分与过滤..."):
        kept, dropped = score_and_filter(df, cfg)

    st.subheader("✅ 推荐清单（按 RelevanceScore 排序）")
    st.dataframe(kept.head(max_n), use_container_width=True)
    st.download_button("⬇️ 下载推荐清单 CSV", to_csv_bytes(kept.head(max_n)), "competitors_recommended.csv", "text/csv")

    st.subheader("📦 全量结果")
    st.dataframe(df, use_container_width=True)
    st.download_button("⬇️ 下载全量 CSV", to_csv_bytes(df), "competitors_full.csv", "text/csv")

    st.subheader("🗂️ 被过滤列表（原因可能：品牌黑名单/排除词/阈值）")
    st.dataframe(dropped.head(200), use_container_width=True)
