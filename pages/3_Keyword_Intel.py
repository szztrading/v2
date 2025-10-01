import streamlit as st
import pandas as pd
from datetime import datetime

from core.config import load_config
from core.exporters import to_csv_bytes, to_csv_bytes_many
from services.keepa_client import KeepaClient
from services.amazon_html import (
    search_related_html,
    enrich_product_info,
    scrape_listing_text,
)
from services.keyword_mining import (
    mine_keywords_from_cluster,
    attach_bsr_signal,
)

st.set_page_config(page_title="Keyword Intelligence", page_icon="🔎", layout="wide")
st.title("🔎 竞品关键词 · 逆向分析（Keepa + HTML + 词频）")

# ----------------------------
# 加载配置（确保 keyword_mining 段存在）
# ----------------------------
cfg = load_config("config.yaml")

# 侧边参数
domain_name = st.selectbox("站点", list(cfg.keepa.domain_map.keys()), index=0)
seed_asin = st.text_input("输入种子 ASIN（如 B08CH9HFSC）").strip().upper()
max_cluster = st.slider("合并后最多纳入多少个相关竞品", 10, 300, 80, 10)

colA, colB = st.columns([1, 1])
with colA:
    do_keepa = st.checkbox("使用 Keepa 相关 ASIN 通道", value=True)
with colB:
    do_html = st.checkbox("使用 HTML 回退通道", value=True)

run = st.button("开始分析", disabled=(not seed_asin))

if run:
    # ----------------------------
    # 1) 相关 ASIN 聚类（Keepa + HTML）
    # ----------------------------
    all_asins = set()
    keepa_err = None
    kc = None  # 占位，便于后续 BSR 信号阶段判断

    if do_keepa:
        with st.spinner("Keepa 抓取相关 ASIN..."):
            kc = KeepaClient(cfg.keepa.api_key, cfg.keepa.domain_map, cfg.keepa.timeout, cfg.keepa.retries)
            rel, keepa_err = kc.product_related(seed_asin, domain_name, cfg.keepa.history)
            if rel:
                all_asins.update(rel)
    if keepa
