import streamlit as st
import pandas as pd

from core.config import load_config
from services.keepa_client import KeepaClient
from services.amazon_html import search_related_html, enrich_product_info, scrape_listing_text
from services.keyword_mining import mine_keywords_from_cluster, attach_bsr_signal
from core.exporters import to_csv_bytes, to_csv_bytes_many

st.set_page_config(page_title="Keyword Intelligence", page_icon="🔎", layout="wide")
st.title("🔎 竞品关键词 · 逆向分析（Keepa + HTML + 词频）")

cfg = load_config("config.yaml")
domain_name = st.selectbox("站点", list(cfg.keepa.domain_map.keys()), index=0)
seed_asin = st.text_input("输入种子 ASIN（如 B08CH9HFSC）").strip().upper()
max_cluster = st.slider("最多拉取多少个相关竞品（Keepa+HTML 合并去重后取前N个）", 10, 300, 80, 10)

colA, colB = st.columns([1,1])
with colA:
    do_keepa = st.checkbox("使用 Keepa 相关 ASIN 通道", value=True)
with colB:
    do_html = st.checkbox("使用 HTML 回退通道", value=True)

if st.button("开始分析", disabled=(not seed_asin)):
    # 1) 相关 ASIN 聚类
    all_asins = set()
    keepa_err = None
    if do_keepa:
        with st.spinner("Keepa 抓取相关 ASIN..."):
            kc = KeepaClient(cfg.keepa.api_key, cfg.keepa.domain_map, cfg.keepa.timeout, cfg.keepa.retries)
            rel, keepa_err = kc.product_related(seed_asin, domain_name, cfg.keepa.history)
            all_asins.update(rel)
    if keepa_err:
        st.warning(f"Keepa 警告：{keepa_err}")

    if do_html:
        with st.spinner("HTML 回退抓取相关 ASIN..."):
            rel_html = search_related_html(seed_asin, domain_name)
            all_asins.update(rel_html)

    all_asins = sorted([a for a in all_asins if a and len(a)==10 and a.upper()!=seed_asin])[:max_cluster]
    st.info(f"合并去重后相关 ASIN 数：{len(all_asins)}")

    if not all_asins:
        st.error("没有拿到相关 ASIN，无法继续。请换一个种子 ASIN 或稍后重试。")
        st.stop()

    # 2) 补全基本信息（价格/评分/评论数/品牌）
    with st.spinner("补全基本信息（标题/价格/评分/评论/品牌）..."):
        df_full = enrich_product_info(all_asins, domain_name)

    # 3) 抓取文案文本（标题/五点/A+）
    with st.spinner("抓取 Listing 文案文本（标题/要点/A+）..."):
        texts = scrape_listing_text(all_asins, domain_name)  # 返回 dict[asin] = {"title","bullets","aplus","brand"}

    # 4) 关键词挖掘（词频 + 权重 + 去噪 + 相关性阈值）
    with st.spinner("关键词挖掘与打分..."):
        kw_table, debug_rows = mine_keywords_from_cluster(texts, cfg.keyword_mining)

    st.subheader("🏷️ 竞品关键词榜单（按得分/覆盖排序）")
    st.dataframe(kw_table.head(cfg.keyword_mining.max_top), use_container_width=True)
    st.download_button(
        "⬇️ 下载关键词榜单（CSV）",
        to_csv_bytes(kw_table.head(cfg.keyword_mining.max_top)),
        file_name=f"{seed_asin}_keyword_leaderboard.csv",
        mime="text/csv",
    )

    # 5) 可选：对核心关键词附加 BSR 同步信号（用 Keepa 的 BSR 曲线做佐证）
    if cfg.keyword_mining.bsr_correlation_window and cfg.keepa.api_key:
        with st.spinner("计算 BSR 同步信号（采样 Top 50 相关 ASIN）..."):
            kw_table2 = attach_bsr_signal(
                kw_table.head(cfg.keyword_mining.max_top).copy(),
                candidate_asins=all_asins[:50],
                keepa_client=kc,
                window=cfg.keyword_mining.bsr_correlation_window
            )
        st.subheader("📈 附带 BSR 同步信号的关键词榜单")
        st.dataframe(kw_table2.head(cfg.keyword_mining.max_top), use_container_width=True)
        st.download_button(
            "⬇️ 下载（含 BSR 信号）CSV",
            to_csv_bytes(kw_table2.head(cfg.keyword_mining.max_top)),
            file_name=f"{seed_asin}_keyword_leaderboard_bsr.csv",
            mime="text/csv",
        )
    else:
        st.info("未启用或无法计算 BSR 同步信号（需要 Keepa API Key 且配置了窗口）。")

    # 6) 调试/透明度输出：原始文本与参与样本
    with st.expander("🔍 调试与可追溯（样本/文本切片）"):
        st.write("用于挖掘的样本统计：", len(debug_rows))
        st.dataframe(pd.DataFrame(debug_rows).head(200), use_container_width=True)

    # 7) 一键打包下载
    from datetime import datetime
    zip_buf = to_csv_bytes_many({
        "competitor_meta.csv": df_full,
        "keyword_leaderboard.csv": kw_table.head(cfg.keyword_mining.max_top),
        "debug_samples.csv": pd.DataFrame(debug_rows),
    })
    st.download_button(
        "📦 一键打包下载（Zip）",
        data=zip_buf.getvalue(),
        file_name=f"keyword_intel_{seed_asin}_{datetime.now().date()}.zip",
        mime="application/zip",
    )
