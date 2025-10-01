# -*- coding: utf-8 -*-
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

# Page layout
st.set_page_config(page_title="Keyword Intelligence", page_icon="🔎", layout="wide")
st.title("Keyword Intelligence - Competitor Keyword Mining (Keepa + HTML + n-gram)")

# Load config (includes safe defaults for keyword_mining)
cfg = load_config("config.yaml")

# Controls
domain_name = st.selectbox("Marketplace", list(cfg.keepa.domain_map.keys()), index=0)
seed_asin = st.text_input("Seed ASIN (e.g., B08CH9HFSC)").strip().upper()
max_cluster = st.slider("Max related ASINs (after merge & dedupe)", 10, 300, 80, 10)

colA, colB = st.columns([1, 1])
with colA:
    do_keepa = st.checkbox("Use Keepa related ASIN channel", value=True)
with colB:
    do_html = st.checkbox("Use HTML fallback channel", value=True)

run = st.button("Run analysis", disabled=(not seed_asin))

if run:
    # 1) Collect related ASINs
    all_asins = set()
    keepa_err = None
    kc = None  # Keepa client placeholder

    if do_keepa:
        with st.spinner("Fetching related ASINs via Keepa..."):
            kc = KeepaClient(
                cfg.keepa.api_key,
                cfg.keepa.domain_map,
                cfg.keepa.timeout,
                cfg.keepa.retries,
            )
            rel, keepa_err = kc.product_related(seed_asin, domain_name, cfg.keepa.history)
            if rel:
                all_asins.update(rel)
    if keepa_err:
        st.warning(f"Keepa warning: {keepa_err}")

    if do_html:
        with st.spinner("Fetching related ASINs via HTML fallback..."):
            rel_html = search_related_html(seed_asin, domain_name)
            if rel_html:
                all_asins.update(rel_html)

    # Clean and cap
    all_asins = sorted(
        [a.upper() for a in all_asins if a and len(a) == 10 and a.upper() != seed_asin]
    )[:max_cluster]
    st.info(f"Related ASINs after merge & dedupe: {len(all_asins)}")

    if not all_asins:
        st.error("No related ASINs found. Try another seed ASIN or enable another channel.")
        st.stop()

    # 2) Enrich meta info (title/price/rating/reviews/brand)
    with st.spinner("Enriching product meta (title/price/rating/reviews/brand)..."):
        df_full = enrich_product_info(all_asins, domain_name)
    st.subheader("Competitor meta (sample)")
    st.dataframe(df_full.head(50), use_container_width=True)

    # 3) Scrape listing texts (title/bullets/A+)
    with st.spinner("Scraping listing texts (title/bullets/A+)..."):
        texts = scrape_listing_text(all_asins, domain_name)  # {asin: {"title","bullets","aplus","brand"}}

    # 4) Keyword mining (n-gram, weighting, de-noise)
    with st.spinner("Mining and scoring keywords..."):
        kw_table, debug_rows = mine_keywords_from_cluster(texts, cfg.keyword_mining)

    st.subheader("Keyword leaderboard (sorted by score/coverage)")
    top_limit = getattr(cfg.keyword_mining, "max_top", 200)
    st.dataframe(kw_table.head(top_limit), use_container_width=True)
    st.download_button(
        "Download keyword leaderboard (CSV)",
        to_csv_bytes(kw_table.head(top_limit)),
        file_name=f"{seed_asin}_keyword_leaderboard.csv",
        mime="text/csv",
    )

    # 5) Optional: BSR sync signal (requires Keepa channel and window configured)
    window = getattr(cfg.keyword_mining, "bsr_correlation_window", None)
    if window and kc is not None and cfg.keepa.api_key:
        with st.spinner("Computing BSR sync signal (sampling top 50 related ASINs)..."):
            kw_table_bsr = attach_bsr_signal(
                kw_table.head(top_limit).copy(),
                candidate_asins=all_asins[:50],
                keepa_client=kc,
                window=window,
                domain_name=domain_name,
            )
        st.subheader("Keyword leaderboard with BSR sync signal")
        st.dataframe(kw_table_bsr.head(top_limit), use_container_width=True)
        st.download_button(
            "Download (with BSR signal) CSV",
            to_csv_bytes(kw_table_bsr.head(top_limit)),
            file_name=f"{seed_asin}_keyword_leaderboard_bsr.csv",
            mime="text/csv",
        )
    else:
        st.info("
