import io
import pandas as pd
import streamlit as st
from core.config import load_config
from ppc.loader import load_search_terms
from services.ppc_rules import make_recommendations
from core.exporters import to_excel_zip

st.set_page_config(page_title="PPC Optimizer", page_icon="📈", layout="wide")
st.title("📈 PPC Optimizer")

cfg = load_config("config.yaml")

uploaded = st.file_uploader("上传 Amazon Search Term Report（CSV/XLSX）", type=["csv","xlsx"])
if uploaded:
    with st.spinner("解析报表中..."):
        df = load_search_terms(uploaded)
    st.success(f"加载完成：{len(df)} 行")
    st.dataframe(df.head(50), use_container_width=True)

    with st.spinner("计算建议中..."):
        recs = make_recommendations(df, cfg)
    st.subheader("建议摘要")
    for k, v in recs.items():
        st.write(f"**{k}**：{len(v)} 行")
        if len(v):
            st.dataframe(v.head(50), use_container_width=True)

    buf = to_excel_zip(recs)
    st.download_button(
        "⬇️ 下载全部建议（Zip 内含多表）",
        data=buf.getvalue(),
        file_name="ppc_output.zip",
        mime="application/zip",
    )
else:
    st.info("请先上传报表。")
