import streamlit as st

st.set_page_config(page_title="Hopsbrew Ads Console v2.0", page_icon="🍺", layout="wide")

st.sidebar.title("🍺 Hopsbrew Console v2.0")
st.sidebar.write("选择左侧页面进入：")
st.title("欢迎使用 Hopsbrew 广告与竞品情报控制台 v2.0")
st.markdown("""
**模块：**
- PPC Optimizer：上传 Search Term 报表，自动出价建议/否定建议/收割计划  
- Competitor Intelligence：输入 ASIN，Keepa + HTML 双通道抓取并去重，相关性打分  
- Market Radar（预留）：后续接入历史曲线与生命周期分析
""")
st.info("从左侧 Pages 进入具体模块。")
