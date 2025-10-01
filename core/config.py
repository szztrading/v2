import os, yaml
from pydantic import BaseModel
import streamlit as st

class KeepaCfg(BaseModel):
    api_key: str | None = None
    domain_map: dict
    prefer_rest: bool = True
    history: int = 0
    timeout: int = 25
    retries: int = 2

class Thresholds(BaseModel):
    min_clicks: int
    min_conversions: int
    harvest_cvr: float

class BidSteps(BaseModel):
    up_pct: float
    down_pct: float

class NegScan(BaseModel):
    include_terms: list[str]
    exclude_terms: list[str]
    price_min: float
    price_max: float
    rating_min: float
    reviews_min: int
    brand_whitelist: list[str]
    brand_blacklist: list[str]

# ------- 新增：关键词挖掘配置 -------
class KeywordMiningWeight(BaseModel):
    title: float = 1.0
    bullets: float = 0.7
    aplus: float = 0.4
    brand_bonus: float = 0.0

class KeywordMiningCfg(BaseModel):
    min_df: int = 2
    max_top: int = 200
    ngram_range: list[int] = [1, 3]     # [n_min, n_max]
    stopwords: list[str] = []
    suggest_probe: bool = False
    weight: KeywordMiningWeight = KeywordMiningWeight()
    bsr_correlation_window: int | None = 5

class Cfg(BaseModel):
    marketplace: str
    target_acos: float
    bid_steps: BidSteps
    thresholds: Thresholds
    negatives_scan: NegScan
    keepa: KeepaCfg
    keyword_mining: KeywordMiningCfg = KeywordMiningCfg()  # 新增字段（含默认）

def _read_secrets_key():
    api = st.secrets.get("KEEPA_API_KEY") if "KEEPA_API_KEY" in st.secrets else None
    if not api and "keepa" in st.secrets and "api_key" in st.secrets["keepa"]:
        api = st.secrets["keepa"]["api_key"]
    return api

def load_config(path: str) -> Cfg:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    # 确保 keepa 节存在 & 注入 API Key
    raw.setdefault("keepa", {})
    raw["keepa"]["api_key"] = _read_secrets_key()

    # 确保 keyword_mining 节存在（即使 YAML 未配置也不报错）
    raw.setdefault("keyword_mining", {})
    raw["keyword_mining"].setdefault("ngram_range", [1, 3])
    raw["keyword_mining"].setdefault("min_df", 2)
    raw["keyword_mining"].setdefault("max_top", 200)
    raw["keyword_mining"].setdefault("stopwords", [])
    raw["keyword_mining"].setdefault("suggest_probe", False)
    raw["keyword_mining"].setdefault("weight", {
        "title": 1.0, "bullets": 0.7, "aplus": 0.4, "brand_bonus": 0.0
    })
    raw["keyword_mining"].setdefault("bsr_correlation_window", 5)

    return Cfg(**raw)
