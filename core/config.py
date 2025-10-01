# -*- coding: utf-8 -*-
import yaml
from pydantic import BaseModel
import streamlit as st

# ---------------------------
# Pydantic models
# ---------------------------
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

class KeywordMiningWeight(BaseModel):
    title: float = 1.0
    bullets: float = 0.7
    aplus: float = 0.4
    brand_bonus: float = 0.0

class KeywordMiningCfg(BaseModel):
    min_df: int = 2
    max_top: int = 200
    ngram_range: list[int] = [1, 3]
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
    keyword_mining: KeywordMiningCfg = KeywordMiningCfg()

# ---------------------------
# Secrets helper
# ---------------------------
def _read_secrets_key() -> str | None:
    # Support both KEEPA_API_KEY and [keepa].api_key
    api = None
    try:
        if "KEEPA_API_KEY" in st.secrets:
            api = st.secrets.get("KEEPA_API_KEY")
        elif "keepa" in st.secrets and "api_key" in st.secrets["keepa"]:
            api = st.secrets["keepa"]["api_key"]
    except Exception:
        # st.secrets may not exist in local plain runs; ignore
        api = None
    return api

# ---------------------------
# Loader
# ---------------------------
def load_config(path: str) -> Cfg:
    # read yaml
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    # ensure keepa section exists and inject api key
    raw.setdefault("keepa", {})
    raw["keepa"]["api_key"] = _read_secrets_key()

    # ensure keyword_mining exists with safe defaults
    km = raw.setdefault("keyword_mining", {})
    km.setdefault("min_df", 2)
    km.setdefault("max_top", 200)
    km.setdefault("ngram_range", [1, 3])
    km.setdefault("stopwords", [])
    km.setdefault("suggest_probe", False)
    km.setdefault("weight", {
        "title": 1.0,
        "bullets": 0.7,
        "aplus": 0.4,
        "brand_bonus": 0.0,
    })
    km.setdefault("bsr_correlation_window", 5)

    return Cfg(**raw)
