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

class Cfg(BaseModel):
    marketplace: str
    target_acos: float
    bid_steps: BidSteps
    thresholds: Thresholds
    negatives_scan: NegScan
    keepa: KeepaCfg

def _read_secrets_key():
    # 支持两种 key 位置
    api = st.secrets.get("KEEPA_API_KEY") if "KEEPA_API_KEY" in st.secrets else None
    if not api and "keepa" in st.secrets and "api_key" in st.secrets["keepa"]:
        api = st.secrets["keepa"]["api_key"]
    return api

def load_config(path: str) -> Cfg:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    api_key = _read_secrets_key()
    raw.setdefault("keepa", {})
    raw["keepa"]["api_key"] = api_key
    return Cfg(**raw)
