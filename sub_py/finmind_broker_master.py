# save_finmind_broker_master.py
# 目的：一次性把 FinMind「券商/分點主檔」抓下來存成 CSV（可選：同時輸出只含縣市的 master）
import os
import re
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

DATA_PATH = "./data"
os.makedirs(DATA_PATH, exist_ok=True)

FINMIND_TOKEN = (os.getenv("FINMIND_API_TOKEN") or "").strip()

FINMIND_V4_DATA_URL = "https://api.finmindtrade.com/api/v4/data"

# 建議：Windows/公司網路若 SSL 沒問題就用 True；你先前抓 FinMind 成功，通常 True 沒問題
VERIFY_SSL = True

OUT_FULL = os.path.join(DATA_PATH, "broker_master_finmind_full.csv")
OUT_CITY = os.path.join(DATA_PATH, "broker_master_finmind_city.csv")

def build_session(token: str) -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=[408, 429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    # FinMind v4：支援 Bearer；同時也保留 token param（保險）
    if token:
        s.headers.update({"Authorization": f"Bearer {token}"})
    return s

def extract_city(addr: str) -> str:
    if not isinstance(addr, str):
        return ""
    a = addr.strip().replace("臺", "台")
    m = re.match(r"^(.{1,4}[縣市])", a)
    return m.group(1) if m else ""

def request_dataset(session: requests.Session, dataset: str) -> pd.DataFrame:
    params = {"dataset": dataset}
    if FINMIND_TOKEN:
        params["token"] = FINMIND_TOKEN  # 保險：有些環境 header 不吃就用 query token

    t0 = time.time()
    r = session.get(FINMIND_V4_DATA_URL, params=params, timeout=60, verify=VERIFY_SSL)
    latency = time.time() - t0

    try:
        js = r.json()
    except Exception:
        raise RuntimeError(f"FinMind response non-json | http={r.status_code} | text={r.text[:200]}")

    api_status = js.get("status")
    msg = js.get("msg", "")
    print(f"[FinMind] dataset={dataset} http={r.status_code} api={api_status} latency={latency:.2f}s msg={msg}")

    if r.status_code == 200 and api_status == 200:
        return pd.DataFrame(js.get("data", []))
    raise RuntimeError(f"FinMind API failed | http={r.status_code} api={api_status} msg={msg}")

def main():
    session = build_session(FINMIND_TOKEN)

    # FinMind 券商/分點主檔（一次性抓取）
    # 官方教材常用：TaiwanSecuritiesTraderInfo
    df = request_dataset(session, "TaiwanSecuritiesTraderInfo")
    if df.empty:
        raise RuntimeError("TaiwanSecuritiesTraderInfo returned empty data")

    # 欄位防呆：不同版本可能欄名略有差異，以下做兼容
    # 常見欄位：securities_trader_id, securities_trader, address, phone
    rename_map = {}
    if "securities_trader_id" in df.columns: rename_map["securities_trader_id"] = "broker_id"
    if "securities_trader" in df.columns: rename_map["securities_trader"] = "broker_name"
    df = df.rename(columns=rename_map)

    # 生成 city
    if "address" not in df.columns:
        df["address"] = ""
    df["city"] = df["address"].astype(str).apply(extract_city)

    # 只保留核心欄位（full 版）
    keep_cols = []
    for c in ["broker_id", "broker_name", "city", "address", "phone"]:
        if c in df.columns:
            keep_cols.append(c)
    out_full = df[keep_cols].copy()

    # 主鍵清洗 + 去重
    out_full["broker_id"] = out_full["broker_id"].astype(str).str.strip()
    out_full = out_full[out_full["broker_id"] != ""]
    out_full = out_full.drop_duplicates(subset=["broker_id"], keep="last").reset_index(drop=True)

    out_full.to_csv(OUT_FULL, index=False, encoding="utf-8-sig")
    print(f"✅ 已輸出：{OUT_FULL} | rows={len(out_full)}")

    # city master（更精簡：你目前需求只到縣市）
    out_city = out_full[["broker_id", "broker_name", "city"]].copy()
    out_city.to_csv(OUT_CITY, index=False, encoding="utf-8-sig")
    print(f"✅ 已輸出：{OUT_CITY} | rows={len(out_city)}")

    # 品質指標
    miss_city = (out_city["city"].astype(str).str.strip() == "").mean()
    print(f"📌 city_missing_ratio={miss_city:.4%}")

if __name__ == "__main__":
    main()
