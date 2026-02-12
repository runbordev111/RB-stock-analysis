# download_broker_info.py
# 目的：下載 FinMind 券商/分點主檔 TaiwanSecuritiesTraderInfo，輸出 CSV

import os
import argparse
import requests
import pandas as pd
from dotenv import load_dotenv

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

def download_broker_info(out_csv: str, verify_ssl: bool = True):
    load_dotenv()
    token = os.getenv("FINMIND_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError("找不到 FINMIND_API_TOKEN，請在 .env 設定 FINMIND_API_TOKEN=你的token")

    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "dataset": "TaiwanSecuritiesTraderInfo",
        # 這個 dataset 通常不需要日期參數；若未來 API 要求，可再補 start_date
    }

    resp = requests.get(FINMIND_URL, headers=headers, params=params, verify=verify_ssl, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    if payload.get("status") != 200:
        raise RuntimeError(f"FinMind API error: status={payload.get('status')} msg={payload.get('msg')}")

    df = pd.DataFrame(payload.get("data", []))
    if df.empty:
        raise RuntimeError("取得的券商資料為空：請檢查 token 權限或 dataset 是否變更")

    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="./data/broker_info_finmind.csv", help="輸出 CSV 路徑")
    parser.add_argument("--no_ssl_verify", action="store_true", help="關閉 SSL verify（公司網路憑證問題才用）")
    args = parser.parse_args()

    df = download_broker_info(out_csv=args.out, verify_ssl=not args.no_ssl_verify)
    print(f"✅ 下載完成：{args.out} | rows={len(df)} | cols={list(df.columns)}")

if __name__ == "__main__":
    main()
