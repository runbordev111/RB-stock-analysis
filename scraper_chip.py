# scraper_chip.py (入口/流程編排)
# 目的：抓 FinMind 分點明細（TaiwanStockTradingDailyReport），輸出：
#   ./data/{stock_id}_whale_track.json  （dashboard 使用）
#   ./data/{stock_id}_boss_list.csv     （Top20 彙總表）
#
# 增量快取：每筆 TDR 存於 data/cache/tdr/{stock_id}/{date}.csv，
# 已有快取的日期不再向 FinMind 請求，只抓缺失的日期（例如 2/27~3/10）。

import os
import time
import json
import argparse
import urllib3
from datetime import datetime
from dotenv import load_dotenv

import pandas as pd

from core.io.finmind_client import FinMindClient
from core.services.adapter_tw import TaiwanStockAdapter
from core.io.broker_master import load_broker_master_enriched
from core.pipeline import analyze_whale_trajectory
from core.pipeline import _load_company_geo_map

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(PROJECT_ROOT, "data")
RAW_PATH = os.path.join(PROJECT_ROOT, "rawdata")
CACHE_TDR_DIR = os.path.join(DATA_PATH, "cache", "tdr")

os.makedirs(DATA_PATH, exist_ok=True)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _tdr_cache_path(stock_id: str, date_str: str) -> str:
    """data/cache/tdr/{stock_id}/{date}.csv"""
    d = os.path.join(CACHE_TDR_DIR, str(stock_id))
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, f"{date_str}.csv")


def _load_tdr_from_cache(stock_id: str, date_str: str) -> pd.DataFrame | None:
    p = _tdr_cache_path(stock_id, date_str)
    if not os.path.exists(p):
        return None
    try:
        df = pd.read_csv(p, encoding="utf-8-sig")
        if df.empty:
            return None
        df["date"] = date_str
        return df
    except Exception:
        return None


def _save_tdr_to_cache(stock_id: str, date_str: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    p = _tdr_cache_path(stock_id, date_str)
    df.to_csv(p, index=False, encoding="utf-8-sig")


def run_strategy(
    stock_id: str,
    days: int = 20,
    throttle_sec: float = 0.6,
    verify_ssl: bool = True,
    debug_tv: bool = False,
):
    load_dotenv()
    token = os.getenv("FINMIND_API_TOKEN", "").strip()
    if not token:
        print("❌ 找不到 FINMIND_API_TOKEN，請在 .env 設定 FINMIND_API_TOKEN=你的token")
        return

    client = FinMindClient(token=token, verify_ssl=verify_ssl)
    adapter = TaiwanStockAdapter(client)
    broker_map = load_broker_master_enriched(RAW_PATH)

    # 事先計算交易日，供快取判斷與 pipeline 使用
    all_dates = adapter.get_trading_dates(lookback=180)
    today_str = datetime.now().strftime("%Y-%m-%d")
    available_dates = [d for d in all_dates if d < today_str]

    if not available_dates:
        print("❌ 無法取得交易日，終止。")
        return

    last_trading_date = available_dates[-1]

    # 簡單快取：若已有同一檔股票、且 probe_date 已是最近交易日，就不用重抓 FinMind
    json_path = os.path.join(DATA_PATH, f"{stock_id}_whale_track.json")
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
            existing_probe = str(existing.get("probe_date", "")).strip()
        except Exception:
            existing_probe = ""

        if existing_probe == last_trading_date:
            print(
                f"✅ {stock_id} 已有最新資料（probe_date={existing_probe}），"
                "略過 FinMind 抓取與重新分析。"
            )
            return

    # ✅ Warm-up：強制用絕對路徑載入公司總部經緯度（避免 pipeline 用相對路徑失敗）
    _load_company_geo_map(
        tse_csv=os.path.join(RAW_PATH, "TSE_Company_V2.csv"),
        otc_csv=os.path.join(RAW_PATH, "OTC_Company_V2.csv"),
    )

    # 1) 交易日（排除今天），只取最近 days 天
    target_dates = available_dates[-days:]

    if len(target_dates) < days:
        print(f"⚠️ 交易日不足 {days} 天，實際取得 {len(target_dates)} 天: {target_dates[0]} ~ {target_dates[-1]}")

    # 2) 抓分點明細（增量：先查快取，沒有才向 FinMind 請求）
    stock_name = adapter.get_stock_name(stock_id)
    print(f"開始追蹤 {stock_id} {stock_name} | 近 {len(target_dates)} 日分點（增量快取：已有不重抓）...")

    frames = []
    n_from_cache = 0
    n_from_api = 0
    for d in target_dates:
        report = _load_tdr_from_cache(stock_id, d)
        if report is not None and not report.empty:
            frames.append(report)
            n_from_cache += 1
            print(f"  [cache] {d}: {len(report)} rows")
        else:
            report = adapter.get_daily_report(stock_id, d)
            if not report.empty:
                report = report.copy()
                report["date"] = d
                frames.append(report)
                _save_tdr_to_cache(stock_id, d, report)
                n_from_api += 1
                print(f"  [API]   {d}: {len(report)} rows")
            else:
                print(f"  [skip]  {d}: 分點數據為空")
            time.sleep(throttle_sec)

    if n_from_cache > 0 or n_from_api > 0:
        print(f"  -> cache {n_from_cache} days, fetched {n_from_api} days from API")
    if n_from_api > 0 and n_from_cache == 0:
        print("  -> First run: cache populated. Next run will only fetch new dates.")

    if not frames:
        print("❌ 全部日期分點都為空：請檢查 sponsor 權限或 token 是否正確。")
        return

    # 3) 分析與輸出
    insight, boss_list_df = analyze_whale_trajectory(
        frames=frames,
        target_dates=target_dates,
        broker_map=broker_map,
        adapter=adapter,
        stock_id=stock_id,
        debug_tv=debug_tv,
    )
    if insight is None or boss_list_df is None:
        print("❌ 分析失敗：分點欄位不齊或資料格式異常。")
        return

    # stock meta
    insight["stock_id"] = stock_id
    insight["stock_name"] = stock_name
    insight["probe_date"] = target_dates[-1]
    insight["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    json_path = os.path.join(DATA_PATH, f"{stock_id}_whale_track.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(insight, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON輸出完成: {json_path}")

    csv_path = os.path.join(DATA_PATH, f"{stock_id}_boss_list.csv")
    boss_list_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"💾 CSV輸出完成: {csv_path}")

    # 更新 manifest.json（供 GitHub Pages 靜態 dashboard 讀取股票清單）
    manifest_path = os.path.join(DATA_PATH, "manifest.json")
    try:
        if os.path.exists(manifest_path):
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
        else:
            manifest = {"stock_ids": [], "stocks": [], "updated": ""}
        stock_ids = list(dict.fromkeys(manifest.get("stock_ids", []) + [stock_id]))
        stocks_list = manifest.get("stocks", [])
        by_id = {s["id"]: s for s in stocks_list if isinstance(s, dict) and s.get("id")}
        by_id[stock_id] = {"id": stock_id, "name": stock_name}
        manifest["stock_ids"] = sorted(stock_ids, key=lambda x: int(x) if str(x).isdigit() else 0)
        manifest["stocks"] = [by_id[sid] for sid in manifest["stock_ids"]]
        manifest["updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        print(f"💾 Manifest 已更新: {manifest_path}")
    except Exception as e:
        print(f"⚠️ 更新 manifest 略過: {e}")

    sig = insight.get("signals", {})
    print(
        f"📊 完成：Score={sig.get('score',0)} | Trend={sig.get('trend','')} | "
        f"TV={sig.get('tv_score',0)}/5({sig.get('tv_grade','')}) | "
        f"5D集中度={sig.get('concentration_5d',0)}% | 20D集中度={sig.get('concentration_20d',0)}% | "
        f"20D買超家數={sig.get('buy_count_20d',0)} | 20D賣超家數={sig.get('sell_count_20d',0)} | "
        f"series_len={len(sig.get('labels_20d',[]))}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock_id", default="6239", help="股票代號，如 6239")
    parser.add_argument("--days", type=int, default=20, help="回溯交易日數（建議 20，對應 dashboard 的 20日）")
    parser.add_argument("--throttle", type=float, default=0.6, help="每次請求間隔秒數")
    parser.add_argument("--no_ssl_verify", action="store_true", help="關閉 SSL verify（公司網路憑證問題才用）")
    parser.add_argument("--debug_tv", action="store_true", help="印出 OHLCV / TV debug 資訊（並把 tv_debug 寫進 JSON）")
    args = parser.parse_args()

    run_strategy(
        stock_id=args.stock_id,
        days=args.days,
        throttle_sec=args.throttle,
        verify_ssl=not args.no_ssl_verify,
        debug_tv=args.debug_tv,
    )


if __name__ == "__main__":
    main()
