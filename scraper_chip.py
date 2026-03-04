# scraper_chip.py (入口/流程編排)
# 目的：抓 FinMind 分點明細（TaiwanStockTradingDailyReport），輸出：
#   ./data/{stock_id}_whale_track.json  （dashboard 使用）
#   ./data/{stock_id}_boss_list.csv     （Top20 彙總表）

import os
import time
import json
import argparse
import urllib3
from datetime import datetime
from dotenv import load_dotenv

from core.io.finmind_client import FinMindClient
from core.services.adapter_tw import TaiwanStockAdapter
from core.io.broker_master import load_broker_master_enriched
from core.pipeline import analyze_whale_trajectory
from core.pipeline import _load_company_geo_map  # ✅ 新增：強制載入公司經緯度主檔（warm-up）

# ✅ 建議：用 scraper_chip.py 所在位置當根目錄，避免 cwd 不同造成相對路徑失效
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

DATA_PATH = os.path.join(PROJECT_ROOT, "data")
RAW_PATH = os.path.join(PROJECT_ROOT, "rawdata")

os.makedirs(DATA_PATH, exist_ok=True)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATA_PATH = "./data"
os.makedirs(DATA_PATH, exist_ok=True)


def run_strategy(stock_id: str, days: int = 20, throttle_sec: float = 0.6, verify_ssl: bool = True, debug_tv: bool = False):
    load_dotenv()
    token = os.getenv("FINMIND_API_TOKEN", "").strip()
    if not token:
        print("❌ 找不到 FINMIND_API_TOKEN，請在 .env 設定 FINMIND_API_TOKEN=你的token")
        return

    client = FinMindClient(token=token, verify_ssl=verify_ssl)
    adapter = TaiwanStockAdapter(client)
    broker_map = load_broker_master_enriched(RAW_PATH)


    # ✅ Warm-up：強制用絕對路徑載入公司總部經緯度（避免 pipeline 用相對路徑失敗）
    _load_company_geo_map(
        tse_csv=os.path.join(RAW_PATH, "TSE_Company_V2.csv"),
        otc_csv=os.path.join(RAW_PATH, "OTC_Company_V2.csv"),
    )

    # 1) 交易日（排除今天）
    all_dates = adapter.get_trading_dates(lookback=180)
    today_str = datetime.now().strftime("%Y-%m-%d")
    target_dates = [d for d in all_dates if d < today_str][-days:]

    if not target_dates:
        print("❌ 無法取得交易日，終止。")
        return

    if len(target_dates) < days:
        print(f"⚠️ 交易日不足 {days} 天，實際取得 {len(target_dates)} 天: {target_dates[0]} ~ {target_dates[-1]}")

    # 2) 抓分點明細
    stock_name = adapter.get_stock_name(stock_id)
    print(f"🎯 開始追蹤 {stock_id} {stock_name} | 近 {len(target_dates)} 日分點...")

    frames = []
    for d in target_dates:
        report = adapter.get_daily_report(stock_id, d)
        if not report.empty:
            report["date"] = d
            frames.append(report)
            print(f"✅ 已載入 {d} 數據: {len(report)} rows")
        else:
            print(f"⚠️ {d} 分點數據為空（可能權限不足/尚未入庫/假日）")
        time.sleep(throttle_sec)

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
