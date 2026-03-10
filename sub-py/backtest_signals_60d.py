"""
backtest_signals_60d.py

目的：
- 針對少量關注標的（例如 TradingView 篩出的 10~20 檔），
  在最近 30~60 個交易日內，逐日重跑 pipeline，蒐集：
    - 當日各種 signals（score / monitor_state / coherence / geo / tv / regime ...）
    - 未來 5/10/20 日的實際報酬 (close)
- 輸出一個 CSV，方便後續用 Excel / Notebook 做統計與調參。
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import urllib3
from dotenv import load_dotenv

# --- 確保可以匯入專案根目錄的 core 套件 ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from core.io.finmind_client import FinMindClient
from core.services.adapter_tw import TaiwanStockAdapter
from core.pipeline import analyze_whale_trajectory
from core.io.broker_master import load_broker_master_enriched
from core.pipeline import _load_company_geo_map


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATA_PATH = os.path.join(PROJECT_ROOT, "data")
RAW_PATH = os.path.join(PROJECT_ROOT, "rawdata")
os.makedirs(DATA_PATH, exist_ok=True)


def _ensure_datetime(date_str: str) -> str:
    """確保是 YYYY-MM-DD 字串。"""
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")


def fetch_price_window(
    client: FinMindClient,
    stock_id: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """抓一段區間的 TaiwanStockPrice，只保留 date / close。"""
    df = client.request_data(
        "TaiwanStockPrice",
        data_id=stock_id,
        start_date=start_date,
        end_date=end_date,
    )
    if df.empty or "date" not in df.columns or "close" not in df.columns:
        return pd.DataFrame()
    out = df[["date", "close"]].copy()
    out["date"] = out["date"].astype(str)
    out["close"] = pd.to_numeric(out["close"], errors="coerce").fillna(0.0)
    out = out.sort_values("date").reset_index(drop=True)
    return out


def compute_forward_returns(
    price_df: pd.DataFrame,
    trade_date: str,
    horizons: List[int],
) -> Dict[str, float]:
    """在 price_df 中，對指定 trade_date 計算未來 h 日的收盤報酬。"""
    out: Dict[str, float] = {}
    if price_df is None or price_df.empty:
        for h in horizons:
            out[f"ret_{h}d"] = float("nan")
        return out

    price_df = price_df.copy()
    price_df["date"] = price_df["date"].astype(str)
    price_df = price_df.sort_values("date").reset_index(drop=True)

    if trade_date not in set(price_df["date"]):
        for h in horizons:
            out[f"ret_{h}d"] = float("nan")
        return out

    idx = price_df.index[price_df["date"] == trade_date][0]
    p0 = float(price_df.loc[idx, "close"])
    if p0 == 0:
        for h in horizons:
            out[f"ret_{h}d"] = float("nan")
        return out

    for h in horizons:
        j = idx + h
        if j >= len(price_df):
            out[f"ret_{h}d"] = float("nan")
            continue
        p_h = float(price_df.loc[j, "close"])
        out[f"ret_{h}d"] = round((p_h - p0) / p0, 4)
    return out


def extract_signal_features(
    stock_id: str,
    trade_date: str,
    signals: Dict[str, Any],
) -> Dict[str, Any]:
    """從 signals dict 抽出我們關心的欄位，扁平化成一列。"""
    row: Dict[str, Any] = {
        "stock_id": stock_id,
        "trade_date": trade_date,
    }

    # 基礎分數與趨勢
    row["score"] = signals.get("score")
    row["final_score"] = signals.get("final_score")
    row["trend"] = signals.get("trend")
    row["monitor_state"] = signals.get("monitor_state")

    # 集中度 / 廣度 / NetBuy
    for k in [
        "concentration_5d",
        "concentration_20d",
        "netbuy_1d_lot",
        "netbuy_5d_lot",
        "netbuy_20d_lot",
        "buy_count_5d",
        "sell_count_5d",
        "breadth_5d",
        "breadth_ratio_5d",
        "buy_count_20d",
        "sell_count_20d",
        "breadth_20d",
        "breadth_ratio_20d",
        "pressure_ratio_5d",
        "pressure_ratio_20d",
        "top15_buy_stability_10d",
        "top15_buy_stability_20d",
    ]:
        row[k] = signals.get(k)

    # Enhanced coherence / cost zone
    ez = signals.get("enhanced") or {}
    for k in [
        "coherence_today",
        "coherence_slope_5d",
        "coherence_persistence_20d",
        "coherence_max_20d",
        "cost_low",
        "cost_high",
        "cost_deviation",
        "cost_zone_status",
    ]:
        row[f"enh_{k}"] = ez.get(k)

    # TV / Regime / Geo / Distribution
    row["tv_score"] = signals.get("tv_score")
    row["tv_grade"] = signals.get("tv_grade")
    row["regime_score"] = signals.get("regime_score")
    row["regime_trend"] = signals.get("regime_trend")
    row["geo_grade"] = signals.get("geo_grade")
    row["geo_zscore"] = signals.get("geo_zscore")
    row["geo_baseline_weight"] = signals.get("geo_baseline_weight")
    row["dist_risk_flag"] = signals.get("dist_risk_flag")

    # Institutional / Margin
    for k in [
        "inst_foreign_net_5d",
        "inst_trust_net_5d",
        "inst_three_net_5d",
        "inst_foreign_net_20d",
        "inst_trust_net_20d",
        "inst_three_net_20d",
        "inst_foreign_net_60d",
        "inst_trust_net_60d",
        "inst_three_net_60d",
        "inst_three_align_5d",
        "margin_balance_20d_change",
        "margin_risk_flag",
    ]:
        row[k] = signals.get(k)

    # Validation / Risk
    val = signals.get("validation") or {}
    risk = signals.get("risk") or {}
    row["breakout_flag"] = val.get("breakout_flag")
    row["divergence_flag"] = val.get("divergence_flag")
    row["confirmation_score"] = val.get("confirmation_score")
    row["atr_pct_20d"] = risk.get("atr_pct_20d")
    row["avg_turnover_20d"] = risk.get("avg_turnover_20d")
    row["invalid_flag"] = risk.get("invalid_flag")

    # Broker archetype（Top6 波段主力傾向）
    row["top6_avg_wave_score"] = signals.get("top6_avg_wave_score")
    row["top6_max_wave_score"] = signals.get("top6_max_wave_score")
    row["top6_wave_leader_id"] = signals.get("top6_wave_leader_id")
    row["top6_wave_leader_name"] = signals.get("top6_wave_leader_name")

    return row


def _add_cross_sectional_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 2：在 backtest 輸出中加入 cross-sectional percentile 特徵。

    對每個 trade_date，針對關鍵指標計算當日橫截面的百分位排名（0~1）。
    例如：score_pctile 表示該日 score 在所有樣本中的相對位置。
    """
    if df.empty:
        return df

    if "trade_date" not in df.columns:
        return df

    df = df.copy()

    cols = [
        "score",
        "final_score",
        "tv_score",
        "regime_score",
        "concentration_5d",
        "concentration_20d",
        "netbuy_1d_lot",
        "netbuy_5d_lot",
        "netbuy_20d_lot",
        "breadth_5d",
        "breadth_20d",
        "pressure_ratio_5d",
        "pressure_ratio_20d",
        "top15_buy_stability_10d",
        "top15_buy_stability_20d",
    ]

    for c in cols:
        if c not in df.columns:
            continue
        try:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        except Exception:
            continue
        df[f"{c}_pctile"] = (
            df.groupby("trade_date")[c]
            .rank(pct=True, method="average")
            .astype(float)
        )

    return df


def run_backtest(
    stock_ids: List[str],
    days: int,
    horizons: List[int],
    throttle_sec: float,
    verify_ssl: bool,
    debug_tv: bool,
    output_path: str,
) -> None:
    load_dotenv()
    token = os.getenv("FINMIND_API_TOKEN", "").strip()
    if not token:
        print("❌ 找不到 FINMIND_API_TOKEN，請在 .env 設定 FINMIND_API_TOKEN=你的token")
        return

    client = FinMindClient(token=token, verify_ssl=verify_ssl)
    adapter = TaiwanStockAdapter(client)
    # 與原本行為保持一致：從 data/ 讀 broker_master（而不是 rawdata/）
    broker_map = load_broker_master_enriched(DATA_PATH)

    rows: List[Dict[str, Any]] = []

    for stock_id in stock_ids:
        stock_id = stock_id.strip()
        if not stock_id:
            continue

        print(f"===== Backtest {stock_id} 最近 {days} 個交易日 =====")

        # 交易日：給足 buffer（days + 40），避免 20D 視窗不足
        all_dates = adapter.get_trading_dates(lookback=days + 80)
        if not all_dates:
            print(f"⚠️ 無法取得交易日：{stock_id}")
            continue

        all_dates = [d for d in all_dates if d <= datetime.now().strftime("%Y-%m-%d")]
        if len(all_dates) < days + 20:
            print(f"⚠️ 交易日數不足（需要 >= {days+20}，實際 {len(all_dates)}）: {stock_id}")

        # 樣本區間：最後 days 個交易日
        sample_dates = all_dates[-days:]

        # 預先抓 TDR，避免重複 hitting API
        report_by_date: Dict[str, pd.DataFrame] = {}
        tdr_dates = all_dates[-(days + 20) :]
        for d in tdr_dates:
            rpt = adapter.get_daily_report(stock_id, d)
            if not rpt.empty:
                rpt = rpt.copy()
                rpt["date"] = d
            report_by_date[d] = rpt
            time.sleep(throttle_sec)

        # 價格資料，用於 forward returns
        if tdr_dates:
            start_date = tdr_dates[0]
            end_date = tdr_dates[-1]
        else:
            start_date = all_dates[0]
            end_date = all_dates[-1]
        price_df = fetch_price_window(client, stock_id, start_date=start_date, end_date=end_date)

        date_index = {d: i for i, d in enumerate(all_dates)}

        for d in sample_dates:
            idx = date_index.get(d)
            if idx is None or idx < 19:
                continue
            window_dates = all_dates[idx - 19 : idx + 1]  # 20 交易日視窗

            frames: List[pd.DataFrame] = []
            for wd in window_dates:
                rpt = report_by_date.get(wd)
                if isinstance(rpt, pd.DataFrame) and not rpt.empty:
                    frames.append(rpt)

            if not frames:
                continue

            insight, _ = analyze_whale_trajectory(
                frames=frames,
                target_dates=window_dates,
                broker_map=broker_map,
                adapter=adapter,
                stock_id=stock_id,
                debug_tv=debug_tv,
            )
            if insight is None:
                continue

            signals = insight.get("signals", {}) or {}
            feature_row = extract_signal_features(stock_id, d, signals)
            ret_row = compute_forward_returns(price_df, d, horizons)
            feature_row.update(ret_row)

            rows.append(feature_row)

        print(f"✅ {stock_id} 完成樣本數：{len([r for r in rows if r['stock_id']==stock_id])}")

    if not rows:
        print("⚠️ 沒有任何樣本被產生，請檢查資料或參數。")
        return

    df_new = pd.DataFrame(rows)
    df_new["stock_id"] = df_new["stock_id"].astype(str).str.strip()

    # 一律使用絕對路徑，避免因執行目錄不同讀寫到不同檔案
    output_path = os.path.abspath(output_path)
    print(f"📁 輸出檔：{output_path}")

    # 若已存在舊檔，先讀進來，刪掉本次 stock_ids 的舊樣本，再與新樣本合併
    df_old: pd.DataFrame
    if os.path.exists(output_path):
        try:
            df_old = pd.read_csv(output_path, dtype={"stock_id": str}, encoding="utf-8-sig")
            df_old["stock_id"] = df_old["stock_id"].astype(str).str.strip()
        except Exception as e:
            print(f"⚠️ 讀取既有 CSV 失敗（{e}），將只寫入本次結果，舊資料不會被合併。")
            df_old = pd.DataFrame()
    else:
        df_old = pd.DataFrame()

    if not df_old.empty:
        # 只刪除「本次有跑」的股票舊資料，其餘全部保留
        df_keep = df_old[~df_old["stock_id"].isin(stock_ids)]
        n_removed = len(df_old) - len(df_keep)
        df_out = pd.concat([df_keep, df_new], ignore_index=True)
        old_stocks = sorted(df_keep["stock_id"].unique().tolist())
        new_stocks = sorted(df_new["stock_id"].unique().tolist())
        print(f"📎 合併：既有 {len(df_old)} 筆（{len(df_old['stock_id'].unique())} 檔），移除本次 {len(stock_ids)} 檔的舊樣本（{n_removed} 筆）→ 保留 {len(df_keep)} 筆 + 本次 {len(df_new)} 筆 = 共 {len(df_out)} 筆（{len(df_out['stock_id'].unique())} 檔）")
    else:
        df_out = df_new
        if os.path.exists(output_path):
            print("⚠️ 未讀到既有資料，本次僅寫入新結果。")
        else:
            print("📎 為新檔案，直接寫入。")

    df_out = df_out.sort_values(["stock_id", "trade_date"]).reset_index(drop=True)

    # Phase 2：加上 cross-sectional percentile 特徵
    df_out = _add_cross_sectional_features(df_out)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_out.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"💾 輸出完成：{output_path} （{len(df_out)} 筆，{df_out['stock_id'].nunique()} 檔股票）")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest whale signals in last N trading days.")
    parser.add_argument(
        "--stock_ids",
        type=str,
        required=True,
        help="以逗號分隔的股票代號，例如：2454,2357,6239",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=60,
        help="向前回溯的交易日數（預設 60）",
    )
    parser.add_argument(
        "--horizons",
        type=str,
        default="5,10,20",
        help="未來報酬的觀察天數，逗號分隔，例如：5,10,20",
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=0.6,
        help="每次 TDR 請求的間隔秒數（避免 FinMind 流量壓力）",
    )
    parser.add_argument(
        "--no_ssl_verify",
        action="store_true",
        help="關閉 SSL verify（公司網路憑證問題時才用）",
    )
    parser.add_argument(
        "--debug_tv",
        action="store_true",
        help="是否印出 TV / OHLCV debug 資訊（轉給 pipeline）",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="backtest_signals_60d.csv",
        help="輸出 CSV 檔名（會寫在 ./data/ 下）",
    )

    args = parser.parse_args()
    stock_ids = [x.strip() for x in args.stock_ids.split(",") if x.strip()]
    horizons = [int(x) for x in args.horizons.split(",") if x.strip()]

    out_path = args.output
    if not os.path.isabs(out_path):
        out_path = os.path.join(DATA_PATH, out_path)

    run_backtest(
        stock_ids=stock_ids,
        days=args.days,
        horizons=horizons,
        throttle_sec=args.throttle,
        verify_ssl=not args.no_ssl_verify,
        debug_tv=args.debug_tv,
        output_path=out_path,
    )


if __name__ == "__main__":
    main()

