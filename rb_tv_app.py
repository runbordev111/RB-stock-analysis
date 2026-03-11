# rb_tv_app.py
import os
import json
import time
import glob
import requests
import pandas as pd
import urllib3
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template
from dotenv import load_dotenv

from core.io.finmind_client import FinMindClient
from core.io.broker_master import load_broker_master_enriched

# ------------------------------------------------------
# 基本設定
# ------------------------------------------------------
load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATA_PATH = "./data"
os.makedirs(DATA_PATH, exist_ok=True)

TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
FINMIND_TOKEN = (os.getenv("FINMIND_API_TOKEN") or "").strip()

TWSE_T86_URL = "https://www.twse.com.tw/rwd/zh/fund/T86"

# 建議：公司網路/憑證怪怪可改 False
VERIFY_SSL = True

app = Flask(__name__)


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


fm = FinMindClient(FINMIND_TOKEN, verify_ssl=VERIFY_SSL)


# ======================================================
# B. 工具函式
# ======================================================
def clean_numeric_columns(df: pd.DataFrame, columns):
    df = df.copy()
    for col in columns:
        if col not in df.columns:
            continue
        s = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
        s = s.str.replace("null", "0", regex=False).str.replace("--", "0", regex=False)
        df[col] = pd.to_numeric(s, errors="coerce").fillna(0)
    return df


def append_unique_row_csv(file_path: str, df_row: pd.DataFrame, key_col: str = "日期"):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    if not os.path.exists(file_path):
        df_row.to_csv(file_path, index=False, encoding="utf-8-sig")
        return

    try:
        existing = pd.read_csv(file_path, dtype=str)
        existing_keys = set(existing[key_col].astype(str).tolist()) if key_col in existing.columns else set()
        new_key = str(df_row[key_col].iloc[0])
        if new_key in existing_keys:
            return
        df_row.to_csv(file_path, mode="a", header=False, index=False, encoding="utf-8-sig")
    except Exception:
        df_row.to_csv(file_path, index=False, encoding="utf-8-sig")


def send_telegram(msg: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"⚠️ [{now_str()}] Telegram 未設定，略過推播。")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=15)
    except Exception as e:
        print(f"⚠️ [{now_str()}] Telegram 發送失敗: {repr(e)}")


def get_recent_trading_dates(days: int = 10, lookback: int = 90):
    start_date = (datetime.now() - timedelta(days=lookback)).strftime("%Y-%m-%d")
    df = fm.request_data("TaiwanStockTradingDate", start_date=start_date)
    if df.empty or "date" not in df.columns:
        return []
    today_str = datetime.now().strftime("%Y-%m-%d")
    dates = [d for d in df["date"].astype(str).tolist() if d < today_str]
    return dates[-days:]


def get_stock_name(stock_id: str) -> str:
    """
    優先用 FinMind TaiwanStockInfo 抓中文名；失敗就回 stock_id（避免 6239 6239）
    """
    name = stock_id
    try:
        info_df = fm.request_data("TaiwanStockInfo", data_id=stock_id)
        if not info_df.empty:
            if "stock_name" in info_df.columns and str(info_df["stock_name"].iloc[0]).strip():
                name = str(info_df["stock_name"].iloc[0]).strip()
            elif "name" in info_df.columns and str(info_df["name"].iloc[0]).strip():
                name = str(info_df["name"].iloc[0]).strip()
    except Exception:
        pass
    return name


def enrich_top6_details(top6_details: list, broker_map: dict) -> list:
    """
    把 top6_details 補上 city / broker_org_type / is_proprietary / seat_type
    """
    if not isinstance(top6_details, list):
        return []

    out = []
    for b in top6_details:
        if not isinstance(b, dict):
            continue
        bid = str(b.get("broker_id", "")).strip()
        meta = broker_map.get(bid, {}) if bid else {}
        nb = dict(b)
        # 補欄位（若 scraper 已經有就不覆蓋非空）
        for k in ["city", "broker_org_type", "is_proprietary", "seat_type"]:
            if (k not in nb) or (nb.get(k) in (None, "", "nan")):
                nb[k] = meta.get(k, "")
        out.append(nb)
    return out

def list_available_stock_ids(data_path: str) -> list:
    """
    從 ./data/*_whale_track.json 掃描出 stock_id 清單（字串），並排序
    """
    pattern = os.path.join(data_path, "*_whale_track.json")
    ids = []
    for p in glob.glob(pattern):
        base = os.path.basename(p)
        # 例如 2330_whale_track.json → 2330
        sid = base.replace("_whale_track.json", "").strip()
        if sid.isdigit():
            ids.append(sid)
    return sorted(set(ids), key=lambda x: int(x))


def load_backtest_signals(data_path: str) -> tuple[pd.DataFrame | None, str | None]:
    """
    嘗試讀取 backtest_signals_60d.csv，回傳 DataFrame 與檔案路徑。
    """
    csv_path = os.path.join(data_path, "backtest_signals_60d.csv")
    if not os.path.exists(csv_path):
        return None, None
    try:
        df = pd.read_csv(csv_path)
        return df, csv_path
    except Exception as e:
        print(f"⚠️ 讀取 backtest_signals_60d.csv 失敗: {repr(e)}")
        return None, csv_path


def summarize_backtest_by_state(df: pd.DataFrame) -> list[dict]:
    """
    依 stock_id × monitor_state 匯總：
      - 樣本數 n
      - ret_5d/10d/20d 平均
      - ret_5d/10d/20d 勝率
    """
    if df is None or df.empty:
        return []

    for col in ["ret_5d", "ret_10d", "ret_20d"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.copy()
    df["monitor_state"] = df.get("monitor_state", "NEUTRAL").fillna("NEUTRAL").astype(str)

    groups = df.groupby(["stock_id", "monitor_state"], dropna=False)

    stats: list[dict] = []
    for (stock_id, state), g in groups:
        row: dict = {
            "stock_id": str(stock_id),
            "monitor_state": str(state),
            "n": int(len(g)),
        }
        for h in [5, 10, 20]:
            col = f"ret_{h}d"
            if col in g.columns:
                series = g[col].dropna()
                if len(series) > 0:
                    row[f"ret_{h}d_avg"] = float(series.mean())
                    row[f"ret_{h}d_win"] = float((series > 0).mean())
                else:
                    row[f"ret_{h}d_avg"] = 0.0
                    row[f"ret_{h}d_win"] = 0.0
            else:
                row[f"ret_{h}d_avg"] = 0.0
                row[f"ret_{h}d_win"] = 0.0
        stats.append(row)

    order = ["ACCUMULATION", "MARKUP", "FADING", "DISTRIBUTION", "NEUTRAL"]
    def sort_key(r: dict):
        st = r.get("monitor_state", "NEUTRAL")
        idx = order.index(st) if st in order else len(order)
        return (int(r.get("stock_id", "0") or 0), idx)

    stats.sort(key=sort_key)
    return stats


def summarize_backtest_by_score(df: pd.DataFrame) -> list[dict]:
    """
    依 stock_id × score 區間（0-40 / 40-60 / 60-80 / 80+）匯總：
      - 樣本數 n
      - ret_5d/10d/20d 平均
      - ret_5d/10d/20d 勝率
    """
    if df is None or df.empty:
        return []

    df = df.copy()

    # 先決定有效分數：優先 final_score，其次 score/ trend_score
    score_cols = ["final_score", "score", "trend_score"]
    score_series = None
    for c in score_cols:
        if c in df.columns:
            score_series = pd.to_numeric(df[c], errors="coerce")
            break
    if score_series is None:
        return []

    df["score_eff"] = score_series.fillna(0.0)

    def bucket_label(x: float) -> str:
        if x < 40:
            return "0-40"
        if x < 60:
            return "40-60"
        if x < 80:
            return "60-80"
        return "80+"

    df["score_bucket"] = df["score_eff"].apply(bucket_label)

    for col in ["ret_5d", "ret_10d", "ret_20d"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    groups = df.groupby(["stock_id", "score_bucket"], dropna=False)

    stats: list[dict] = []
    for (stock_id, bucket), g in groups:
        row: dict = {
            "stock_id": str(stock_id),
            "score_bucket": str(bucket),
            "n": int(len(g)),
        }
        for h in [5, 10, 20]:
            col = f"ret_{h}d"
            if col in g.columns:
                series = g[col].dropna()
                if len(series) > 0:
                    row[f"ret_{h}d_avg"] = float(series.mean())
                    row[f"ret_{h}d_win"] = float((series > 0).mean())
                else:
                    row[f"ret_{h}d_avg"] = 0.0
                    row[f"ret_{h}d_win"] = 0.0
            else:
                row[f"ret_{h}d_avg"] = 0.0
                row[f"ret_{h}d_win"] = 0.0
        stats.append(row)

    bucket_order = ["0-40", "40-60", "60-80", "80+"]

    def sort_key(r: dict):
        b = r.get("score_bucket", "0-40")
        idx = bucket_order.index(b) if b in bucket_order else len(bucket_order)
        return (int(r.get("stock_id", "0") or 0), idx)

    stats.sort(key=sort_key)
    return stats

# ======================================================
# C. 籌碼規則（TWSE T86 → 近 5 日外資+投信同買）
# ======================================================
def check_stock_monk_rule(stock_id: str):
    date_str = datetime.now().strftime("%Y%m%d")
    chip_file = os.path.join(DATA_PATH, f"{stock_id}_chip.csv")

    # 1) 抓今日 T86
    try:
        resp = requests.get(
            TWSE_T86_URL,
            params={"date": date_str, "selectType": "ALL", "response": "json"},
            timeout=20,
        )
        js = resp.json()
        if js.get("stat") == "OK":
            df = pd.DataFrame(js.get("data", []), columns=js.get("fields", []))
            if not df.empty and "證券代號" in df.columns:
                row = df[df["證券代號"].astype(str).str.strip() == str(stock_id)].copy()
                if not row.empty:
                    row.insert(0, "日期", date_str)
                    row = clean_numeric_columns(row, ["外資買賣超股數", "投信買賣超股數"])
                    append_unique_row_csv(chip_file, row, key_col="日期")
    except Exception as e:
        return False, f"⚠️ 籌碼查詢失敗(TWSE): {repr(e)}"

    # 2) 讀本地近 5 日判斷
    if not os.path.exists(chip_file):
        return False, "⚠️ 尚無籌碼歷史檔，請先累積資料。"

    try:
        hist = pd.read_csv(chip_file)
        if hist.empty or "外資買賣超股數" not in hist.columns or "投信買賣超股數" not in hist.columns:
            return False, "⚠️ 籌碼檔欄位不足。"

        hist = hist.tail(5).copy()
        hist = clean_numeric_columns(hist, ["外資買賣超股數", "投信買賣超股數"])

        f_sum = float(hist["外資買賣超股數"].sum())
        t_sum = float(hist["投信買賣超股數"].sum())

        if f_sum > 0 and t_sum > 0:
            return True, f"✅ 籌碼合格！(外資:{f_sum:,.0f}, 投信:{t_sum:,.0f})"
        return False, f"❌ 籌碼不合。(外資:{f_sum:,.0f}, 投信:{t_sum:,.0f})"
    except Exception as e:
        return False, f"⚠️ 籌碼檔讀取失敗: {repr(e)}"


# ======================================================
# D. 路由設定
# ======================================================
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    if not data:
        return jsonify({"status": "error", "msg": "no json body"}), 400

    ticker = str(data.get("ticker", "")).replace("TWSE:", "").replace("TPEX:", "").strip()
    price = str(data.get("price", "0")).strip()

    if not ticker.isdigit():
        return jsonify({"status": "error", "msg": "invalid ticker"}), 400

    is_pass, chip_status = check_stock_monk_rule(ticker)

    msg = (
        f"🔔 RB 訊號觸發: {ticker}\n"
        f"現價: {price}\n"
        f"策略: 均線5%紀律\n"
        f"狀態: {chip_status}"
    )
    send_telegram(msg)

    return jsonify({"status": "success", "ticker": ticker, "chip_pass": is_pass}), 200


@app.route("/dashboard/")
def dashboard():
    stock_id = request.args.get("stock_id", "6239").strip()

    # 防呆：只允許數字
    if not stock_id.isdigit():
        stock_id = "6239"

    available_ids = list_available_stock_ids(DATA_PATH)
    available_stocks = [{"id": sid, "name": get_stock_name(sid)} for sid in available_ids]

    stock_name = get_stock_name(stock_id)

    json_path = os.path.join(DATA_PATH, f"{stock_id}_whale_track.json")
    # 對 dashboard 也使用 rawdata 版本的券商主檔，確保 GEO/距離資訊齊全
    broker_map = load_broker_master_enriched(RAW_PATH)

    stock_info = {
        "id": stock_id,
        "name": stock_name,
        "status": "⚪ 等待數據載入中",
        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "probe_date": "",
        # 走向/訊號（新版放 signals）
        "signals": {},
        # Top6 軌跡圖
        "history_labels": [],
        "whale_data": [],
        "total_whale_values": [],
        # Top6 表
        "top6_details": [],
    }

    if os.path.exists(json_path):
        try:
            # ✅ 先讀 JSON
            with open(json_path, "r", encoding="utf-8") as f:
                track_data = json.load(f)

            # ✅ signals + 統一分數
            signals = track_data.get("signals", {}) or {}
            if not isinstance(signals, dict):
                signals = {}

            final_score = signals.get("final_score", None)
            score_raw = signals.get("score", 0)
            signals["score_unified"] = final_score if final_score is not None else score_raw
            stock_info["signals"] = signals

            # ✅ Geo KPI（給 dashboard.html 直接使用）
            stock_info["geo"] = {
                "grade": signals.get("geo_grade"),
                "tag": signals.get("geo_tag"),
                "zscore": signals.get("geo_zscore"),
                "wavg_km": signals.get("geo_top5_wavg_km"),
                "avg_km": signals.get("geo_top5_avg_km"),
                "min_km": signals.get("geo_top5_min_km"),
                "affinity": signals.get("geo_affinity_score"),
                "baseline_tag": signals.get("geo_baseline_tag"),
                "baseline_weight": signals.get("geo_baseline_weight"),
                "adjust": signals.get("geo_adjust"),
                "top5": signals.get("geo_top5_detail", []) or [],
            }

            # --- flags: 直接用現有 signals 欄位推導（不用改 scraper） ---
            f5 = float(signals.get("foreign_net_5d", 0) or 0)
            l5 = float(signals.get("local_net_5d", 0) or 0)

            buy5  = float(signals.get("buy_count_5d", 0) or 0)
            sell5 = float(signals.get("sell_count_5d", 0) or 0)

            score_u = float(signals.get("score_unified", 0) or 0)
            trend = str(signals.get("trend", "") or "")

            signals["flags"] = {
                # 外/本分歧：一正一負（方向相反）
                "fb_diverge": 1 if (f5 > 0 and l5 < 0) or (f5 < 0 and l5 > 0) else 0,

                # 實盤擴散：買超家數 > 賣超家數（可自行改門檻）
                "breadth_expand": 1 if (buy5 - sell5) > 0 else 0,

                # 淨賣主導：trend 出現「偏空」或主力得分很低
                "net_sell_dominate": 1 if ("偏空" in trend) or (score_u <= 30) else 0,

                # 強度高：主力得分 >= 75（可調）
                "strength_high": 1 if score_u >= 75 else 0,
            }

            # ✅ 主要圖表欄位
            stock_info["history_labels"] = track_data.get("history_labels", []) or []
            stock_info["whale_data"] = track_data.get("whale_data", []) or []
            stock_info["total_whale_values"] = track_data.get("total_whale_values", []) or []
            top6 = track_data.get("top6_details", []) or []
            stock_info["top6_details"] = enrich_top6_details(top6, broker_map)

            # ✅ 更新時間：用檔案 mtime
            mtime = datetime.fromtimestamp(os.path.getmtime(json_path)).strftime("%Y-%m-%d %H:%M")
            stock_info["last_update"] = mtime

            # ✅ 參考日：用 labels 的最後一天（若有）
            if stock_info["history_labels"]:
                stock_info["probe_date"] = stock_info["history_labels"][-1]

            # ✅ 狀態字：用 score_unified（你問的這行就放這裡）
            score_for_status = stock_info["signals"].get("score_unified", 0)
            c5 = stock_info["signals"].get("concentration_5d", 0)
            c20 = stock_info["signals"].get("concentration_20d", 0)
            trend = stock_info["signals"].get("trend", "觀察中")
            stock_info["status"] = f"✅ 籌碼訊號已載入 | Score={score_for_status} | 5D={c5}% | 20D={c20}% | {trend}"

        except Exception as e:
            stock_info["status"] = f"⚠️ JSON 讀取失敗: {repr(e)}"

    return render_template("dashboard.html", stock=stock_info, available_stocks=available_stocks)


@app.route("/analytics/")
def analytics():
    df, csv_path = load_backtest_signals(DATA_PATH)
    if df is None:
        return render_template(
            "analytics.html",
            error="找不到 backtest_signals_60d.csv，請先執行 SubPY/backtest_signals_60d.py 產生回測樣本。",
            csv_name=csv_path or "backtest_signals_60d.csv",
            total_samples=0,
            generated_at="N/A",
            stats=[],
            stats_score=[],
            available_stock_ids=[],
            selected_stock_ids=[],
            filter_applied=False,
        )

    df["stock_id"] = df["stock_id"].astype(str)
    available_stock_ids = sorted(df["stock_id"].dropna().unique().tolist())
    selected_stock_ids = request.args.getlist("stock_ids")
    filter_requested = "filter" in request.args
    if filter_requested:
        if selected_stock_ids:
            df = df[df["stock_id"].isin(selected_stock_ids)].copy()
        else:
            df = df.iloc[0:0].copy()
    # 未送篩選時：顯示全部

    stats = summarize_backtest_by_state(df)
    stats_score = summarize_backtest_by_score(df)
    total_samples = len(df)
    try:
        mtime = datetime.fromtimestamp(os.path.getmtime(csv_path)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        mtime = "N/A"

    return render_template(
        "analytics.html",
        error=None,
        csv_name=os.path.basename(csv_path) if csv_path else "backtest_signals_60d.csv",
        total_samples=total_samples,
        generated_at=mtime,
        stats=stats,
        stats_score=stats_score,
        available_stock_ids=available_stock_ids,
        selected_stock_ids=selected_stock_ids,
        filter_applied=filter_requested,
    )

if __name__ == "__main__":
    # 本機建議 5000；GCP/容器可用環境變數 PORT
    port = int(os.getenv("PORT", "80"))
    app.run(host="0.0.0.0", port=port, debug=False)
