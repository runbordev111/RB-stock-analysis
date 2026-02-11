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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
FINMIND_V4_DATA_URL = "https://api.finmindtrade.com/api/v4/data"

# 建議：公司網路/憑證怪怪可改 False（會自動降級）
VERIFY_SSL = True

app = Flask(__name__)


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ======================================================
# A. FinMind Client（不用 FinMind 套件，避免 import 問題）
# ======================================================
class FinMindClient:
    def __init__(self, token: str, verify_ssl: bool = True):
        self.token = (token or "").strip()
        self.verify_ssl = verify_ssl
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
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

        if self.token:
            s.headers.update({"Authorization": f"Bearer {self.token}"})
        return s

    def request_data(self, dataset: str, data_id: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        params = {"dataset": dataset}
        if data_id:
            params["data_id"] = data_id
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        try:
            r = self.session.get(FINMIND_V4_DATA_URL, params=params, timeout=30, verify=self.verify_ssl)
            js = r.json() if r.content else {}
            if r.status_code == 200 and js.get("status") == 200:
                return pd.DataFrame(js.get("data", []))
            return pd.DataFrame()
        except requests.exceptions.SSLError:
            # SSL 失敗自動降級
            try:
                r = self.session.get(FINMIND_V4_DATA_URL, params=params, timeout=30, verify=False)
                js = r.json() if r.content else {}
                if r.status_code == 200 and js.get("status") == 200:
                    return pd.DataFrame(js.get("data", []))
            except Exception:
                pass
            return pd.DataFrame()
        except Exception:
            return pd.DataFrame()


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


def load_broker_master_enriched(data_path: str) -> dict:
    """
    讀 ./data/broker_master_enriched.csv，回傳 mapping:
      broker_id -> {city, broker_org_type, is_proprietary, seat_type, broker_name(optional)}
    若檔案不存在就回空 dict（不讓 dashboard 爆）
    """
    path = os.path.join(data_path, "broker_master_enriched.csv")
    if not os.path.exists(path):
        return {}

    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return {}

    keep_cols = set(df.columns)
    out = {}
    for _, r in df.iterrows():
        bid = str(r.get("broker_id", "")).strip()
        if not bid:
            continue
        out[bid] = {
            "city": str(r.get("city", "")).strip(),
            "broker_org_type": str(r.get("broker_org_type", "")).strip(),  # foreign/local/unknown
            "is_proprietary": str(r.get("is_proprietary", "")).strip(),    # Y/N
            "seat_type": str(r.get("seat_type", "")).strip(),              # hq/branch/aggregate/unknown
        }
        if "broker_name" in keep_cols:
            out[bid]["broker_name"] = str(r.get("broker_name", "")).strip()
    return out


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
    broker_map = load_broker_master_enriched(DATA_PATH)

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

if __name__ == "__main__":
    # 本機建議 5000；GCP/容器可用環境變數 PORT
    port = int(os.getenv("PORT", "80"))
    app.run(host="0.0.0.0", port=port, debug=False)
