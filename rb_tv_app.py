import os
import time
import json
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
FINMIND_TDR_URL = "https://api.finmindtrade.com/api/v4/taiwan_stock_trading_daily_report"

# 建議：預設先 True；若你公司網路/憑證有問題可改 False
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
            api_status = js.get("status")
            if r.status_code == 200 and api_status == 200:
                return pd.DataFrame(js.get("data", []))
            return pd.DataFrame()
        except requests.exceptions.SSLError:
            # SSL 驗證失敗時自動降級（避免整站掛）
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

    def request_trading_daily_report(self, stock_id: str, date_yyyy_mm_dd: str) -> pd.DataFrame:
        params = {"data_id": stock_id, "date": date_yyyy_mm_dd}
        try:
            r = self.session.get(FINMIND_TDR_URL, params=params, timeout=30, verify=self.verify_ssl)
            js = r.json() if r.content else {}
            if r.status_code == 200 and js.get("status") == 200:
                return pd.DataFrame(js.get("data", []))
            return pd.DataFrame()
        except requests.exceptions.SSLError:
            try:
                r = self.session.get(FINMIND_TDR_URL, params=params, timeout=30, verify=False)
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
        existing = pd.read_csv(file_path)
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


def get_recent_trading_dates(days: int = 10, lookback: int = 60):
    start_date = (datetime.now() - timedelta(days=lookback)).strftime("%Y-%m-%d")
    df = fm.request_data("TaiwanStockTradingDate", start_date=start_date)
    if df.empty or "date" not in df.columns:
        return []
    today_str = datetime.now().strftime("%Y-%m-%d")
    dates = [d for d in df["date"].astype(str).tolist() if d < today_str]
    return dates[-days:]


# ======================================================
# C. 核心邏輯：籌碼檢查（TWSE T86 → 近 5 日同買）
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

    ticker = str(data.get("ticker", "")).replace("TWSE:", "").strip()
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

    json_path = os.path.join(DATA_PATH, f"{stock_id}_whale_track.json")
    boss_path = os.path.join(DATA_PATH, f"{stock_id}_boss_list.csv")

    stock_info = {
        "id": stock_id,
        "name": "力成",
        "status": "⚪ 初始化中",
        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "trend": "觀察中",
        "concentration_10d": 0,
        "concentration_5d": 0,
        "history_labels": [],
        "whale_data": [],
        "total_whale_values": [],
        "top6_details": [],
        "boss_list": [],
        "today_action": [],
        "unit": "share",   # 預設以股為單位（前端 /1000 顯示張）
    }

    # 0) 讀 boss_list.csv（可選）
    if os.path.exists(boss_path):
        try:
            boss_df = pd.read_csv(boss_path)
            if not boss_df.empty:
                stock_info["boss_list"] = boss_df.head(10).to_dict("records")
        except Exception:
            pass

    # 1) 讀取 10 日軌跡 JSON
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                track_data = json.load(f)

            stock_info.update({
                "history_labels": track_data.get("history_labels", []),
                "whale_data": track_data.get("whale_data", []),
                "total_whale_values": track_data.get("total_whale_values", []),
                "concentration_10d": track_data.get("concentration_10d", 0),
                "concentration_5d": track_data.get("concentration_5d", 0),
                "top6_details": track_data.get("top6_details", []),
                "status": "✅ 大戶軌跡數據已載入"
            })

            c10 = float(stock_info.get("concentration_10d", 0) or 0)
            if c10 > 15:
                stock_info["trend"] = "🔥 籌碼高度集中，主力強勢吃貨"
            elif c10 > 5:
                stock_info["trend"] = "📈 籌碼溫和緩集"
            else:
                stock_info["trend"] = "⚖️ 籌碼分布中性"

        except Exception as e:
            print(f"⚠️ JSON 讀取失敗: {repr(e)}")

    # 2) 抓取最近一日動作（用 top6_details 過濾）
    recent_dates = get_recent_trading_dates(days=3)
    if recent_dates:
        probe_date = recent_dates[-1]
        daily_df = fm.request_trading_daily_report(stock_id=stock_id, date_yyyy_mm_dd=probe_date)

        if not daily_df.empty:
            if "securities_trader_id" in daily_df.columns and "broker_id" not in daily_df.columns:
                daily_df.rename(columns={"securities_trader_id": "broker_id"}, inplace=True)
            if "securities_trader" in daily_df.columns and "broker_name" not in daily_df.columns:
                daily_df.rename(columns={"securities_trader": "broker_name"}, inplace=True)

            need_cols = {"broker_id", "broker_name", "buy", "sell"}
            if need_cols.issubset(set(daily_df.columns)):
                daily_df["buy"] = pd.to_numeric(daily_df["buy"], errors="coerce").fillna(0)
                daily_df["sell"] = pd.to_numeric(daily_df["sell"], errors="coerce").fillna(0)
                daily_df["net"] = daily_df["buy"] - daily_df["sell"]

                boss_ids = [str(b.get("broker_id")) for b in stock_info.get("top6_details", []) if b.get("broker_id") is not None]
                if boss_ids:
                    today_boss = daily_df[daily_df["broker_id"].astype(str).isin(boss_ids)].copy()
                    if not today_boss.empty:
                        top_one = today_boss.sort_values(by="net", ascending=False).iloc[0]
                        stock_info["status"] = f"✅ {probe_date} 關鍵分點『{top_one['broker_name']}』大買 {int(top_one['net']/1000)} 張！"
                        stock_info["today_action"] = today_boss.sort_values(by="net", ascending=False).head(50).to_dict("records")
            else:
                stock_info["status"] = f"⚠️ {probe_date} 分點欄位不齊，無法計算"

    return render_template("dashboard.html", stock=stock_info)


if __name__ == "__main__":
    # Windows 本機預設 5000（避免 80 權限/佔用/ERR_NGROK_8012）
    port = int(os.getenv("PORT", "80"))
    app.run(host="0.0.0.0", port=port)
