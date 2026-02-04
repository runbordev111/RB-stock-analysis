import os
import time
import requests
import pandas as pd
import urllib3
from datetime import datetime, timedelta
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 禁用 SSL 警告（若你環境 SSL 正常，建議改回 verify=True）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =============================
# 基本設定
# =============================
DATA_PATH = "./data"
os.makedirs(DATA_PATH, exist_ok=True)

FINMIND_V4_DATA_URL = "https://api.finmindtrade.com/api/v4/data"
# Sponsor 分點 endpoint（重點：不是 /api/v4/data）
FINMIND_TDR_URL = "https://api.finmindtrade.com/api/v4/taiwan_stock_trading_daily_report"


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ======================================================
# A. API Client 層：處理連線、授權與容錯
# ======================================================
class FinMindClient:
    def __init__(self, token: str, verify_ssl: bool = False):
        self.token = (token or "").strip()
        self.verify_ssl = verify_ssl
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()

        # Retry：針對頻率限制與暫時性錯誤做 backoff
        # connect/read 分開設定，且允許 GET retry
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
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        if self.token:
            session.headers.update({"Authorization": f"Bearer {self.token}"})

        return session

    def _log(self, tag: str, url: str, params: dict, http_status: int, api_status, msg: str, latency: float):
        # 簡潔且可定位問題的 log
        dataset = params.get("dataset", "")
        data_id = params.get("data_id", "")
        date = params.get("date", "")
        start_date = params.get("start_date", "")
        end_date = params.get("end_date", "")

        print(
            f"[{now_str()}] {tag} | http={http_status} api={api_status} "
            f"| dataset={dataset} data_id={data_id} date={date} "
            f"| start={start_date} end={end_date} | latency={latency:.2f}s | msg={msg}"
        )

    def request_data(self, dataset: str, data_id: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        通用 dataset：走 /api/v4/data
        """
        params = {"dataset": dataset}
        if data_id:
            params["data_id"] = data_id
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        t0 = time.time()
        try:
            resp = self.session.get(
                FINMIND_V4_DATA_URL, params=params, timeout=30, verify=self.verify_ssl
            )
            latency = time.time() - t0

            # 解析 JSON（FinMind 回傳通常是 JSON）
            try:
                js = resp.json()
            except Exception:
                self._log("DATA", FINMIND_V4_DATA_URL, params, resp.status_code, "N/A", "non-json response", latency)
                return pd.DataFrame()

            api_status = js.get("status")
            msg = js.get("msg", "")

            self._log("DATA", FINMIND_V4_DATA_URL, params, resp.status_code, api_status, msg, latency)

            if resp.status_code == 200 and api_status == 200:
                return pd.DataFrame(js.get("data", []))
            return pd.DataFrame()

        except Exception as e:
            latency = time.time() - t0
            self._log("DATA", FINMIND_V4_DATA_URL, params, -1, "EXC", repr(e), latency)
            return pd.DataFrame()

    def request_trading_daily_report(self, stock_id: str, date_yyyy_mm_dd: str) -> pd.DataFrame:
        """
        Sponsor 分點：走 /api/v4/taiwan_stock_trading_daily_report
        參數：data_id + date（單日）
        """
        params = {"data_id": stock_id, "date": date_yyyy_mm_dd}

        t0 = time.time()
        try:
            resp = self.session.get(
                FINMIND_TDR_URL, params=params, timeout=30, verify=self.verify_ssl
            )
            latency = time.time() - t0

            try:
                js = resp.json()
            except Exception:
                self._log("TDR", FINMIND_TDR_URL, params, resp.status_code, "N/A", "non-json response", latency)
                return pd.DataFrame()

            api_status = js.get("status")
            msg = js.get("msg", "")

            self._log("TDR", FINMIND_TDR_URL, params, resp.status_code, api_status, msg, latency)

            if resp.status_code == 200 and api_status == 200:
                return pd.DataFrame(js.get("data", []))

            # 權限不足提示（sponsor 常見）
            if api_status in (401, 402, 403) or resp.status_code in (401, 402, 403):
                print(f"⚠️ 分點資料需要 sponsor 權限或 token 無效 | api_status={api_status} | msg={msg}")
            return pd.DataFrame()

        except Exception as e:
            latency = time.time() - t0
            self._log("TDR", FINMIND_TDR_URL, params, -1, "EXC", repr(e), latency)
            return pd.DataFrame()


# ======================================================
# B. Dataset Adapter 層：標準化資料轉換
# ======================================================
class TaiwanStockAdapter:
    def __init__(self, client: FinMindClient):
        self.client = client

    def get_trading_dates(self, lookback=45) -> list:
        # 交易日曆
        start_date = (datetime.now() - timedelta(days=lookback)).strftime("%Y-%m-%d")
        df = self.client.request_data("TaiwanStockTradingDate", start_date=start_date)
        if df.empty or "date" not in df.columns:
            return []
        return df["date"].astype(str).tolist()

    def get_institutional_investors_net(self, stock_id: str, date_yyyy_mm_dd: str) -> tuple[float, float]:
        """
        回傳：外資淨買、投信淨買
        """
        df = self.client.request_data(
            "TaiwanStockInstitutionalInvestorsBuySell",
            data_id=stock_id,
            start_date=date_yyyy_mm_dd,
            end_date=date_yyyy_mm_dd
        )
        if df.empty:
            return 0.0, 0.0

        # 欄位保護
        for c in ["name", "buy", "sell"]:
            if c not in df.columns:
                return 0.0, 0.0

        df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0)
        df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0)

        # 使用明確 key（避免模糊匹配混到其他類別）
        f = df.loc[df["name"] == "Foreign_Investor"]
        i = df.loc[df["name"] == "Investment_Trust"]

        f_net = float(f["buy"].sum() - f["sell"].sum())
        i_net = float(i["buy"].sum() - i["sell"].sum())
        return f_net, i_net

    def get_daily_report(self, stock_id: str, date_yyyy_mm_dd: str) -> pd.DataFrame:
        return self.client.request_trading_daily_report(stock_id, date_yyyy_mm_dd)


# ======================================================
# C. CSV 工具：可選（你若要存法人資料）
# ======================================================
def save_boss_list(df: pd.DataFrame, stock_id: str):
    out_path = os.path.join(DATA_PATH, f"{stock_id}_boss_list.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"💾 輸出完成: {out_path}")


# ======================================================
# D. 策略層 (Strategy)
# ======================================================
def run_strategy(stock_id: str, days: int = 5, throttle_sec: float = 0.8):
    load_dotenv()
    token = os.getenv("FINMIND_API_TOKEN", "").strip()
    if not token:
        print("❌ 找不到 FINMIND_API_TOKEN，請在 .env 設定 FINMIND_API_TOKEN=你的token")
        return

    client = FinMindClient(token=token, verify_ssl=False)
    adapter = TaiwanStockAdapter(client)

    # 1) 取得交易日（排除今天，避免資料尚未入庫）
    all_dates = adapter.get_trading_dates(lookback=60)
    today_str = datetime.now().strftime("%Y-%m-%d")
    valid_dates = [d for d in all_dates if d < today_str]
    target_dates = valid_dates[-days:]

    if not target_dates:
        print("❌ 無法取得有效交易日（交易日曆為空），終止。")
        return

    print(f"🔍 檢查 {stock_id} 近 {days} 交易日法人動向: {target_dates[0]} ~ {target_dates[-1]}")

    # 2) 法人同買過濾
    total_f, total_i = 0.0, 0.0
    for d in target_dates:
        f_net, i_net = adapter.get_institutional_investors_net(stock_id, d)
        total_f += f_net
        total_i += i_net
        time.sleep(0.2)

    print(f"📊 累計外資淨買: {total_f:,.0f} | 累計投信淨買: {total_i:,.0f}")

    if not (total_f > 0 and total_i > 0):
        print("⏭️ 籌碼未達同買標準，跳過分點抓取以節省點數。")
        return

    # 3) 同買 → 觸發 sponsor 分點
    print("🎯 籌碼過濾合格！開始拉取分點資料 (Sponsor Call)...")

    frames = []
    for d in target_dates:
        report = adapter.get_daily_report(stock_id, d)
        if not report.empty:
            frames.append(report)
            print(f"✅ 已抓取 {d} 分點明細: {len(report)} rows")
        else:
            print(f"⚠️ {d} 分點明細為空（可能無資料/權限/入庫延遲）")
        time.sleep(throttle_sec)

    if not frames:
        print("❌ 近五日分點資料皆為空，請檢查 sponsor 權限或日期是否有資料。")
        return

    combined = pd.concat(frames, ignore_index=True)

    # 4) 標準化欄位（依常見欄位：securities_trader_id / securities_trader / buy / sell）
    # 若 API 回傳欄位不同，這裡會自動降級為原樣輸出
    col_map = {
        "securities_trader_id": "broker_id",
        "securities_trader": "broker_name"
    }
    for k, v in col_map.items():
        if k in combined.columns and v not in combined.columns:
            combined.rename(columns={k: v}, inplace=True)

    if all(c in combined.columns for c in ["broker_id", "broker_name", "buy", "sell"]):
        combined["buy"] = pd.to_numeric(combined["buy"], errors="coerce").fillna(0)
        combined["sell"] = pd.to_numeric(combined["sell"], errors="coerce").fillna(0)

        agg = (
            combined
            .groupby(["broker_id", "broker_name"], as_index=False)
            .agg(buy=("buy", "sum"), sell=("sell", "sum"))
        )
        agg["net_buy"] = agg["buy"] - agg["sell"]

        top_20 = agg.sort_values("net_buy", ascending=False).head(20).reset_index(drop=True)
        save_boss_list(top_20, stock_id)
    else:
        # 欄位不齊就先原樣輸出，方便你觀察 schema
        out_path = os.path.join(DATA_PATH, f"{stock_id}_daily_report_raw.csv")
        combined.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"⚠️ 欄位不齊，已輸出原始資料供你檢查 schema: {out_path}")


# =============================
# Main
# =============================
if __name__ == "__main__":
    run_strategy("6239", days=5, throttle_sec=0.8)
