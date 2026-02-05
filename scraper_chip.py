import os
import time
import requests
import pandas as pd
import urllib3
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATA_PATH = "./data"
os.makedirs(DATA_PATH, exist_ok=True)

FINMIND_V4_DATA_URL = "https://api.finmindtrade.com/api/v4/data"
FINMIND_TDR_URL = "https://api.finmindtrade.com/api/v4/taiwan_stock_trading_daily_report"


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class FinMindClient:
    def __init__(self, token: str, verify_ssl: bool = True):
        self.token = (token or "").strip()
        self.verify_ssl = verify_ssl
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
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

    def _log(self, tag: str, params: dict, http_status: int, api_status, msg: str, latency: float):
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
        params = {"dataset": dataset}
        if data_id:
            params["data_id"] = data_id
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        t0 = time.time()
        try:
            resp = self.session.get(FINMIND_V4_DATA_URL, params=params, timeout=30, verify=self.verify_ssl)
            latency = time.time() - t0
            try:
                js = resp.json()
            except Exception:
                self._log("DATA", params, resp.status_code, "N/A", "non-json response", latency)
                return pd.DataFrame()

            api_status = js.get("status")
            msg = js.get("msg", "")
            self._log("DATA", params, resp.status_code, api_status, msg, latency)

            if resp.status_code == 200 and api_status == 200:
                return pd.DataFrame(js.get("data", []))
            return pd.DataFrame()

        except Exception as e:
            latency = time.time() - t0
            self._log("DATA", params, -1, "EXC", repr(e), latency)
            return pd.DataFrame()

    def request_trading_daily_report(self, stock_id: str, date_yyyy_mm_dd: str) -> pd.DataFrame:
        params = {"data_id": stock_id, "date": date_yyyy_mm_dd}
        t0 = time.time()
        try:
            resp = self.session.get(FINMIND_TDR_URL, params=params, timeout=30, verify=self.verify_ssl)
            latency = time.time() - t0
            try:
                js = resp.json()
            except Exception:
                self._log("TDR", params, resp.status_code, "N/A", "non-json response", latency)
                return pd.DataFrame()

            api_status = js.get("status")
            msg = js.get("msg", "")
            self._log("TDR", params, resp.status_code, api_status, msg, latency)

            if resp.status_code == 200 and api_status == 200:
                return pd.DataFrame(js.get("data", []))

            if api_status in (401, 402, 403) or resp.status_code in (401, 402, 403):
                print(f"⚠️ 分點資料需要 sponsor 權限或 token 無效 | api_status={api_status} | msg={msg}")
            return pd.DataFrame()

        except Exception as e:
            latency = time.time() - t0
            self._log("TDR", params, -1, "EXC", repr(e), latency)
            return pd.DataFrame()


class TaiwanStockAdapter:
    def __init__(self, client: FinMindClient):
        self.client = client

    def get_trading_dates(self, lookback=60) -> list:
        start_date = (datetime.now() - timedelta(days=lookback)).strftime("%Y-%m-%d")
        df = self.client.request_data("TaiwanStockTradingDate", start_date=start_date)
        if df.empty or "date" not in df.columns:
            return []
        return df["date"].astype(str).tolist()

    def get_daily_report(self, stock_id: str, date_yyyy_mm_dd: str) -> pd.DataFrame:
        return self.client.request_trading_daily_report(stock_id, date_yyyy_mm_dd)


def analyze_whale_trajectory(frames: list[pd.DataFrame], target_dates: list[str]):
    if not frames:
        return None, None

    combined = pd.concat(frames, ignore_index=True)

    # 欄位標準化
    if "securities_trader_id" in combined.columns and "broker_id" not in combined.columns:
        combined.rename(columns={"securities_trader_id": "broker_id"}, inplace=True)
    if "securities_trader" in combined.columns and "broker_name" not in combined.columns:
        combined.rename(columns={"securities_trader": "broker_name"}, inplace=True)

    # 必要欄位檢查
    need_cols = {"date", "broker_id", "broker_name", "buy", "sell"}
    if not need_cols.issubset(set(combined.columns)):
        return None, None

    # 型別統一
    combined["broker_id"] = combined["broker_id"].astype(str)
    combined["broker_name"] = combined["broker_name"].astype(str)

    combined["buy"] = pd.to_numeric(combined["buy"], errors="coerce").fillna(0)
    combined["sell"] = pd.to_numeric(combined["sell"], errors="coerce").fillna(0)
    combined["net"] = combined["buy"] - combined["sell"]

    # price 欄位若存在才算均價
    has_price = "price" in combined.columns
    if has_price:
        combined["price"] = pd.to_numeric(combined["price"], errors="coerce").fillna(0)

    # 10 日彙總（用 net 排 Top6）
    agg_10d = combined.groupby(["broker_id", "broker_name"], as_index=False).agg(
        buy=("buy", "sum"),
        sell=("sell", "sum"),
        net_buy=("net", "sum")
    )
    top6 = agg_10d.sort_values("net_buy", ascending=False).head(6).copy()
    top6_ids = top6["broker_id"].astype(str).tolist()

    last_1d = target_dates[-1]
    last_5d = target_dates[-5:]

    # top6_details（給 dashboard 做監控用）
    top6_details = []
    for _, r in top6.iterrows():
        bid = str(r["broker_id"])
        bname = r["broker_name"]
        bdata = combined[combined["broker_id"] == bid]

        n10d = float(r["net_buy"]) / 1000
        n5d = float(bdata[bdata["date"].isin(last_5d)]["net"].sum()) / 1000
        n1d = float(bdata[bdata["date"] == last_1d]["net"].sum()) / 1000

        avg_p = 0.0
        if has_price:
            buy_only = bdata[bdata["buy"] > 0]
            if not buy_only.empty and buy_only["buy"].sum() > 0:
                avg_p = float((buy_only["buy"] * buy_only["price"]).sum() / buy_only["buy"].sum())

        top6_details.append({
            "broker_id": bid,
            "broker_name": bname,
            "net_10d": round(n10d, 1),
            "net_5d": round(n5d, 1),
            "net_1d": round(n1d, 1),
            "avg_price": round(avg_p, 2)
        })

    # 軌跡矩陣（Top6）
    whale_detail = combined[combined["broker_id"].isin(top6_ids)].copy()
    pivot_net = whale_detail.pivot_table(
        index="date", columns="broker_name", values="net", aggfunc="sum"
    ).fillna(0)

    pivot_cumsum = pivot_net.reindex(target_dates).fillna(0).cumsum()

    # 顏色只是展示用途
    colors = ["#FF6384", "#36A2EB", "#FFCE56", "#4BC0C0", "#9966FF", "#FF9F40"]
    whale_data = []
    for i, name in enumerate(pivot_cumsum.columns):
        whale_data.append({
            "name": name,
            "values": (pivot_cumsum[name] / 1000).round(1).tolist(),
            "color": colors[i % len(colors)]
        })

    # 集中度（可解釋、穩定）：Top6 買入量 / 全市場買入量
    total_buy = float(combined["buy"].sum())
    top6_buy = float(whale_detail["buy"].sum())
    concentration_10d = round((top6_buy / total_buy) * 100, 2) if total_buy > 0 else 0.0

    # 5 日集中度
    comb_5d = combined[combined["date"].isin(last_5d)]
    whale_5d = whale_detail[whale_detail["date"].isin(last_5d)]
    total_buy_5d = float(comb_5d["buy"].sum())
    top6_buy_5d = float(whale_5d["buy"].sum())
    concentration_5d = round((top6_buy_5d / total_buy_5d) * 100, 2) if total_buy_5d > 0 else 0.0

    insight = {
        "history_labels": [d[5:] for d in target_dates],  # MM-DD
        "whale_data": whale_data,
        "concentration_10d": concentration_10d,
        "concentration_5d": concentration_5d,
        "total_whale_values": (pivot_cumsum.sum(axis=1) / 1000).round(1).tolist(),
        "top6_details": top6_details
    }

    # boss_list.csv（給尋根表格用）— 欄位契約：broker_id, broker_name, buy, sell, net_buy
    boss_list_df = agg_10d.sort_values("net_buy", ascending=False).head(20).reset_index(drop=True)

    # ====== Join 城市（FinMind 主檔 + TWSE 備援）======
    def load_city_map(data_path: str) -> dict:
        finmind_path = os.path.join(data_path, "broker_master_finmind_city.csv")
        twse_path = os.path.join(data_path, "broker_master_city.csv")

        city_map = {}

        # 先載入 FinMind（覆蓋更廣）
        if os.path.exists(finmind_path):
            df = pd.read_csv(finmind_path, dtype=str).fillna("")
            for _, r in df.iterrows():
                bid = str(r.get("broker_id", "")).strip()
                c = str(r.get("city", "")).strip()
                if bid:
                    city_map[bid] = c

        # 再用 TWSE 覆蓋（若 TWSE 的縣市較完整/較準就覆蓋）
        if os.path.exists(twse_path):
            df = pd.read_csv(twse_path, dtype=str).fillna("")
            for _, r in df.iterrows():
                bid = str(r.get("broker_id", "")).strip()
                c = str(r.get("city", "")).strip()
                if bid and c:
                    city_map[bid] = c

        return city_map

    def classify_seat(broker_id: str, broker_name: str, city: str) -> str:
        """
        local_branch: 可地理追蹤（有 city）
        foreign_inst: 外資席位（無 city 且名稱命中外資）
        inst_seat:    法人席位（無 city 但不一定是外資）
        """
        name = (broker_name or "").strip()

        if city and str(city).strip():
            return "local_branch"

        foreign_keywords = ["摩根", "花旗", "匯豐", "高盛", "瑞銀", "野村", "德意志", "巴黎", "麥格理", "美林"]
        if any(k in name for k in foreign_keywords):
            return "foreign_inst"

        return "inst_seat"

    city_map = load_city_map(DATA_PATH)

    # 一次寫入 city + seat_type
    color_map = {w["name"]: w["color"] for w in whale_data}    
    for b in top6_details:
        bid = str(b.get("broker_id", "")).strip()
        b["color"] = color_map.get(str(b.get("broker_name", "")), "#ffffff")
        b["city"] = city_map.get(bid, "")
        b["seat_type"] = classify_seat(
            broker_id=bid,
            broker_name=str(b.get("broker_name", "")),
            city=str(b.get("city", "")),
        )

    return insight, boss_list_df

def run_strategy(stock_id: str, days: int = 10, throttle_sec: float = 0.6):
    load_dotenv()
    token = os.getenv("FINMIND_API_TOKEN", "").strip()
    if not token:
        print("❌ 找不到 FINMIND_API_TOKEN，請在 .env 設定 FINMIND_API_TOKEN=你的token")
        return

    client = FinMindClient(token=token, verify_ssl=True)
    adapter = TaiwanStockAdapter(client)

    # 1) 最近交易日（排除今天）
    all_dates = adapter.get_trading_dates(lookback=90)
    today_str = datetime.now().strftime("%Y-%m-%d")
    target_dates = [d for d in all_dates if d < today_str][-days:]

    if not target_dates:
        print("❌ 無法取得交易日，終止。")
        return

    if len(target_dates) < days:
        print(f"⚠️ 交易日不足 {days} 天，實際取得 {len(target_dates)} 天: {target_dates[0]} ~ {target_dates[-1]}")

    # 2) 抓取分點明細
    print(f"🎯 開始追蹤 {stock_id} 近 {len(target_dates)} 日大戶軌跡...")
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
    insight, boss_list_df = analyze_whale_trajectory(frames, target_dates)

    if insight is None or boss_list_df is None:
        print("❌ 分析失敗：分點欄位不齊或資料格式異常。")
        return

    json_path = os.path.join(DATA_PATH, f"{stock_id}_whale_track.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(insight, f, ensure_ascii=False, indent=4)
    print(f"💾 JSON輸出完成: {json_path}")

    csv_path = os.path.join(DATA_PATH, f"{stock_id}_boss_list.csv")
    boss_list_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"💾 CSV輸出完成: {csv_path}")

    print(f"📊 分析完成：10日集中度 {insight['concentration_10d']}% | 5日集中度 {insight['concentration_5d']}%")


if __name__ == "__main__":
    run_strategy("6239", days=10, throttle_sec=0.6)
