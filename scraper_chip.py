# scraper_chip.py

import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# =============================
# 基本設定
# =============================
load_dotenv()
DATA_PATH = "./data"
os.makedirs(DATA_PATH, exist_ok=True)

FINMIND_TOKEN = os.getenv("FINMIND_API_TOKEN", "").strip()

TWSE_T86_URL = "https://www.twse.com.tw/rwd/zh/fund/T86"
FINMIND_V4_URL = "https://api.finmindtrade.com/api/v4/data"


def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# =============================
# 數據清洗
# =============================
def clean_numeric_columns(df: pd.DataFrame, columns):
    for col in columns:
        if col not in df.columns:
            continue
        s = df[col].astype(str)
        s = s.str.replace(",", "", regex=False)
        s = s.str.replace("null", "0", regex=False)
        s = s.str.replace("--", "0", regex=False)
        s = s.str.strip()
        df[col] = pd.to_numeric(s, errors="coerce").fillna(0)
    return df


def auto_clean_csv(file_path: str):
    if not os.path.exists(file_path):
        return None
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"❌ [{_now_str()}] 讀取 CSV 失敗：{file_path} | {repr(e)}")
        return None

    if "日期" in df.columns:
        df["日期"] = df["日期"].astype(str).str.replace("-", "", regex=False).str.strip()
        df = (
            df.drop_duplicates(subset=["日期"], keep="last")
            .sort_values("日期")
            .reset_index(drop=True)
        )
        try:
            df.to_csv(file_path, index=False, encoding="utf-8-sig")
        except Exception as e:
            print(f"❌ [{_now_str()}] 寫入 CSV 失敗：{file_path} | {repr(e)}")
    return df


def save_and_merge(df: pd.DataFrame, file_name: str):
    path = os.path.join(DATA_PATH, file_name)
    df = df.copy()
    df["日期"] = df["日期"].astype(str).str.replace("-", "", regex=False).str.strip()

    if os.path.exists(path):
        try:
            old_df = pd.read_csv(path)
            if "日期" in old_df.columns:
                old_df["日期"] = old_df["日期"].astype(str).str.replace("-", "", regex=False).str.strip()
            combined = pd.concat([old_df, df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["日期"], keep="last")
            combined = combined.sort_values(by="日期").reset_index(drop=True)
            combined.to_csv(path, index=False, encoding="utf-8-sig")
        except Exception as e:
            print(f"❌ [{_now_str()}] 合併寫入失敗：{path} | {repr(e)}")
    else:
        try:
            df.to_csv(path, index=False, encoding="utf-8-sig")
        except Exception as e:
            print(f"❌ [{_now_str()}] 新建寫入失敗：{path} | {repr(e)}")


# =============================
# TWSE 法人籌碼
# =============================
def get_stock_chip(date_str_yyyymmdd: str, stock_id: str, max_retries: int = 4):
    params = {"date": date_str_yyyymmdd, "selectType": "ALL", "response": "json"}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    session = requests.Session()
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            # TWSE 比較嚴格，超時設定長一點
            resp = session.get(TWSE_T86_URL, params=params, headers=headers, timeout=(10, 30))
            resp.raise_for_status()
            js = resp.json()
            if js.get("stat") != "OK":
                return None
            df = pd.DataFrame(js["data"], columns=js["fields"])
            stock_data = df[df["證券代號"].astype(str).str.strip() == str(stock_id)].copy()
            if stock_data.empty:
                return None
            rename_map = {"外陸資買賣超股數(不含外資自營商)": "外資買賣超股數"}
            stock_data.rename(columns=rename_map, inplace=True)
            stock_data.insert(0, "日期", date_str_yyyymmdd)
            stock_data = clean_numeric_columns(stock_data, ["外資買賣超股數", "投信買賣超股數"])
            return stock_data
        except Exception as e:
            last_err = e
            sleep_s = 2 ** attempt # 稍微加長重試間隔
            print(f"⚠️ TWSE 失敗重試({attempt}/{max_retries}) | {date_str_yyyymmdd} | {repr(e)} | sleep {sleep_s}s")
            time.sleep(sleep_s)
    return None

def update_chip_csv(stock_id: str, need_days: int = 5, scan_calendar_days: int = 12):
    chip_file = os.path.join(DATA_PATH, f"{stock_id}_chip.csv")
    existing_df = auto_clean_csv(chip_file)
    existing_dates = set(existing_df["日期"].astype(str).tolist()) if existing_df is not None else set()

    print(f"🚀 開始更新 {stock_id} 法人數據...")
    got = 0
    for i in range(0, scan_calendar_days):
        d = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        if d in existing_dates:
            got += 1
            if got >= need_days: break
            continue
        data = get_stock_chip(d, stock_id)
        if data is not None:
            save_and_merge(data, f"{stock_id}_chip.csv")
            print(f"📝 補抓 {d} 法人資料")
            got += 1
        if got >= need_days: break
        time.sleep(1.5) # TWSE 建議間隔 1.5s 以上

# =============================
# FinMind 分點尋根 (Sponsor 會員版)
# =============================
def finmind_get_dataset(dataset: str, data_id: str, start_date: str, end_date: str = None):
    params = {"dataset": dataset, "data_id": data_id, "start_date": start_date}
    if end_date: params["end_date"] = end_date
    if FINMIND_TOKEN: params["token"] = FINMIND_TOKEN
    try:
        r = requests.get(FINMIND_V4_URL, params=params, timeout=25)
        js = r.json()
        if js.get("status") == 200:
            return 200, "", pd.DataFrame(js.get("data", [])), js
        return js.get("status"), js.get("msg", "error"), None, js
    except Exception as e:
        return -1, repr(e), None, None

def find_key_branches(stock_id: str, lookback_days: int = 10):
    out_path = os.path.join(DATA_PATH, f"{stock_id}_boss_list.csv")
    
    # ✨ 點數優化：快取檢查
    if os.path.exists(out_path):
        mtime = datetime.fromtimestamp(os.path.getmtime(out_path))
        if mtime.date() == datetime.now().date():
            print(f"♻️ [{_now_str()}] 今日已抓取過 {stock_id} 分點，使用快取避免重複扣點。")
            return pd.read_csv(out_path)

    print(f"🔎 執行 {stock_id} 尋根 (分點抓取 - 扣除 FinMind 點數)...")
    for i in range(1, lookback_days + 1):
        d = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        status, msg, df, _ = finmind_get_dataset("TaiwanStockTradingDailyReport", stock_id, d, d)
        
        if status in (402, 429):
            print(f"⚠️ FinMind 點數用盡或限流 | status={status}")
            return None
        if status == 200 and df is not None and not df.empty:
            # 欄位正規化
            df.columns = [str(c).replace("'", "").replace('"', "").strip() for c in df.columns]
            df.rename(columns={"securities_trader_id": "broker_id", "securities_trader": "broker_name"}, inplace=True)
            df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0)
            df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0)
            df["net_buy"] = df["buy"] - df["sell"]
            top_20 = df.groupby(["broker_id", "broker_name"])["net_buy"].sum().reset_index().sort_values("net_buy", ascending=False).head(20)
            top_20.to_csv(out_path, index=False, encoding="utf-8-sig")
            print(f"✅ [{_now_str()}] {d} 分點更新成功")
            return top_20
        time.sleep(1)
    return None

# =============================
# 判斷邏輯
# =============================
def chip_filter_logic(stock_id: str, days: int = 5):
    path = os.path.join(DATA_PATH, f"{stock_id}_chip.csv")
    df = auto_clean_csv(path)
    if df is None or df.empty: return False
    df = clean_numeric_columns(df, ["外資買賣超股數", "投信買賣超股數"])
    recent = df.tail(days)
    f_sum, i_sum = recent["外資買賣超股數"].sum(), recent["投信買賣超股數"].sum()
    print(f"📊 統計(近{days}日) - 外資: {f_sum:,.0f} | 投信: {i_sum:,.0f}")
    if f_sum <= 0: print("   -> ❌ 外資累計未達標")
    if i_sum <= 0: print("   -> ❌ 投信累計未達標")
    return (f_sum > 0 and i_sum > 0)

# =============================
# 主程式 (省點數版)
# =============================
if __name__ == "__main__":
    target = "6239"

    # 1. 抓法人 (免費資料)
    update_chip_csv(target, need_days=5)

    # 2. 判斷籌碼
    is_chip_ok = chip_filter_logic(target)

    # 3. 籌碼合格才進分點抓取 (省點數邏輯)
    if is_chip_ok:
        print("🎯 籌碼合格，執行分點尋根...")
        find_key_branches(target)
    else:
        print("⏭️ 籌碼不合格，跳過分點抓取以節省點數。")

    print(f"💡 最終結果：{'✅ 合格' if is_chip_ok else '❌ 不合格'}")