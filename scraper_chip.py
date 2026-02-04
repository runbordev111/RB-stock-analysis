import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import time
from FinMind.data import DataLoader
from dotenv import load_dotenv

load_dotenv()
DATA_PATH = "./data"
os.makedirs(DATA_PATH, exist_ok=True)

api = DataLoader()
token = os.getenv('FINMIND_API_TOKEN')
if token: api.login_by_token(api_token=token)

def clean_numeric_columns(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '').replace('null', '0')
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

# ✨ 自動清理重複資料的函數
def auto_clean_csv(file_path):
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        if '日期' in df.columns:
            df['日期'] = df['日期'].astype(str).str.replace('-', '')
            # 刪除重複日期，保留最後一筆，並按日期排序
            df = df.drop_duplicates(subset=['日期'], keep='last').sort_values('日期')
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            return df
    return None

def get_stock_chip(date_str, stock_id):
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALL&response=json"
    try:
        resp = requests.get(url, timeout=10).json()
        if resp.get('stat') == 'OK':
            df = pd.DataFrame(resp['data'], columns=resp['fields'])
            stock_data = df[df['證券代號'].str.strip() == str(stock_id)].copy()
            if not stock_data.empty:
                rename_map = {'外陸資買賣超股數(不含外資自營商)': '外資買賣超股數'}
                stock_data.rename(columns=rename_map, inplace=True)
                stock_data.insert(0, '日期', date_str)
                return clean_numeric_columns(stock_data, ['外資買賣超股數', '投信買賣超股數'])
    except: pass
    return None

def save_and_merge(df, file_name):
    path = os.path.join(DATA_PATH, file_name)
    df['日期'] = df['日期'].astype(str).str.replace('-', '')
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        old_df['日期'] = old_df['日期'].astype(str).str.replace('-', '')
        combined = pd.concat([old_df, df]).drop_duplicates(subset=['日期'], keep='last')
        combined = combined.sort_values(by='日期').reset_index(drop=True)
        combined.to_csv(path, index=False, encoding='utf-8-sig')
    else:
        df.to_csv(path, index=False, encoding='utf-8-sig')

def find_key_branches(stock_id):
    print(f"🔎 執行 {stock_id} 尋根...")
    for i in range(1, 4):
        target_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            df = api.taiwan_stock_trading_daily_report(stock_id=stock_id, date=target_date)
            if isinstance(df, pd.DataFrame) and not df.empty and 'buy' in df.columns:
                df['net_buy'] = df['buy'] - df['sell']
                top_20 = df.groupby(['broker_id', 'broker_name'])['net_buy'].sum().reset_index()
                top_20 = top_20.sort_values(by='net_buy', ascending=False).head(20)
                top_20.to_csv(os.path.join(DATA_PATH, f"{stock_id}_boss_list.csv"), index=False, encoding='utf-8-sig')
                print(f"✅ {target_date} 分點清單更新成功")
                return top_20
        except: pass
        time.sleep(1)
    return None

def chip_filter_logic(stock_id):
    path = os.path.join(DATA_PATH, f"{stock_id}_chip.csv")
    df = auto_clean_csv(path) # 判定前再掃一次確保乾淨
    if df is None: return False
    
    df = clean_numeric_columns(df, ['外資買賣超股數', '投信買賣超股數'])
    recent_5 = df.tail(5)
    f_sum, i_sum = recent_5['外資買賣超股數'].sum(), recent_5['投信買賣超股數'].sum()
    print(f"📊 統計(近5日) - 外資: {f_sum:,.0f} | 投信: {i_sum:,.0f}")
    return f_sum > 0 and i_sum > 0

if __name__ == "__main__":
    target = "6239"
    chip_file = os.path.join(DATA_PATH, f"{target}_chip.csv")
    
    # 1. 啟動先清理舊資料
    existing_df = auto_clean_csv(chip_file)
    existing_dates = existing_df['日期'].tolist() if existing_df is not None else []

    # 2. 智慧抓取：只抓 CSV 裡沒有的日期
    print(f"🚀 開始更新 {target} 法人數據...")
    found_count = 0
    for i in range(0, 10):
        d = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        if d in existing_dates:
            found_count += 1
            continue # 已有資料，跳過不抓
            
        data = get_stock_chip(d, target)
        if data is not None:
            save_and_merge(data, f"{target}_chip.csv")
            print(f"📝 補抓 {d} 法人資料")
            found_count += 1
        
        if found_count >= 5: break
        time.sleep(0.5)

    find_key_branches(target)
    res = chip_filter_logic(target)
    print(f"💡 最終結果：{'✅ 籌碼合格' if res else '❌ 不合格'}")