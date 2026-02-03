import requests
import pandas as pd
from datetime import datetime
import os

# ======================================================
# 助手函式：移除千分位逗號並轉為浮點數
# ======================================================
def clean_numeric_columns(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '').replace('null', '0')
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

# ======================================================
# 抓取大盤三大法人買賣超統計
# ======================================================
def get_twse_investors(date_str):
    url = f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?date={date_str}&response=json"
    try:
        response = requests.get(url)
        data = response.json()
        if data.get('stat') == 'OK':
            df = pd.DataFrame(data['data'], columns=data['fields'])
            # 清洗數值資料
            df = clean_numeric_columns(df, ['買進金額', '賣出金額', '買賣差額'])
            print(f"📊 {date_str} 大盤三大法人統計：")
            print(df[['單位名稱', '買進金額', '賣出金額', '買賣差額']])
            return df
        else:
            print(f"⚠️ {date_str} 大盤查無資料（假日或未開盤）")
    except Exception as e:
        print(f"❌ 大盤爬取失敗: {e}")
    return None

# ======================================================
# 抓取特定個股的三大法人買賣超 (T86)
# ======================================================
def get_stock_chip(date_str, stock_id):
    """抓取特定個股當日的三大法人買賣超"""
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALL&response=json"
    try:
        response = requests.get(url)
        data = response.json()
        if data.get('stat') == 'OK':
            df = pd.DataFrame(data['data'], columns=data['fields'])
            # 找到指定的股票
            stock_data = df[df['證券代號'].str.strip() == str(stock_id)].copy()
            if not stock_data.empty:
                # 加入日期欄位
                stock_data.insert(0, '日期', date_str)
                # 清洗關鍵數據
                target_cols = ['外資買進股數', '外資賣出股數', '外資買賣超股數', 
                               '投信買進股數', '投信賣出股數', '投信買賣超股數']
                stock_data = clean_numeric_columns(stock_data, target_cols)
                return stock_data
        print(f"⚠️ {date_str} 股票 {stock_id} 查無資料")
    except Exception as e:
        print(f"❌ 錯誤: {e}")
    return None

# ======================================================
# 數據同步至本地資料庫 (CSV)
# ======================================================
DATA_PATH = "./data"
if not os.path.exists(DATA_PATH):
    os.makedirs(DATA_PATH)

def save_to_local_db(stock_id, df):
    """儲存至本地 CSV，避免重複寫入"""
    file_path = os.path.join(DATA_PATH, f"{stock_id}_chip.csv")
    if not os.path.isfile(file_path):
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
    else:
        existing_df = pd.read_csv(file_path)
        # 檢查日期，不重複紀錄
        if str(df['日期'].iloc[0]) not in existing_df['日期'].astype(str).values:
            df.to_csv(file_path, mode='a', header=False, index=False, encoding='utf-8-sig')
    print(f"✅ {stock_id} 數據已存入 {file_path}")

# ======================================================
# 掃地僧過濾邏輯：1. 讀取最近 5 天資料 2. 判斷外資與投信是否同步站買方
# ======================================================
def chip_filter_logic(stock_id):
    file_path = os.path.join(DATA_PATH, f"{stock_id}_chip.csv")
    if not os.path.exists(file_path): 
        print(f"🚫 找不到 {stock_id} 的歷史資料，無法執行過濾邏輯")
        return False
    
    df = pd.read_csv(file_path).tail(5) # 抓最後 5 筆
    
    # 計算外資與投信的總和買賣超
    foreign_buy = df['外資買賣超股數'].sum()
    trust_buy = df['投信買賣超股數'].sum()
    
    print(f"🔍 {stock_id} 近5日籌碼統計：外資總買賣 {foreign_buy}, 投信總買賣 {trust_buy}")
    
    # 過濾條件：外資與投信皆為正數
    if foreign_buy > 0 and trust_buy > 0:
        return True
    return False

# ======================================================
# 測試執行區
# ======================================================
if __name__ == "__main__":
    # 設定測試日期與股票
    test_date = datetime.now().strftime("%Y%m%d") # 您可以手動改為 "20260203"
    target_stock = "2330"

    print(f"🚀 開始執行掃地僧籌碼測試：{target_stock} @ {test_date}")

    # 1. 測試大盤
    get_twse_investors(test_date)

    # 2. 測試個股抓取並存入資料庫
    chip_data = get_stock_chip(test_date, target_stock)
    if chip_data is not None:
        save_to_local_db(target_stock, chip_data)
        
        # 3. 測試過濾邏輯
        is_pass = chip_filter_logic(target_stock)
        print(f"💡 最終過濾結果：{'✅ 通過 (符合大戶趨勢)' if is_pass else '❌ 未通過'}")