import os
import requests
import pandas as pd
from datetime import datetime
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# 讀取 .env 檔案中的 Telegram 金鑰
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

app = Flask(__name__)

# ======================================================
# 1. 助手函式：資料清洗與儲存
# ======================================================
DATA_PATH = "./data"
if not os.path.exists(DATA_PATH):
    os.makedirs(DATA_PATH)

def clean_numeric_columns(df, columns):
    """移除千分位逗號並轉為浮點數"""
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '').replace('null', '0')
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def save_to_local_db(stock_id, df):
    """將籌碼資料存入本地 CSV，避免重複紀錄"""
    file_path = os.path.join(DATA_PATH, f"{stock_id}_chip.csv")
    if not os.path.isfile(file_path):
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
    else:
        existing_df = pd.read_csv(file_path)
        if str(df['日期'].iloc[0]) not in existing_df['日期'].astype(str).values:
            df.to_csv(file_path, mode='a', header=False, index=False, encoding='utf-8-sig')

# ======================================================
# 2. 籌碼抓取與過濾邏輯 (掃地僧核心)
# ======================================================
def check_stock_monk_rule(stock_id):
    """執行完整過濾流程：抓取 -> 存檔 -> 判斷趨勢"""
    date_str = datetime.now().strftime("%Y%m%d")
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALL&response=json"
    
    try:
        response = requests.get(url)
        data = response.json()
        if data.get('stat') == 'OK':
            df = pd.DataFrame(data['data'], columns=data['fields'])
            stock_data = df[df['證券代號'].str.strip() == str(stock_id)].copy()
            
            if not stock_data.empty:
                stock_data.insert(0, '日期', date_str)
                stock_data = clean_numeric_columns(stock_data, ['外資買賣超股數', '投信買賣超股數'])
                save_to_local_db(stock_id, stock_data)
        
        # 執行過濾：讀取 CSV 判斷最近 5 日趨勢
        file_path = os.path.join(DATA_PATH, f"{stock_id}_chip.csv")
        if os.path.exists(file_path):
            hist_df = pd.read_csv(file_path).tail(5)
            foreign_sum = hist_df['外資買賣超股數'].sum()
            trust_sum = hist_df['投信買賣超股數'].sum()
            
            if foreign_sum > 0 and trust_sum > 0:
                return True, f"✅ 籌碼合格！(外資:{foreign_sum}, 投信:{trust_sum})"
            return False, f"❌ 籌碼不合。(外資:{foreign_sum}, 投信:{trust_sum})"
            
    except Exception as e:
        return False, f"⚠️ 籌碼查詢失敗: {str(e)}"
    
    return False, "⚠️ 查無今日籌碼"

# ======================================================
# 3. Webhook 接收端與 Telegram 發送
# ======================================================
def send_telegram_msg(msg):
    """發送訊息至老闆的手機"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}
    requests.post(url, json=payload)

@app.route('/webhook', methods=['POST'])
def webhook():
    """接收 TradingView 訊號的入口"""
    data = request.json
    if not data:
        return jsonify({"status": "error"}), 400

    ticker = data.get('ticker', '未知股票').replace('TWSE:', '')
    price = data.get('price', '0')

    # 執行掃地僧籌碼檢查
    is_pass, chip_status = check_stock_monk_rule(ticker)

    # 組裝訊息
    title = f"🔔 RB 訊號觸發: {ticker}"
    body = f"\n現價: {price}\n策略: 均線5%紀律\n狀態: {chip_status}"
    
    # 只要有訊號就通知，並告知籌碼狀態
    send_telegram_msg(title + body)
    
    print(f"📡 處理訊號: {ticker} | 結果: {is_pass}")
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    # 啟動在本機 Port 80
    app.run(host='0.0.0.0', port=80)