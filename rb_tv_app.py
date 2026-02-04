import os
import requests
import pandas as pd
from datetime import datetime
from flask import Flask, request, jsonify, render_template # 全部集中在這
from dotenv import load_dotenv
from FinMind.data import DataLoader # 移到最上方

# ======================================================
# 1. 初始化與金鑰
# ======================================================
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

app = Flask(__name__)
api = DataLoader() # 建議全域初始化一次即可，省流量
DATA_PATH = "./data"

# ======================================================
# 2. 助手函式
# ======================================================
def clean_numeric_columns(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '').replace('null', '0')
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def save_to_local_db(stock_id, df):
    if not os.path.exists(DATA_PATH): os.makedirs(DATA_PATH)
    file_path = os.path.join(DATA_PATH, f"{stock_id}_chip.csv")
    if not os.path.isfile(file_path):
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
    else:
        existing_df = pd.read_csv(file_path)
        if str(df['日期'].iloc[0]) not in existing_df['日期'].astype(str).values:
            df.to_csv(file_path, mode='a', header=False, index=False, encoding='utf-8-sig')

# ======================================================
# 3. 核心邏輯
# ======================================================
def check_stock_monk_rule(stock_id):
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
        
        file_path = os.path.join(DATA_PATH, f"{stock_id}_chip.csv")
        if os.path.exists(file_path):
            hist_df = pd.read_csv(file_path).tail(5)
            f_sum, t_sum = hist_df['外資買賣超股數'].sum(), hist_df['投信買賣超股數'].sum()
            if f_sum > 0 and t_sum > 0:
                return True, f"✅ 籌碼合格！(外資:{f_sum}, 投信:{t_sum})"
            return False, f"❌ 籌碼不合。(外資:{f_sum}, 投信:{t_sum})"
    except Exception as e:
        return False, f"⚠️ 籌碼查詢失敗: {str(e)}"
    return False, "⚠️ 查無今日籌碼"

# ======================================================
# 4. 路由設定
# ======================================================
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    if not data: return jsonify({"status": "error"}), 400
    ticker = data.get('ticker', '未知股票').replace('TWSE:', '')
    price = data.get('price', '0')
    is_pass, chip_status = check_stock_monk_rule(ticker)
    
    # 發送 Telegram
    msg = f"🔔 RB 訊號觸發: {ticker}\n現價: {price}\n策略: 均線5%紀律\n狀態: {chip_status}"
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg})
    
    return jsonify({"status": "success"}), 200

@app.route('/dashboard/') # 建議加斜線，相容性更好
def dashboard():
    stock_id = "6239"
    boss_path = f"./data/{stock_id}_boss_list.csv"
    
    if os.path.exists(boss_path):
        boss_ids = pd.read_csv(boss_path)['broker_id'].astype(str).tolist()
        today_str = datetime.now().strftime("%Y-%m-%d")
        daily_df = api.taiwan_stock_trading_daily_report(stock_id=stock_id, start_date=today_str)
        
        if not daily_df.empty:
            daily_df['broker_id'] = daily_df['broker_id'].astype(str)
            today_boss = daily_df[daily_df['broker_id'].isin(boss_ids)].copy()
            today_boss['net'] = today_boss['buy'] - today_boss['sell']
            
            if not today_boss.empty:
                top_one = today_boss.sort_values(by='net', ascending=False).iloc[0]
                status_msg = f"✅ 關鍵分點『{top_one['broker_name']}』今日大買 {int(top_one['net'])} 張！"
                trend_msg = "🔥 大戶正在同步建倉"
            else:
                status_msg = "⚪ 今日大戶名單無顯著動作"
                trend_msg = "觀察中..."
        else:
            status_msg = "⏳ 今日數據尚未更新 (請於盤後 21:30 查詢)"
            trend_msg = "等待數據中..."
    else:
        status_msg = "⚠️ 尚未執行尋根計畫"
        trend_msg = "請執行 scraper_chip.py 生成名單"

    return render_template('dashboard.html', stocks=[{
        "id": stock_id, "name": "力成", "status": status_msg, "trend": trend_msg,
        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M")
    }])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)