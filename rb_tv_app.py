import os
from flask import Flask, request, abort
import shioaji as sj
from telegram import Bot
import asyncio
from dotenv import load_dotenv

# 1. 初始化環境
load_dotenv()
app = Flask(__name__)
tg_bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
chat_id = os.getenv("TELEGRAM_CHAT_ID")

# 2. 永豐金 API 登入函數
def login_shioaji():
    api = sj.Shioaji()
    api.login(api_key=os.getenv("SHIOAJI_API_KEY"), secret_key=os.getenv("SHIOAJI_SECRET_KEY"))
    api.activate_ca(
        ca_path=os.getenv("SHIOAJI_CERT_PATH"),
        ca_passwd=os.getenv("SHIOAJI_CERT_PASSWORD"),
        person_id=os.getenv("SHIOAJI_PERSON_ID")
    )
    return api

# 3. 接收 TradingView 訊號的路徑=============================================
# Version 1.0.0
#"ticker": "{{ticker}}",    // TradingView 會自動填入股票代號 (例如 2330)
#"price": "{{close}}",      // 會填入觸發當時的收盤價
#"support": "1000",         // 這是您手動填入的支撐位 (目前暫代 200MA)
#"action": "buy"            // 告訴您的程式這是一筆「買入」訊號

@app.route('/webhook', methods=['POST'])
def webhook():
    # 1. 建議改用 JSON 解析，方便邏輯判斷
    data = request.get_json()
    if not data:
        return "Invalid JSON", 400

    ticker = data.get("ticker")
    price = float(data.get("price", 0))
    support_level = float(data.get("support", 0)) # 假設你從 TV 傳送 MA 數值
    
    # 2. 落實掃地僧 5% 紀律 [cite: 81]
    # 計算買入價離支撐位（如 200MA）的距離
    distance_pct = (price - support_level) / support_level if support_level > 0 else 999
    
    msg = f"🔔 訊號：{ticker} 價格 {price}\n"
    
    if distance_pct <= 0.05:
        msg += f"✅ 符合 5% 紀律 (距離: {distance_pct:.2%})\n🚀 執行下單邏輯..."
        # 這裡執行 api.place_order()
    else:
        msg += f"❌ 距離支撐過遠 ({distance_pct:.2%})，不予追高。"

    # 3. 發送 Telegram 通知
    asyncio.run(tg_bot.send_message(chat_id=chat_id, text=msg))
    print(msg)
    return "OK", 200
# ==========================================================================================
if __name__ == '__main__':
    # 僅供測試開發使用，正式生產環境會改用 gunicorn
    app.run(host='0.0.0.0', port=80)
