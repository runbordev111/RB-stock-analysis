import os
import asyncio
from telegram import Bot
from dotenv import load_dotenv

# 加載 .env 檔案中的變數
load_dotenv()

async def send_test():
    # 這裡要從環境變數讀取，不要直接寫 Token 字串
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    bot = Bot(token=token)
    
    try:
        await bot.send_message(chat_id=chat_id, text="🚀 潤鉑雲端報告：Telegram 已連線成功！")
        print("✅ 測試訊息已送出，請檢查手機！")
    except Exception as e:
        print(f"❌ 傳送失敗，原因：{e}")

if __name__ == "__main__":
    asyncio.run(send_test())
