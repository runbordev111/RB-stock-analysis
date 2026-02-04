import shioaji as sj
import os
from dotenv import load_dotenv

# 讀取 .env
load_dotenv()

api = sj.Shioaji()

# 嘗試登入
try:
    api.login(
        api_key=os.getenv("SHIOAJI_API_KEY"),
        secret_key=os.getenv("SHIOAJI_SECRET_KEY")
    )
    # 載入憑證
    api.activate_ca(
        ca_path=os.getenv("SHIOAJI_CERT_PATH"),
        ca_passwd=os.getenv("SHIOAJI_CERT_PASSWORD"),
        person_id=os.getenv("SHIOAJI_PERSON_ID")
    )
    print("✅ 潤鉑系統報告：永豐金 API 登入並驗證成功！")
except Exception as e:
    print(f"❌ 登入失敗，錯誤原因：{e}")

api.logout()
