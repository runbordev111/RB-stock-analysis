from FinMind.data import DataLoader
import pandas as pd
import os

# 1. 初始化
api = DataLoader()
# 如果您有 Token，請解除下面這行的註解並填入：
# api.login_by_token(api_token="您的TOKEN")

stock_id = "6239"
print(f"🚀 正在強力抓取 {stock_id} 的大戶名單...")

try:
    # 2. 抓取分點資料 (回溯 180 天)
    df = api.taiwan_stock_trading_daily_report(
        stock_id=stock_id,
        start_date='2025-08-01'
    )

    if df.empty:
        print("❌ 錯誤：FinMind 回傳空資料！可能是流量限制，請一小時後再試，或檢查網路。")
    else:
        # 3. 計算淨買超並取前 20 名
        df['net_buy'] = df['buy'] - df['sell']
        boss_list = df.groupby(['broker_id', 'broker_name'])['net_buy'].sum().reset_index()
        top_20 = boss_list.sort_values(by='net_buy', ascending=False).head(20)

        # 4. 強制寫入檔案
        data_path = "./data"
        if not os.path.exists(data_path): os.makedirs(data_path)
        
        file_path = os.path.join(data_path, f"{stock_id}_boss_list.csv")
        top_20.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print(f"✅ 成功！名單已存至: {file_path}")
        print("以下是您的前五大地頭蛇：")
        print(top_20.head(5))

except Exception as e:
    print(f"💥 發生意外錯誤: {e}")