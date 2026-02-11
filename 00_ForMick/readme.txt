VS Code
Ctrl + Shift + P >>輸入並選擇：Python: Select Interpreter 選擇路徑中有 ('venv': venv) 的那一個。
Ctrl + ~ >>(數字 1 左邊那個鍵) 開啟終端機 >> git pull origin master
//--------------------------------------------------------------------------------------
start cmd /k "cd C:\ngrok\ && ngrok http --domain=medicably-aeromechanical-yadiel.ngrok-free.dev 80"
start cmd /k "cd C:\ngrok\RB_DataMining && .\venv\Scripts\activate && python rb_tv_app.py"

.\ngrok http --domain=medicably-aeromechanical-yadiel.ngrok-free.dev 80
python rb_tv_app.py  (ngrok Authtoken)

//---------------------------------------------
(右邊2視窗)	Python 程式 (rb_tv_app.py)	要venv！	因為程式需要用到您裝在裡面的 pandas、flask 等套件。
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
python -m venv venv
.\venv\Scripts\activate
pip install FinMind
pip install tqdm
pip install tqdm pandas flask requests FinMind python-dotenv
>>>python rb_tv_app.py

//------------
(左邊視窗)	ngrok 工具 (ngrok.exe) 它跟 Python 無關。
git pull origin master
cd c:\ngrok
>>.\ngrok config add-authtoken 2JU9XuyzEviEi5agopY8srTvXNp_5Em8z3aqiDk9txMpEpR4W
>>.\ngrok http --domain=medicably-aeromechanical-yadiel.ngrok-free.dev 80


(永久門牌)
https://medicably-aeromechanical-yadiel.ngrok-free.dev/webhook

git config --global user.name "RTKmick"
git config --global user.email "mick.sung01@gmail.com"


//===========================================================================

# --- 永豐金證券 Shioaji API 設定 ---
SHIOAJI_API_KEY="14WkzEdjiPEPzaS6LF9Hwa1aFDKBNsPN68A3HLxBF7d"
SHIOAJI_SECRET_KEY="79VwzxE3rrXpD6UzqydMvtCThfUvN4cNbZqeonoxVNXu"
SHIOAJI_CERT_PATH="./Sinopac.pfx"
SHIOAJI_CERT_PASSWORD="B121565115"
SHIOAJI_PERSON_ID="B121565115"

# --- Telegram Bot 設定 ---
TELEGRAM_BOT_TOKEN="8193955449:AAG50J-zOIdy-8R_6Totsa2Fh_THf6E1qWI"
TELEGRAM_CHAT_ID="-4917957878"

# --- LINE Notify / Bot ES_LBot_ID 設定 --- 
LINE_USER_ID="Ucd148c769b3342b47244c6b0013fcb0c"
LINE_CHANNEL_ACCESS_TOKEN="41y5oArc5Rb+tcBFeWjq3nvSFWQmorqLu8QPt5v3Nx38tyl6SnMp1kVfx8j2swXxMALcz+gJ7WQvhgL8HL7>
LINE_CHANNEL_SECRET="eba6cfb3df016211a02d46b3ca41b654"

# --- 安全驗證 ---
WEBHOOK_PASSPHRASE="runbor111"

//===========================================================================
#ngrok Authtokens
.\ngrok.exe config add-authtoken 2JU9XuyzEviEi5agopY8srTvXNp_5Em8z3aqiDk9txMpEpR4W

#ngrok 安裝 （或重新開啟 CMD）手動輸入
cd C:\ngrok\RB_DataMining
python -m venv venv
.\venv\Scripts\activate
pip install flask requests shioaji
python -m pip install --upgrade pip
pip install flask requests shioaji python-dotenv

//===========================================================================
#ngrok git pull to GitHub (本機上傳到GitHub)
C:\ngrok\RB_DataMining>
git add .
git commit -m "V1.0.0"
git push origin master
//===========================================================================//
#GitHub git push to ngrok (本機下載到ngrok)
cd C:\ngrok
git clone https://github.com/RTKmick/RB_DataMining.git

cd C:\ngrok\RB_DataMining
git pull origin master
//===========================================================================//
# 建立並啟動環境
python -m venv venv
.\venv\Scripts\activate

# 一次裝好所有裝備
pip install --upgrade pip
pip install flask requests shioaji python-dotenv python-telegram-bot pandas
//============================================================================
# ======================================================
# 3. 接收 TradingView 訊號的路徑
#=======================================================
# Version 1.0.0
#"ticker": "{{ticker}}",    // TradingView 會自動填入股票代號 (例如 2330)
#"price": "{{close}}",      // 會填入觸發當時的收盤價
#"support": "1000",         // 這是您手動填入的支撐位 (目前暫代 200MA)
#"action": "buy"            // 告訴您的程式這是一筆「買入」訊號

//=================================================================================
FinMind
mick.sung01@gmail.com
mickplus1


地緣券商（新竹分點）：力成總部在新竹，若發現如「新竹、竹北」區域的券商在 1/28-2/3 連續買超，這通常代表公司內部人或區域大戶的動作，具有極高參考價值。
外資關鍵庫存券商：觀察「摩根大通」、「美林」或「高盛」這類分點。如果你的法人數據顯示外資在買，但分點數據顯示是這幾家主要外資券商在「連續慣性買進」，則趨勢較穩。
隔日沖分點：如果在某天大買後，隔 1-2 天就全數出脫（如凱基-台北、富邦-建國），則要小心籌碼不穩。
//======================================
//TradingView
//FinMind
//VS Code
//SQLite https://sqlitebrowser.org/dl/
//TradingView → Python → SQLite → AI → 永豐 API → Dashboard

第一個資料庫 tw_stock.db
第一張表 chip_inst
Python 寫入程式（CSV → SQLite）
SQL 查詢模板（外資連買、大戶連買等）

即便技術面突破了，籌碼面不夠硬就不給過
內部大戶（大股東/董監事）：這類籌碼通常看「長線」，因為大股東不會天天買賣。>>「長線」(X)
法人大戶（三大法人）：外資、投信、自營商。這類籌碼看「波段」，股價發動的推手。「短線」
主力大戶（分點大戶）：隱藏在證券分點（例如：凱基-台北、摩根大通）後面的神祕資金。>>「短線」

操作方式：收盤後（建議在 21:30 之後，等數據完全處理完畢），腳本自動去抓這 10 支股的分點買賣紀錄。
>>監看大戶持有比例

時間,動作,目的
14:00 - 15:00,抓取「三大法人」初步數據,快速確認技術面突破是否伴隨法人買盤。
18:00 - 20:00,抓取「分點買賣明細」,確認是否有「隔日沖分點」或「關鍵分點」進場。
21:30 之後,執行「最終過濾」並存檔,這時數據最完整，適合存入您的 CSV 資料庫做長期記錄。

//=================================================================================
RB_DataMining/
  scraper_chip.py                # 只留 main + run_strategy（流程）
  core/
    __init__.py
core/finmind_client.py：    API I/O、retry、log、回傳 DataFrame
core/adapter_tw.py：        把 API dataset 抽象成 domain method（trading dates、name、daily report）
core/broker_master.py：     靜態資料載入與映射
core/price_data.py：        價格資料抽取 + 欄位 normalize（OHLCV schema 固化）
core/indicators_tv.py：     TV radar 指標（只吃 OHLCV DataFrame，不碰 API）
core/regime.py：            中期 regime（只吃 price_df，不碰 API）
core/signals_whale.py：     分點資料清洗、集中度、廣度、top15、master_trend（只吃 trading df，不碰 API）
core/pipeline.py：          組裝流程（整合 price/regime/tv/whale signals）
core/config.py :            集中所有門檻與權重
core/types.py :             把 signals/pack 的資料契約固定
core/aggregate.py :         把 aggregation 抽成
core/signals_validation.py  負責：輸入 price_df(ohlcv_20d) + signals(score等) → 輸出 validation dict（MVP 3 欄）
core/risk_rules.py          負責：輸入 price_df(ohlcv_20d) → 輸出 risk dict（MVP 3 欄）

你現在先做 MVP：每個檔案各一個 function，compute_validation_signals() / compute_risk_signals()。

scraper_chip.py：entrypoint / orchestration（args、run_strategy、輸出 JSON/CSV）


//=================================================================================