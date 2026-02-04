VS Code
Ctrl + Shift + P >>輸入並選擇：Python: Select Interpreter 選擇路徑中有 ('venv': venv) 的那一個。
Ctrl + ~ >>(數字 1 左邊那個鍵) 開啟終端機 >> git pull origin master
//--------------------------------------------------------------------------------------
start cmd /k "cd C:\ngrok\ && ngrok http --domain=medicably-aeromechanical-yadiel.ngrok-free.dev 80"
start cmd /k "cd C:\ngrok\RB_DataMining && .\venv\Scripts\activate && python rb_tv_app.py"

.\ngrok http --domain=medicably-aeromechanical-yadiel.ngrok-free.dev 80
python rb_tv_app.py
(ngrok Authtoken)

//---------------------------------------------
(右邊視窗)	Python 程式 (rb_tv_app.py)	要venv！	因為程式需要用到您裝在裡面的 pandas、flask 等套件。
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
python -m venv venv
.\venv\Scripts\activate
pip install FinMind
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

