@echo off
setlocal

REM 可選：僅在需要本機 Flask 儀表板或 /webhook 時使用。靜態版請用 GitHub Pages 開啟 index.html。

rem 專案根目錄（與 Git repo 一致）
set "ROOT=C:\ngrok\RB-stock-analysis"

if not exist "%ROOT%" (
    echo 找不到專案根目錄 "%ROOT%"。
    pause
    exit /b 1
)

cd /d "%ROOT%"
if errorlevel 1 (
    echo 無法切換到 "%ROOT%"。
    pause
    exit /b 1
)

rem 寫入 Log（放在 Bat 資料夾下）
echo %date% %time% - Run [StartCenter] (start_flask_rb) >> Bat\Log.txt

rem 檢查 venv 存在才啟動，不存在才建立
if not exist "venv\Scripts\activate.bat" (
    echo no venv, start to build venv...
    python -m venv venv
    echo venv done.
)

rem 啟動程序
echo start Flask Web Server...
call ".\venv\Scripts\activate.bat"
python rb_tv_app.py

echo 🚀 RB TradingCenter Flask Started!

endlocal