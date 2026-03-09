@echo off
setlocal

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
echo %date% %time% - Run [StartCenter] (scraper_chip) >> Bat\Log2.txt

rem 檢查 venv 資料夾是否存在，不存在才建立
if not exist "venv\Scripts\activate.bat" (
    echo no venv, start to build venv...
    python -m venv venv
    echo venv done.
)

rem 啟動程序：直接啟動並執行 python，不需要每次都 build
echo start to mining data from FinMind...
call ".\venv\Scripts\activate.bat"

rem 這裡你可以依需要啟動 scraper 或只跑 backtest
rem python .\scraper_chip.py --stock_id 2330 --days 30 --debug_tv
python .\sub-py\backtest_signals_60d.py --stock_ids 2610 --days 60 --horizons 5,10,20

echo 🚀 RB TradingCenter Started!

endlocal
