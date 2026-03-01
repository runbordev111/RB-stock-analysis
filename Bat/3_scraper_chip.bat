@echo off
cd /d C:\ngrok\RB_DataMining

:: 寫入 Log
echo %date% %time% - Run [StartCentor] (scraper_chip) >> Log2.txt

:: 【核心修改】檢查 venv 資料夾是否存在，不存在才建立
if not exist "venv\Scripts\activate" (
    echo no venv, start to build venv...
    python -m venv venv
    echo venv done.
)

:: 啟動程序：直接啟動並執行 python，不需要每次都 build
echo start to mining data from FinMind...
call .\venv\Scripts\activate
python .\scraper_chip.py --stock_id 3105 --days 30 --debug_tv

echo 🚀 RB TradingCentor Started!