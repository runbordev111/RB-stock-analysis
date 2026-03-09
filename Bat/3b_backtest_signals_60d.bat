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

rem 先切到專案根目錄，再用 venv 的 Python 跑 backtest
if not exist "venv\Scripts\activate.bat" (
    echo 建立 venv...
    py -m venv venv 2>nul
    if errorlevel 1 python -m venv venv 2>nul
    if errorlevel 1 (
        echo 請先安裝 Python 或使用 py 指令。
        pause
        exit /b 1
    )
)
call ".\venv\Scripts\activate.bat"

rem 若 venv 裡沒有 pandas，自動安裝依賴（首次或 venv 重建後）
python -c "import pandas" 2>nul
if errorlevel 1 (
    echo 正在 venv 安裝依賴...
    python -m pip install -r requirements.txt -q
)

rem 可改 --stock_ids 與 --days（目前預設 3105 一檔、60 日）
python .\Sub-py\backtest_signals_60d.py --stock_ids 3105 --days 60 --horizons 5,10,20

echo.
pause

endlocal
