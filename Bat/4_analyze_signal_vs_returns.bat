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

echo Phase 1: Signal vs 未來報酬 分析...

rem 使用專案的 venv（與 2/3/3b 批次檔一致）
if not exist "venv\Scripts\activate.bat" (
    echo 建立 venv...
    python -m venv venv
)

call ".\venv\Scripts\activate.bat"
if errorlevel 1 (
    echo 無法啟用 venv。
    pause
    exit /b 1
)

python .\sub-py\analyze_signal_vs_returns.py

echo.
echo 請開啟 data\signal_vs_returns_report.html 檢視報告。
pause

endlocal
