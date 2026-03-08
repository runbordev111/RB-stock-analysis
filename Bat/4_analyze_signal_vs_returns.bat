@echo off
cd /d C:\ngrok\RB_DataMining

echo Phase 1: Signal vs 未來報酬 分析...
if not exist "venv\Scripts\activate" (
    python -m venv venv
)
call .\venv\Scripts\activate
python .\SubPY\analyze_signal_vs_returns.py

echo.
echo 請開啟 data\signal_vs_returns_report.html 檢視報告。
pause
