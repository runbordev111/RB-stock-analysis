@echo off
setlocal

rem Project root (same as Git repo)
set "ROOT=C:\ngrok\RB-stock-analysis"

if not exist "%ROOT%" (
    echo Project root "%ROOT%" not found.
    pause
    exit /b 1
)

cd /d "%ROOT%"
if errorlevel 1 (
    echo Failed to change directory to "%ROOT%".
    pause
    exit /b 1
)

rem Ensure venv exists; create if missing
if not exist "venv\Scripts\activate.bat" (
    echo Creating venv...
    py -m venv venv 2>nul
    if errorlevel 1 python -m venv venv 2>nul
    if errorlevel 1 (
        echo Please install Python or use the py launcher.
        pause
        exit /b 1
    )
)
call ".\venv\Scripts\activate.bat"

rem Auto-install dependencies in venv if pandas is missing
python -c "import pandas" 2>nul
if errorlevel 1 (
    echo Installing dependencies into venv...
    python -m pip install -r requirements.txt -q
)

rem You can change --stock_ids and --days (currently 3105, last 60 trading days)
python .\Sub-py\backtest_signals_60d.py --stock_ids 3105 --days 60 --horizons 5,10,20

echo.
pause

endlocal
