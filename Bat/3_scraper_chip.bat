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

rem Append log (under bat folder)
echo %date% %time% - Run [StartCenter] (scraper_chip) >> bat\Log2.txt

rem Ensure venv exists; create if missing
if not exist "venv\Scripts\activate.bat" (
    echo no venv, start to build venv...
    python -m venv venv
    echo venv done.
)

rem Start process: activate venv and run Python (no rebuild each time)
echo Start to mining data from FinMind...
call ".\venv\Scripts\activate.bat"

rem You can enable scraper or only run backtest here
rem python .\scraper_chip.py --stock_id 2330 --days 30 --debug_tv
python .\sub-py\backtest_signals_60d.py --stock_ids 2610 --days 60 --horizons 5,10,20

echo RB TradingCenter Started!

endlocal
