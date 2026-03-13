@echo off
setlocal

rem Project root (auto-detect：本檔已在專案根目錄)
set "ROOT=%~dp0"
for %%I in ("%ROOT%") do set "ROOT=%%~fI"

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
    python -m venv venv
    if errorlevel 1 (
        echo Failed to create venv. Please check Python installation.
        pause
        exit /b 1
    )
)

call ".\venv\Scripts\activate.bat"
if errorlevel 1 (
    echo Failed to activate venv.
    pause
    exit /b 1
)

echo ================================
echo One-key pipeline for stock(s)
echo ================================
set "STOCK_IDS="
set /p STOCK_IDS=Enter stock id(s) (e.g. 3105 or 2330 2603) ^> 
if "%STOCK_IDS%"=="" (
    set "STOCK_IDS=3105"
)

set "DAYS="
set /p DAYS=Enter lookback trading days (default 60) ^> 
if "%DAYS%"=="" (
    set "DAYS=60"
)

echo.
echo [1/6] Running scraper_chip.py for %STOCK_IDS% (days=%DAYS%) ...
for %%S in (%STOCK_IDS%) do (
    echo --- Running scraper_chip.py for %%S ---
    python scraper_chip.py --stock_id %%S --days %DAYS%
    if errorlevel 1 (
        echo scraper_chip.py failed for %%S. Please check the error above.
        pause
        exit /b 1
    )
)

echo.
echo [2/6] Rebuilding data\manifest.json ...
python sub-py\build_manifest.py

echo.
echo [3/6] Running backtest_signals_60d.py ...
python sub-py\backtest_signals_60d.py --stock_ids %STOCK_IDS% --days %DAYS% --horizons 5,10,20
if errorlevel 1 (
    echo backtest_signals_60d.py failed. Please check the error above.
    pause
    exit /b 1
)

echo.
echo [4/6] Running Phase 1 analysis (analyze_signal_vs_returns.py) ...
python sub-py\analyze_signal_vs_returns.py
if errorlevel 1 (
    echo analyze_signal_vs_returns.py failed. Please check the error above.
    pause
    exit /b 1
)

echo.
echo [5/6] Running Phase 3 ML winrate (ml_signal_winrate.py, horizons 5,10,20) ...
python sub-py\ml_signal_winrate.py --horizons 5,10,20
if errorlevel 1 (
    echo ml_signal_winrate.py failed. Install scikit-learn and joblib: pip install scikit-learn joblib
    pause
    exit /b 1
)

echo.
echo [6/7] Smoke check outputs (smoke_check_artifacts.py) ...
python sub-py\smoke_check_artifacts.py
if errorlevel 1 (
    echo Smoke check failed. Please fix issues above before upload.
    pause
    exit /b 1
)

echo.
echo [7/7] Optional: upload changes to GitHub (upl_rb.bat)
choice /M "Run upl_rb.bat to commit/push now?"
if errorlevel 2 (
    echo Skipping upload. You can run upl_rb.bat later.
) else (
    call upl_rb.bat
)

echo.
echo All steps finished.
pause

endlocal

