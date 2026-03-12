@echo off
setlocal

rem Project root (auto-detect)
set "ROOT=%~dp0.."
for %%I in ("%ROOT%") do set "ROOT=%%~fI"

if not exist "%ROOT%" (
    echo Project root "%ROOT%" not found.
    exit /b 1
)

cd /d "%ROOT%"
if errorlevel 1 (
    echo Failed to change directory to "%ROOT%".
    exit /b 1
)

echo Phase 3: ML winrate estimation (from backtest_signals_60d.csv)...

rem Use project venv
if not exist "venv\Scripts\activate.bat" (
    echo Creating venv...
    python -m venv venv
)
call ".\venv\Scripts\activate.bat"
if errorlevel 1 (
    echo Failed to activate venv.
    exit /b 1
)

rem Optional: install Phase 3 deps if missing
pip install scikit-learn joblib --quiet 2>nul

python sub-py\ml_signal_winrate.py --horizons 5,10,20
if errorlevel 1 (
    echo ml_signal_winrate.py failed. Ensure data\backtest_signals_60d.csv exists. Run 3b or 5 first.
    exit /b 1
)

echo.
echo Output: data\models\*.pkl, data\ml_feature_importance_ret*.csv, data\ml_winrate_report_ret*.html

endlocal
