@echo off
setlocal

rem Project root (auto-detect)
set "ROOT=%~dp0.."
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

echo Phase 1: Signal vs future returns analysis...

rem Use project venv (same as 2/3/3b batch files)
if not exist "venv\Scripts\activate.bat" (
    echo Creating venv...
    python -m venv venv
)

call ".\venv\Scripts\activate.bat"
if errorlevel 1 (
    echo Failed to activate venv.
    pause
    exit /b 1
)

python .\sub-py\analyze_signal_vs_returns.py

echo.
echo Open data\signal_vs_returns_report.html to view report.
pause

endlocal
