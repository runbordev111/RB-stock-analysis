@echo off
setlocal

REM Optional: only needed when running local Flask dashboard or /webhook.
REM For static dashboard, just open index.html via GitHub Pages.

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

rem Append log (under ztemp folder)
echo %date% %time% - Run [StartCenter] (start_flask_rb) >> ztemp\Log.txt

rem Ensure venv exists; create if missing
if not exist "venv\Scripts\activate.bat" (
    echo no venv, start to build venv...
    python -m venv venv
    echo venv done.
)

rem Start Flask web server
echo Starting Flask Web Server...
call ".\venv\Scripts\activate.bat"
python rb_tv_app.py

echo RB TradingCenter Flask Started!

endlocal