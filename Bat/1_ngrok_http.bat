@echo off
REM Optional: only needed for public tunnel URL or TradingView webhook.
REM If you only need GitHub Pages dashboard, you don't need this or 2_start_flask_rb.bat.
setlocal

rem Auto-detect ngrok directory: repo_root\bat\..\.. = parent folder of repo
set "NGROK_DIR=%~dp0..\.."
for %%I in ("%NGROK_DIR%") do set "NGROK_DIR=%%~fI"

"%NGROK_DIR%\ngrok.exe" http --domain=medicably-aeromechanical-yadiel.ngrok-free.dev 80

echo ngrok started!

endlocal