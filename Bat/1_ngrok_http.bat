@echo off
REM Optional: only needed for public tunnel URL or TradingView webhook.
REM If you only need GitHub Pages dashboard, you don't need this or 2_start_flask_rb.bat.
cd c:\ngrok

.\ngrok http --domain=medicably-aeromechanical-yadiel.ngrok-free.dev 80

echo ngrok started!