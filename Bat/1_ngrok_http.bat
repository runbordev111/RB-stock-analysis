@echo off
REM 可選：僅在需要「即時對外網址」或 TradingView webhook 時使用。
REM 若只要在 GitHub 上更新數據並用 GitHub Pages 看 dashboard，不需執行本檔與 2_start_flask_rb.bat。
cd c:\ngrok

.\ngrok http --domain=medicably-aeromechanical-yadiel.ngrok-free.dev 80

echo 🚀 ngrok started！