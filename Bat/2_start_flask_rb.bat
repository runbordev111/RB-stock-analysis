@echo off
cd /d C:\ngrok\RB_DataMining

:: 寫入 Log
echo %date% %time% - Run [StartCentor] (start_flask_rb) >> Log.txt

:: 【核心修改】檢查 venv 存在才啟動，不存在才建立
if not exist "venv\Scripts\activate" (
    echo no venv, start to build venv...
    python -m venv venv
    echo venv done.
)

:: 啟動程序
echo start Flask Web Server...
call .\venv\Scripts\activate
python rb_tv_app.py

echo 🚀 RB TradingCentor Flask Started!