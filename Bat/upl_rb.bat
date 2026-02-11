@echo off
cd /d C:\ngrok\RB_DataMining

echo 🚀 Preparing to upload changes...

:: 寫入 Log
echo %date% %time% - Run [Backup Upload] (upl_rb) >> Log.txt

:: 全部上傳（含 Log.txt）
git pull --rebase origin master
git add .
git commit -m "V1.0.6 - %date% %time% Update (including logs)"
git push origin master

echo ✅ Upload and log update complete!
pause