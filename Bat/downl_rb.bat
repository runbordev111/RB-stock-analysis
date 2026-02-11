echo ✅ Synchronization and log update complete!
pause

@echo off
:: 1. 先進入根目錄
cd /d C:\ngrok

:: 2. 檢查資料夾是否存在
if not exist "RB_DataMining" (
    echo 📥 Installed detected, syncing in progress....
    git clone https://github.com/RTKmick/RB_DataMining.git
) else (
    echo 📂 New environment detected, downloading......
    cd RB_DataMining
    :: 直接用 pull 而不是 clone，這樣就不會產生兩層資料夾
    git pull origin master
)

echo ✅ 處理完成！
pause