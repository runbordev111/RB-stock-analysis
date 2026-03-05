@echo off
cd /d C:\ngrok
if not exist "RB_DataMining" (
    echo 📥 New environment detected, cloning repo...
    git clone https://github.com/RTKmick/RB_DataMining.git
    cd RB_DataMining
) else (
    echo 📂 Existing repo detected, pulling latest...
    cd RB_DataMining
    git pull --rebase origin master
)
echo.
echo ✅ Synchronization complete!
git status
pause