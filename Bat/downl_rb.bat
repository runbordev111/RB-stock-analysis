@echo off
set "ROOT=C:\ngrok"
set "REPO=%ROOT%\RB_DataMining"

cd /d "%ROOT%"
if errorlevel 1 (
    echo ❌ Cannot change to %ROOT% - check path exists and permissions.
    pause
    exit /b 1
)

if not exist "%REPO%" (
    echo 📥 New environment detected, cloning repo...
    git clone https://github.com/RTKmick/RB_DataMining.git "%REPO%"
    cd /d "%REPO%"
) else (
    echo 📂 Existing repo detected, pulling latest...
    cd /d "%REPO%"
    git pull --rebase origin master
)
echo.
echo ✅ Synchronization complete!
git status
pause