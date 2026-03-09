@echo off
set "ROOT=C:\ngrok"
set "REPO=%ROOT%\RB_stock_analysis"

cd /d "%ROOT%"
if errorlevel 1 (
    echo ❌ Cannot change to %ROOT% - check path exists and permissions.
    pause
    exit /b 1
)

if not exist "%REPO%" (
    echo 📥 New environment detected, cloning repo...
    git clone https://github.com/runbordev111/RB-stock-analysis.git "%REPO%"
    if errorlevel 1 (
        echo ❌ Clone 失敗，請檢查網路或權限。
        pause
        exit /b 1
    )
    cd /d "%REPO%"

    echo 🔀 Switching to feature/data-mining branch...
    git fetch origin
    git checkout -b feature/data-mining origin/feature/data-mining
) else (
    echo 📂 Existing repo detected, pulling latest for feature/data-mining...
    cd /d "%REPO%"
    git checkout feature/data-mining
    git pull --rebase origin feature/data-mining
)

echo.
echo ✅ Synchronization for feature/data-mining complete!
git status
pause