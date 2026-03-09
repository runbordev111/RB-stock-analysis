@echo off
setlocal

rem Base folders
set "ROOT=C:\ngrok"
set "REPO=%ROOT%\RB-stock-analysis"

rem Check ROOT folder
if not exist "%ROOT%" (
    echo ROOT folder "%ROOT%" not found.
    pause
    exit /b 1
)

cd /d "%ROOT%"
if errorlevel 1 (
    echo Cannot change directory to "%ROOT%".
    pause
    exit /b 1
)

rem Clone repo if missing
if not exist "%REPO%" (
    echo Cloning repo into "%REPO%"...
    git clone https://github.com/runbordev111/RB-stock-analysis.git "%REPO%"
    if errorlevel 1 (
        echo Clone failed. Please check network or permissions.
        pause
        exit /b 1
    )
)

cd /d "%REPO%"
if errorlevel 1 (
    echo Cannot change directory to "%REPO%".
    pause
    exit /b 1
)

rem Ensure msung-data-mining branch exists and is up to date
git fetch origin

git rev-parse --verify msung-data-mining >nul 2>&1
if errorlevel 1 (
    echo Creating local branch msung-data-mining from origin/msung-data-mining...
    git checkout -b msung-data-mining origin/msung-data-mining
) else (
    git checkout msung-data-mining
)

rem Use --autostash so local unstaged changes won't block rebase
git pull --rebase --autostash origin msung-data-mining

echo.
echo Synchronization for msung-data-mining complete.
git status
pause

endlocal