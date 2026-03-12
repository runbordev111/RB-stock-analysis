@echo off
setlocal

rem Project root (auto-detect)
set "ROOT=%~dp0.."
for %%I in ("%ROOT%") do set "ROOT=%%~fI"

if not exist "%ROOT%" (
    echo Project root "%ROOT%" not found.
    pause
    exit /b 1
)

cd /d "%ROOT%"
if errorlevel 1 (
    echo Failed to change directory to "%ROOT%".
    pause
    exit /b 1
)

echo Publish to GitHub Pages (merge msung-data-mining into main)...

rem Append log
echo %date% %time% - Run [Publish to Pages] (6_publish_to_pages) >> bat\Log.txt

rem Ensure msung-data-mining is pushed first
git push origin msung-data-mining 2>nul
if errorlevel 1 (
    echo Warning: push msung-data-mining failed or already up to date.
)

rem Switch to main and update
git checkout main
if errorlevel 1 (
    echo Failed to checkout main.
    pause
    exit /b 1
)

git pull origin main
if errorlevel 1 (
    echo Pull main failed. Please resolve and retry.
    pause
    exit /b 1
)

rem Merge msung-data-mining into main
git merge msung-data-mining -m "Merge msung-data-mining for GitHub Pages"
if errorlevel 1 (
    echo Merge failed. Please resolve conflicts manually.
    pause
    exit /b 1
)

rem Push main to update GitHub Pages
git push origin main
if errorlevel 1 (
    echo Push main failed.
    pause
    exit /b 1
)

rem Switch back to msung-data-mining for daily work
git checkout msung-data-mining

echo.
echo Publish complete. GitHub Pages will update in a few minutes.
echo Refresh: https://runbordev111.github.io/RB-stock-analysis/index.html
pause

endlocal
