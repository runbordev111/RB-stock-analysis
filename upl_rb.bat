@echo off
setlocal

rem Project root (auto-detect：本檔已在專案根目錄)
set "ROOT=%~dp0"
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

echo Preparing to upload changes to msung-data-mining...

rem Append log (stored under ztemp\Log.txt)
if not exist "%ROOT%\ztemp" mkdir "%ROOT%\ztemp"
echo %date% %time% - Run [Backup Upload] (upl_rb) >> "%ROOT%\ztemp\Log.txt"

rem Default local/remote branch
set "REMOTE_BRANCH=msung-data-mining"

rem Stage current changes (respecting .gitignore)
git add .
git status

set "BRANCH="
for /f "tokens=*" %%i in ('git rev-parse --abbrev-ref HEAD 2^>nul') do set BRANCH=%%i
if "%BRANCH%"=="" set BRANCH=%REMOTE_BRANCH%

rem Create simple auto commit message (no fixed version number)
git commit -m "%date% %time% - auto upload (upl_rb)" 2>nul
if errorlevel 1 (
    echo No changes to commit, or already up to date.
) else (
    echo Commit created.
)

rem Rebase with remote and autostash local changes
git pull --rebase --autostash origin %REMOTE_BRANCH%
if errorlevel 1 (
    echo Pull failed, please resolve conflicts and retry.
    pause
    exit /b 1
)

rem Push to origin msung-data-mining
git push origin %BRANCH%:%REMOTE_BRANCH%
if errorlevel 1 (
    echo Push failed, please check remote branch %REMOTE_BRANCH%.
    pause
    exit /b 1
)

echo Upload and log update complete!
pause

endlocal