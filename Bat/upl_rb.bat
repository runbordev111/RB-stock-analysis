@echo off
cd /d C:\ngrok\RB_DataMining

echo 🚀 Preparing to upload changes...

:: 寫入 Log
echo %date% %time% - Run [Backup Upload] (upl_rb) >> Log.txt

:: 先 add / commit，再 pull，最後 push（遠端與本機皆為 main）
set REMOTE_BRANCH=main
git add .
git status
set BRANCH=
for /f "tokens=*" %%i in ('git rev-parse --abbrev-ref HEAD 2^>nul') do set BRANCH=%%i
if "%BRANCH%"=="" set BRANCH=main

git commit -m "V1.0.6 - %date% %time% Update (including logs)" 2>nul
if errorlevel 1 (
  echo ⚠️ 無變更可 commit，或已是最新。
) else (
  echo ✅ 已 commit。
)

git pull --rebase origin %REMOTE_BRANCH%
if errorlevel 1 (
  echo ⚠️ pull 失敗，請手動處理衝突後再執行一次。
  pause
  exit /b 1
)

:: 推送到 origin main
git push origin %BRANCH%:%REMOTE_BRANCH%
if errorlevel 1 (
  echo ❌ push 失敗，請確認遠端有 %REMOTE_BRANCH% 分支。
  pause
  exit /b 1
)

echo ✅ Upload and log update complete!
pause