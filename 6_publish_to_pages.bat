@echo off
setlocal

rem === 專案根目錄（本檔放在 RB-stock-analysis 根目錄） ===
set "ROOT=%~dp0"
for %%I in ("%ROOT%") do set "ROOT=%%~fI"

cd /d "%ROOT%"
if errorlevel 1 (
    echo [錯誤] 無法切換到專案目錄 "%ROOT%".
    pause
    exit /b 1
)

echo.
echo ==========================================
echo  Publish to GitHub Pages
echo  將 msung-data-mining 推到遠端 main（不切換本地分支）
echo ==========================================
echo.

rem === 確認目前在 msung-data-mining 分支上 ===
for /f "tokens=*" %%i in ('git rev-parse --abbrev-ref HEAD 2^>nul') do set "BRANCH=%%i"

if "%BRANCH%"=="" (
    echo [錯誤] 無法取得目前 git 分支，請確認這裡是 git 儲存庫。
    pause
    exit /b 1
)

echo 目前所在分支：%BRANCH%

if /I not "%BRANCH%"=="msung-data-mining" (
    echo.
    echo [錯誤] 目前分支不是 msung-data-mining。
    echo 請先執行：git checkout msung-data-mining
    echo 然後再重新執行本批次檔。
    echo.
    pause
    exit /b 1
)

rem === 將 msung-data-mining 自己先 push 上去（確保遠端同步） ===
echo.
echo [步驟 1] 推送 msung-data-mining 到遠端同名分支...
git push origin msung-data-mining
if errorlevel 1 (
    echo.
    echo [錯誤] Push msung-data-mining 失敗，請先解決衝突或驗證錯誤後再試。
    pause
    exit /b 1
)

rem === 直接更新遠端 main：用 msung-data-mining 覆蓋遠端 main ===
echo.
echo [步驟 2] 將 msung-data-mining 推送為遠端 main（origin/main）...
git push origin msung-data-mining:main
if errorlevel 1 (
    echo.
    echo [錯誤] Push 到 origin/main 失敗，可能是遠端 main 有直接修改。
    echo 請到 GitHub 上檢查 main 分支狀態，或改用手動 PR/合併。
    pause
    exit /b 1
)

echo.
echo ✅ Publish 完成！
echo GitHub Pages 會在幾分鐘內自動重新部署。
echo 頁面網址：
echo   https://runbordev111.github.io/RB-stock-analysis/
echo.
pause

endlocal
