For RUMBOR Data Mining

---

核心概念：**用 FinMind/TWSE 原始資料 → 籌碼/技術/Geo 指標 → 策略分數 + 儀表板**

---

### 快速開始：在 Dashboard 顯示數據（不需 ngrok / 本機 Flask）

若**只要在網頁上看籌碼數據**、不需要即時對外或 TradingView webhook，建議用 **GitHub Pages 靜態版**：

1. **本機產生/更新資料**  
   ```bash
   python scraper_chip.py --stock_id 6239
   ```  
   會輸出 `data/6239_whale_track.json`，並自動更新 `data/manifest.json`。
2. **若已有 `data/*_whale_track.json` 但沒有 manifest**：  
   ```bash
   python SubPY/build_manifest.py
   ```
3. **Push 到 GitHub**（含 `data/`、`index.html`）。
4. **開啟 GitHub Pages**  
   倉庫 → **Settings** → **Pages** → **Source** 選「Deploy from a branch」→ Branch 選 `main`（或你的預設分支）→ Folder 選 **/ (root)** → Save。
5. **開啟 Dashboard**  
   網址：`https://<你的 GitHub 帳號>.github.io/RB_DataMining/`  
   之後只要 push 更新 `data/`，重新整理頁面即可看到最新數據。

**不需要**執行 `ngrok http ...` 或 `python rb_tv_app.py`。  
若需要即時對外網址或 TradingView webhook，再使用 `bat/1_ngrok_http.bat` 與 `bat/2_start_flask_rb.bat`。

---

### 一、分層架構總覽

- **資料抓取與分析層**
  - `scraper_chip.py`：從 FinMind 抓分點 & 價格，呼叫 `core.pipeline.analyze_whale_trajectory`，輸出：
    - `./data/{stock_id}_whale_track.json`（dashboard 使用）
    - `./data/{stock_id}_boss_list.csv`（Top20 大戶彙總表）
  - `core/services/`：流程與服務
    - `core/pipeline.py`：主流程 orchestrator，串接各種 signals 模組與 I/O。
    - `core/services/adapter_tw.py`：台股專用 adapter，封裝 FinMindClient 的操作（交易日、日 K、分點日報等）。
  - `core/signals/`：所有純「訊號/指標」計算
    - `whale.py`：主力集中度/廣度/Top15/外本淨額/主力趨勢分數等。
    - `whale_extras.py`：Turning Points + Whale Radar。
    - `enhanced.py`：coherence / cost zone / streak_strength 等進階統計。
    - `tv.py`：TradingView 風格技術指標與 `tv_score/tv_grade`。
    - `regime.py`：長期 regime 分類（多頭/空頭/盤整/轉折）。
    - `validation.py` / `risk.py`：價格突破驗證、ATR% / 成交值 / 失效條件。
    - `geo.py`：Geo TopN + baseline + zscore + grade/tag。
    - `distribution.py`：HHI / Entropy + 派發風險（Distribution risk）。
    - `monitor.py`：Whale Trend Monitor 五態（ACCUMULATION / MARKUP / FADING / DISTRIBUTION / NEUTRAL）。
  - `core/io/`：外部資料來源與檔案 I/O
    - `finmind_client.py`：FinMind v4 API client（含 retry/log）。
    - `price_data.py`：抓 OHLCV（近 20 日/長期日 K）。
    - `broker_master.py`：讀券商主檔（xlsx/csv），輸出 `broker_id -> meta`。
  - 其它核心：
    - `core/aggregate.py`：主力分數 + regime 分數 + geo_adjust → `final_score/final_grade`。
    - `core/geo_utils.py`：HQ–券商距離與 TopN geo 計算。
    - `core/types.py` / `core/config.py`：TypedDict + 各種 config（PipelineConfig 等）。

- **Web 與視覺化層**
  - **靜態 Dashboard（推薦，不需本機伺服器）**  
    - `index.html`：單頁靜態儀表板，直接讀取 `data/*_whale_track.json` 與 `data/manifest.json`。  
    - 部署方式：將專案 push 到 GitHub，開啟 **GitHub Pages**（Settings → Pages → Source: 分支根目錄），即可在 `https://<username>.github.io/RB_DataMining/` 查看。**不需 ngrok、不需執行 Flask**，只要在本地跑 scraper、push 更新，重新整理網頁即可看到最新數據。
  - `rb_tv_app.py`（Flask，可選）：
    - `/dashboard/`：讀取 `*_whale_track.json` + 券商 master，完整版儀表板。
    - `/webhook`：串 TradingView 訊號 + TWSE T86，Telegram 推播。
  - `templates/dashboard.html`：Flask 版 Bootstrap + Chart.js 儀表板。

- **資料與輔助腳本層**
  - `rawdata/`：公司清單、券商 master、geocode cache 等來源資料。
  - `data/`：pipeline 輸出（JSON/CSV）；`data/manifest.json` 由 scraper 自動更新，供靜態 dashboard 股票清單使用。
  - `SubPY/`：更新券商主檔、geocode、`build_manifest.py`（可從現有 `data/*_whale_track.json` 產生 manifest）等。
  - `bat/`：scraper、下載專案；可選：ngrok、Flask（僅在需要即時對外或 webhook 時使用）。


### 建議流程（摘要）

- **只顯示數據**：見上方「快速開始」→ 本機跑 scraper → push → 開 GitHub Pages → 用 `index.html` 靜態 dashboard 看數據。
- **需要即時對外或 webhook**：再使用 `bat/1_ngrok_http.bat`、`bat/2_start_flask_rb.bat`。

---

python .\SubPY\backtest_signals_60d.py --stock_ids 2317 --days 60 --horizons 5,10,20