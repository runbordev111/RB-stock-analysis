For RUMBOR Data Mining

---

核心概念：**用 FinMind/TWSE 原始資料 → 籌碼/技術/Geo 指標 → 策略分數 + 儀表板**

---

### 快速開始：在 Dashboard 顯示數據（不需 ngrok / 本機 Flask）

若**只要在網頁上看籌碼數據**、不需要即時對外或 TradingView webhook，建議用 **GitHub Pages 靜態版**：

> 資料來自 GitHub 倉庫 `data/`，不需 ngrok 或本機 Flask。Push 更新後重新整理即可。

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
  - `sub-py/`：更新券商主檔、geocode、`build_manifest.py`、**`analyze_signal_vs_returns.py`（Phase 1）**、**`ml_signal_winrate.py`（Phase 3 ML 勝率）** 等。
  - `bat/`：scraper、下載專案、Phase 1 / Phase 3 分析；可選：ngrok、Flask。


### 建議流程（摘要）

- **只顯示數據**：見上方「快速開始」→ 本機跑 scraper → push → 開 GitHub Pages → 用 `index.html` 靜態 dashboard 看數據。
- **需要即時對外或 webhook**：再使用 `bat/1_ngrok_http.bat`、`bat/2_start_flask_rb.bat`。

### Phase 1：把 Signal vs 未來報酬看清楚

1. 先產生 backtest 樣本（需 FinMind token）：  
   `python sub-py/backtest_signals_60d.py --stock_ids 2338 --days 60 --horizons 5,10,20`
2. 再執行 Phase 1 分析：  
   `python sub-py/analyze_signal_vs_returns.py`  
   或雙擊 `bat/4_analyze_signal_vs_returns.bat`
3. 開啟 `data/signal_vs_returns_report.html` 檢視：Score 區間 / Monitor state 的未來 5/10/20 日報酬統計與分佈圖，據此調整策略門檻。

### Phase 3：ML 勝率估計（整合）

從 `data/backtest_signals_60d.csv` 用 RandomForest 估計「訊號 → 未來報酬 > 0」的勝率，並產出模型與特徵重要度供後續 pipeline 或報表使用。

1. **一鍵流程（含 Phase 3）**：雙擊 `bat/5_run_all_for_stock.bat`，輸入股票代號與 lookback 天數，跑完會自動執行 Phase 1 + Phase 3。
2. **僅跑 Phase 3**（需先有 backtest CSV）：  
   `python sub-py/ml_signal_winrate.py --horizons 5,10,20`  
   或雙擊 `bat/7_phase3_ml_winrate.bat`
3. **產出**：
   - `data/models/ml_winrate_ret5d.pkl`、`ret10d`、`ret20d`：訓練好的模型
   - `data/ml_feature_importance_ret{N}d.csv`：特徵重要度排序
   - `data/ml_winrate_report_ret{N}d.html`：簡易 HTML 報表（accuracy / AUC / Top 30 特徵）

依賴：`pip install scikit-learn joblib`（已列入 `requirements.txt`）。

---

python .\sub-py\backtest_signals_60d.py --stock_ids 2338 --days 60 --horizons 5,10,20