For RUMBOR Data Mining

---

核心概念：**用 FinMind/TWSE 原始資料 → 籌碼/技術/Geo 指標 → 策略分數 + 儀表板**

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
  - `rb_tv_app.py`（Flask）：
    - `/dashboard/`：讀取 `*_whale_track.json` + 券商 master，整理成前端需要的 `signals`、Top6、geo 等資料，渲染 `templates/dashboard.html`。
    - `/webhook`：串 TradingView 訊號 + TWSE T86，獨立的「monk 規則」檢查與 Telegram 推播。
  - `templates/dashboard.html`：Bootstrap + Chart.js 儀表板，直接使用 `signals/enhanced/geo_*` 等欄位 + Whale Trend Monitor + GEO Gate 等視覺化區塊。

- **資料與輔助腳本層**
  - `rawdata/`：公司清單、券商 master、geocode cache 等來源資料。
  - `data/`：pipeline 與 webhook 的輸出（JSON/CSV）。
  - `SubPY/`：更新券商主檔、公司 geocode 等小工具。
  - `Bat/`：啟動 Flask、ngrok、scraper 的批次檔。


python .\SubPY\backtest_signals_60d.py --stock_ids 2454,6239,6805 --days 60 --horizons 5,10,20