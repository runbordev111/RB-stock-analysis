## RB_DataMining 系統架構說明（草稿）

> 本文件描述目前專案實際長相 + 建議中的分層藍圖。  
> 目標：讓未來加策略 / 加功能時，有清楚的邊界與成長路徑。

---

## 1. 系統總覽

本專案可以拆成三個主要子系統：

- **批次分析 (Batch / Job)**  
  - 入口：`scraper_chip.py`  
  - 功能：呼叫 FinMind 抓取分點與價格資料，透過 `core.pipeline.analyze_whale_trajectory` 做主力/地緣/TV/Regime 等分析，輸出結果到 `data/`。

- **Dashboard Web UI**  
  - 入口：`rb_tv_app.py` → `/dashboard/`  
  - 功能：從 `data/*_whale_track.json` 讀取分析結果，包裝成 `stock_info`，使用 `templates/dashboard.html`（Bootstrap + Chart.js）呈現。

- **Webhook & Alert**  
  - 入口：`rb_tv_app.py` → `/webhook`  
  - 功能：接收 TradingView 等外部訊號，透過 TWSE T86 + 簡單籌碼規則檢查，使用 Telegram Bot 推送通知。

這三塊共用同一份「核心分析邏輯」（`core/`），但對外介面各自獨立。

---

## 2. 目前檔案與模組職責

### 2.1 入口層（Entrypoints / Interface）

- **`scraper_chip.py`**
  - CLI 入口，負責：
    - 讀取 `.env` 中的 `FINMIND_API_TOKEN`。
    - 建立 `core.finmind_client.FinMindClient` 與 `core.adapter_tw.TaiwanStockAdapter`。
    - 透過 `adapter` 取得近 180 日交易日，選出最近 N 日（預設 20 日）。
    - 呼叫 `adapter.get_daily_report(stock_id, date)` 取得「分點日報表」。
    - 呼叫 `core.broker_master.load_broker_master_enriched` 讀券商主檔。
    - 呼叫 `core.pipeline.analyze_whale_trajectory` 做主力分析。
    - 將結果輸出為：
      - `data/{stock_id}_whale_track.json`
      - `data/{stock_id}_boss_list.csv`

- **`rb_tv_app.py`**
  - Flask Web 入口，負責：
    - 建立 Flask `app`，設定 `DATA_PATH`、讀 `.env`（Telegram / FinMind token）。
    - 自行定義一版輕量 `FinMindClient`（僅用於 Web 端查詢交易日與股票名稱）。
    - 提供工具函式：
      - `clean_numeric_columns` / `append_unique_row_csv`：處理 T86 相關 CSV。
      - `send_telegram`：透過 Telegram Bot API 發訊息。
      - `get_recent_trading_dates` / `get_stock_name`：FinMind 查詢。
      - `load_broker_master_enriched`：讀 Web 版本的券商主檔（簡化版）。
      - `enrich_top6_details`：補 Top6 券商的 meta 資訊。
      - `list_available_stock_ids`：掃描 `data/*_whale_track.json` 取得可用股票清單。
    - 路由：
      - `POST /webhook`：處理 TradingView alert，套用 `check_stock_monk_rule` 規則，並以 Telegram 推播。
      - `GET /dashboard/`：載入指定 `stock_id` 的 `*_whale_track.json`，建構 `stock_info` 給模板渲染。

- **`templates/dashboard.html`**
  - Jinja2 + Bootstrap + Chart.js 的前端模板。
  - 主要顯示：
    - Whale Trend Monitor 狀態（ACCUMULATION / MARKUP / FADING / DISTRIBUTION / NEUTRAL）。
    - 集中度、主力淨買、Top15 名單穩定度、壓力比、主力差等 KPI。
    - Geo Gate（`geo_grade / geo_tag / geo_zscore / geo_affinity_score / geo_baseline_* / geo_adjust`）的中英解釋。
    - TV Smart Money Radar（5 大訊號 + `tv_score / tv_grade`）。
    - Validation / Risk（breakout / divergence / confirmation_score / ATR% / avg_turnover / invalid_flag）。
    - Distribution Risk（HHI + entropy）。
    - Breadth / Coherence 時序圖。
    - Top15 買/賣分點表、Top6 核心大戶細節表。

### 2.2 核心分析（Domain / Core Logic）

> 以下模組大多不直接碰 Flask / 檔案 / HTTP，是 Domain 層的核心。

- **`core/pipeline.py`**
  - 主分析管線 `analyze_whale_trajectory(...)`：
    - 將多日分點 DataFrame 合併 → 切出 20/10/5/1 日子集。
    - 根據 `broker_map` 補上 `org`（foreign / local）。
    - 計算：
      - Top6 核心大戶與其 10 日累積軌跡（含 streak / 成本 / 城市 / 類型）。
      - Top15 買/賣分點與 `net_lot`、外/本、城市、連買/連賣。
      - 集中度（5D / 20D）、淨買張數（1D / 5D / 20D），廣度與廣度序列。
      - 外資 / 本土 5 日淨買、breadth series。
    - 價格面：
      - 透過 `core.price_data.fetch_ohlcv_20d` 取得近 20 日 OHLCV。
      - 呼叫 `core.indicators_tv.compute_tv_radar_signals` 產生 TV Smart Money 5 訊號。
      - 呼叫 `core.regime.compute_regime_signals` 產生中期 Regime 分數與標籤。
    - 驗證 / 風控：
      - 直接在 pipeline 內實作 MVP 的 Validation / Risk 訊號（行為接近 `core.signals_validation` / `core.risk_rules`）。
    - Geo：
      - `_load_company_geo_map()`：讀取 `rawdata/TSE_Company_V2.csv` / `OTC_Company_V2.csv` 取得公司 HQ 經緯度。
      - `core.geo_utils.compute_geo_topn_features`：計算 Top5 買超分點到 HQ 的距離與 affinity。
      - 進一步計算 baseline / Z-score / Grade / Tag 等 geo 指標。
    - 最後：
      - 呼叫 `core.aggregate.compute_final_pack` 合成 `final_score / final_grade / geo_adjust`。
      - 呼叫 `core.signals_whale.compute_master_trend` 產生 `score / trend / tags / whale_radar`。
      - 建立 `Insight` 結構（`history_labels / whale_data / total_whale_values / top6_details / signals`）回傳。

- **`core/signals_whale.py`**
  - 分點籌碼相關的基礎指標：
    - 欄位標準化 `standardize_columns`。
    - 廣度 `calc_breadth`，集中度 `compute_concentration`。
    - broker 連買/連賣 `compute_streaks`。
    - Top15 買/賣表格 `build_top15_tables`。
    - 外資 vs 本土淨買 `compute_foreign_local_net`。
    - 廣度序列 `build_breadth_series`。
    - 主力走向 / 雷達 `compute_master_trend`（含 strength/direction/breadth/divergence 等計算）。

- **`core/indicators_tv.py`**
  - TV Smart Money Radar 相關：
    - VWAP、OBV、累積/分配 (A/D) 計算。
    - 根據 OHLCV 產生：
      - `sig_vwap`（成本線控制）
      - `sig_accumulation`（壓縮+吸籌）
      - `sig_rotation`（爆量換手）
      - `sig_profile_zone`（HVN 區間）
      - `sig_structure_break`（結構突破）
      - `tv_score` + `tv_grade`。

- **`core/regime.py`**
  - 中期 Regime / 趨勢底座：
    - 用 120–250 日 K 計算 `ma20 / ma60 / atr14 / atrp14`。
    - 綜合方向（均線排列 + ma60 斜率）、結構（120 日高低）與波動（ATR%）得到：
      - `regime_score(0~100)`、`regime_trend`（bull/bear/range/transition）、`regime_tags`。

- **`core/geo_utils.py`**
  - 地緣距離計算：
    - `haversine_km`：兩經緯度之間的球面距離 (km)。
    - `compute_geo_topn_features`：輸入 TopN 買超分點 + HQ 經緯度 + broker_map，輸出：
      - `geo_top5_avg_km / geo_top5_min_km / geo_top5_wavg_km / geo_affinity_score / geo_top5_detail`。

- **`core/aggregate.py`**
  - 最終分數聚合：
    - `grade_from_score`：依 `PipelineConfig.FinalScoreConfig` 將分數轉成 A/B/C/D。
    - `compute_final_pack`：
      - 以權重 `weight_whale / weight_regime` 合成 `final_score`。
      - 基於 `geo_baseline_weight` 產生折減係數 `geo_adjust`。
      - 回傳 `final_score / final_grade / weights / geo_adjust`。

- **`core/price_data.py`**
  - 價格資料存取與欄位統一：
    - `_normalize_price_df`：將 FinMind `TaiwanStockPrice` 的輸出統一成 `date, open, high, low, close, volume`。
    - `fetch_ohlcv_20d`：在指定交易日區間抓取近 20 日 OHLCV。
    - `fetch_price_nd`：根據日曆天數抓長期日 K（配合 regime 用）。

- **`core/broker_master.py`**
  - 券商主檔讀取：
    - `_read_broker_master_any`：優先讀 `broker_dimensions_master_V3.xlsx`（含經緯度），否則退回 `broker_master_enriched.csv`。
    - `load_broker_master_enriched`：回傳 `broker_id -> meta(dict)`，包含城市、外/本、是否自營、席位類型、經緯度等。

- **`core/finmind_client.py`**
  - FinMind API 客戶端（核心版）：
    - 建立帶 Retry 的 `requests.Session`。
    - `request_data`：呼叫 `api/v4/data`，帶 dataset / data_id / 日期區間。
    - `request_trading_daily_report`：呼叫 `api/v4/taiwan_stock_trading_daily_report` 抓分點明細。
    - 內建 log（http status / api status / latency / params），並對權限錯誤印警示。

- **`core/adapter_tw.py`**
  - 台股專用 Adapter：
    - `get_trading_dates`：包 `TaiwanStockTradingDate`。
    - `get_daily_report`：呼叫 `request_trading_daily_report`。
    - `get_stock_name`：用 `TaiwanStockInfo` 抓股票名稱。

- **`core/types.py`**
  - TypedDict 型別定義（Signals / Insight / TVPack / RegimePack 等），提供結構約束與 IDE 提示。

- **`core/config.py`**
  - Pipeline 設定 (`RegimeConfig`, `FinalScoreConfig`, `PipelineConfig`)，集中權重與門檻設定。

### 2.3 驗證 / 風控輔助模組

- **`core/signals_validation.py`**
  - 以價格序列 + 主力分數做交易有效性驗證（目前 pipeline 內有類似邏輯）。

- **`core/risk_rules.py`**
  - 以 ATR%、成交金額與均線位置給出簡單的風控指標（目前 pipeline 內也有 MVP 版計算）。

> 註：這兩個模組目前尚未完全被 `pipeline.py` 正式導入，之後重構時可統一由這裡輸出 Validation / Risk。

---

## 3. 建議的分層藍圖（未來 refactor 方向）

雖然現況多數邏輯都在 `scraper_chip.py` / `rb_tv_app.py` / `core/pipeline.py`，  
但從責任切割角度，可以朝以下四層演進：

1. **Interface 層（UI / API / CLI）**
   - 仍然由：
     - `scraper_chip.py`（CLI 批次入口）
     - `rb_tv_app.py`（Flask 入口）
   - 職責：解析輸入參數（CLI args / HTTP request），呼叫 Application 層，不做業務邏輯。

2. **Application 層（Use Case / Service 編排）**
   - 建議新增（示意命名）：
     - `app/usecases/run_whale_analysis.py` → `RunWhaleAnalysisUseCase.run(stock_id, days)`  
     - `app/usecases/get_dashboard_data.py` → `GetDashboardDataUseCase.get(stock_id)`  
     - `app/usecases/check_alert_rule.py` → `CheckAlertRuleUseCase.evaluate(alert_payload)`  
   - 職責：組合 Domain + Infra，定義「一條完整流程」，不關心 Flask 細節或 DataFrame 細節。

3. **Domain 層（分析邏輯 / 策略引擎）**
   - 目前已經存在的：
     - `core/pipeline.py`、`signals_whale.py`、`indicators_tv.py`、`regime.py`、`geo_utils.py`、`aggregate.py`、`types.py`。
   - 未來可以在這層新增：
     - `core/strategies/`：封裝各種策略（例如：Whale Accumulation、Breakout Follow、Geo Focus Entry 等）。
     - 策略介面：`strategy.evaluate(signals) -> Decision`。

4. **Infrastructure 層（外部資源 Adapter）**
   - 目前已存在：
     - `core/finmind_client.py`、`core/broker_master.py`、`core/price_data.py`。
     - `rb_tv_app.py` 內的 T86 抓取、Telegram API 呼叫、讀寫 `data/*` 檔案。
   - 建議將這些逐步集中成：
     - `core/infra/finmind_client.py`
     - `core/infra/twse_t86_client.py`
     - `core/infra/telegram_client.py`
     - `core/infra/file_repo.py`（封裝 `*_whale_track.json` / `*_boss_list.csv` 存取）

此分層完成後：

- **加策略**：只需要在 Domain/strategies 多一個策略 class，並在 Application 層註冊即可。
- **換資料來源**（例如改 FinMind → 其他 API）：只改 Infra 層，不動 Domain。
- **調整 UI**：只動模板 / 前端，不會碰到分析邏輯。

---

## 4. 三個核心流程的資料流（未來畫圖用）

### 4.1 批次分析流程（Batch Pipeline）

1. `scraper_chip.py`（CLI）解析參數：`stock_id`, `days`, `throttle`, `debug_tv`…
2. 建立 `FinMindClient` + `TaiwanStockAdapter` + `broker_map`。
3. 用 Adapter 抓 N 日交易日 → 逐日抓 `TaiwanStockTradingDailyReport`。
4. 呼叫 `core.pipeline.analyze_whale_trajectory(...)`：
   - 內部組合 `signals_whale / indicators_tv / regime / geo_utils / aggregate`。
5. 將 `Insight` + `boss_list_df` 透過檔案（Infra）寫入：
   - `data/{stock}_whale_track.json`
   - `data/{stock}_boss_list.csv`

### 4.2 Dashboard 查詢流程

1. 使用者瀏覽 `/dashboard/?stock_id=2330`。
2. `rb_tv_app.py` 解析 `stock_id`，呼叫：
   - 檔案讀取（未來可由 `file_repo` 提供）。
   - `get_stock_name`、`load_broker_master_enriched` 等輔助函式。
3. 將 JSON 內容與券商 meta 組成 `stock_info`：
   - `stock.id / stock.name / stock.last_update / stock.probe_date`。
   - `stock.signals`（含 `score_unified / trend / geo / tv / regime / validation / risk / flags`）。
   - `stock.whale_data / stock.total_whale_values / stock.top6_details`。
4. `templates/dashboard.html` 使用 `stock_info` 渲染 UI 與 Chart.js 圖表。

### 4.3 Webhook Alert 流程

1. TradingView 對 `/webhook` 發送 JSON（含 `ticker`, `price` 等）。
2. `rb_tv_app.py` → `webhook()`：
   - 正規化 ticker（去掉 TWSE:/TPEX: 前綴）。
   - 呼叫 `check_stock_monk_rule(stock_id)`：
     - 透過 TWSE T86 API 抓當天 T86。
     - 累積近 5 日外資 / 投信買賣超到本地 CSV。
     - 判斷雙資近 5 日是否同買。
   - 組合提醒文字，呼叫 `send_telegram` 推播。
3. 回傳 JSON 給 TradingView，表示規則是否通過。

---

## 5. 未來重構的實作順序建議

此部分作為實作藍圖，重構時可依序進行（不必一次完成）：

1. **文件先行**  
   - 持續在本檔補充與修正實際架構變化，當作「單一真實來源」(single source of truth)。

2. **抽出 Application Use Cases（不必馬上改路由）**
   - 從 `scraper_chip.py` 抽出 `RunWhaleAnalysisUseCase` 到新檔案（例如 `app/usecases/run_whale_analysis.py`）。
   - 從 `rb_tv_app.py` 抽出 `GetDashboardDataUseCase` 與 `CheckAlertRuleUseCase`。

3. **清理 Infra：統一 FinMind / Broker / File Repo**
   - 將 Web 端與 Core 端兩套 FinMindClient 合併為單一實作。
   - 把券商主檔讀取邏輯集中在 `core/broker_master.py`。
   - 寫一個 `file_repo` 封裝 `*_whale_track.json` / `*_boss_list.csv` 的讀寫。

4. **導入策略引擎（選配）**
   - 定義 `Strategy` 介面與 `Decision` 結構。
   - 將現有 `Whale Trend Monitor` / `Geo Gate` / `Distribution Risk` 等邏輯整理為策略或解釋器。
   - 在 Dashboard 與 Webhook 上，改成消費 Strategy Engine 的結果。

透過上述步驟，可以讓這個專案在功能持續擴充的同時，維持清楚的分工與可維護性。

