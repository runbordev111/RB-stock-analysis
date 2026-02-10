VS Code
Ctrl + Shift + P >>輸入並選擇：Python: Select Interpreter 選擇路徑中有 ('venv': venv) 的那一個。
Ctrl + ~ >>(數字 1 左邊那個鍵) 開啟終端機 >> git pull origin master
//--------------------------------------------------------------------------------------
start cmd /k "cd C:\ngrok\ && ngrok http --domain=medicably-aeromechanical-yadiel.ngrok-free.dev 80"
start cmd /k "cd C:\ngrok\RB_DataMining && .\venv\Scripts\activate && python rb_tv_app.py"

.\ngrok http --domain=medicably-aeromechanical-yadiel.ngrok-free.dev 80
python rb_tv_app.py
(ngrok Authtoken)

//---------------------------------------------
(右邊視窗)	Python 程式 (rb_tv_app.py)	要venv！	因為程式需要用到您裝在裡面的 pandas、flask 等套件。
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
python -m venv venv
.\venv\Scripts\activate
pip install FinMind
pip install tqdm
pip install tqdm pandas flask requests FinMind python-dotenv
>>>python rb_tv_app.py

//------------
(左邊視窗)	ngrok 工具 (ngrok.exe) 它跟 Python 無關。
git pull origin master
cd c:\ngrok
>>.\ngrok config add-authtoken 2JU9XuyzEviEi5agopY8srTvXNp_5Em8z3aqiDk9txMpEpR4W
>>.\ngrok http --domain=medicably-aeromechanical-yadiel.ngrok-free.dev 80


(永久門牌)
https://medicably-aeromechanical-yadiel.ngrok-free.dev/webhook

git config --global user.name "RTKmick"
git config --global user.email "mick.sung01@gmail.com"


//===========================================================================

# --- 永豐金證券 Shioaji API 設定 ---
SHIOAJI_API_KEY="14WkzEdjiPEPzaS6LF9Hwa1aFDKBNsPN68A3HLxBF7d"
SHIOAJI_SECRET_KEY="79VwzxE3rrXpD6UzqydMvtCThfUvN4cNbZqeonoxVNXu"
SHIOAJI_CERT_PATH="./Sinopac.pfx"
SHIOAJI_CERT_PASSWORD="B121565115"
SHIOAJI_PERSON_ID="B121565115"

# --- Telegram Bot 設定 ---
TELEGRAM_BOT_TOKEN="8193955449:AAG50J-zOIdy-8R_6Totsa2Fh_THf6E1qWI"
TELEGRAM_CHAT_ID="-4917957878"

# --- LINE Notify / Bot ES_LBot_ID 設定 --- 
LINE_USER_ID="Ucd148c769b3342b47244c6b0013fcb0c"
LINE_CHANNEL_ACCESS_TOKEN="41y5oArc5Rb+tcBFeWjq3nvSFWQmorqLu8QPt5v3Nx38tyl6SnMp1kVfx8j2swXxMALcz+gJ7WQvhgL8HL7>
LINE_CHANNEL_SECRET="eba6cfb3df016211a02d46b3ca41b654"

# --- 安全驗證 ---
WEBHOOK_PASSPHRASE="runbor111"

//===========================================================================
#ngrok Authtokens
.\ngrok.exe config add-authtoken 2JU9XuyzEviEi5agopY8srTvXNp_5Em8z3aqiDk9txMpEpR4W

#ngrok 安裝 （或重新開啟 CMD）手動輸入
cd C:\ngrok\RB_DataMining
python -m venv venv
.\venv\Scripts\activate
pip install flask requests shioaji
python -m pip install --upgrade pip
pip install flask requests shioaji python-dotenv

//===========================================================================
#ngrok git pull to GitHub (本機上傳到GitHub)
C:\ngrok\RB_DataMining>
git add .
git commit -m "V1.0.0"
git push origin master
//===========================================================================//
#GitHub git push to ngrok (本機下載到ngrok)
cd C:\ngrok
git clone https://github.com/RTKmick/RB_DataMining.git

cd C:\ngrok\RB_DataMining
git pull origin master
//===========================================================================//
# 建立並啟動環境
python -m venv venv
.\venv\Scripts\activate

# 一次裝好所有裝備
pip install --upgrade pip
pip install flask requests shioaji python-dotenv python-telegram-bot pandas
//============================================================================
# ======================================================
# 3. 接收 TradingView 訊號的路徑
#=======================================================
# Version 1.0.0
#"ticker": "{{ticker}}",    // TradingView 會自動填入股票代號 (例如 2330)
#"price": "{{close}}",      // 會填入觸發當時的收盤價
#"support": "1000",         // 這是您手動填入的支撐位 (目前暫代 200MA)
#"action": "buy"            // 告訴您的程式這是一筆「買入」訊號

//=================================================================================
FinMind
mick.sung01@gmail.com
mickplus1


地緣券商（新竹分點）：力成總部在新竹，若發現如「新竹、竹北」區域的券商在 1/28-2/3 連續買超，這通常代表公司內部人或區域大戶的動作，具有極高參考價值。
外資關鍵庫存券商：觀察「摩根大通」、「美林」或「高盛」這類分點。如果你的法人數據顯示外資在買，但分點數據顯示是這幾家主要外資券商在「連續慣性買進」，則趨勢較穩。
隔日沖分點：如果在某天大買後，隔 1-2 天就全數出脫（如凱基-台北、富邦-建國），則要小心籌碼不穩。
//======================================
//TradingView
//FinMind
//VS Code
//SQLite https://sqlitebrowser.org/dl/
//TradingView → Python → SQLite → AI → 永豐 API → Dashboard
//


第一個資料庫 tw_stock.db
第一張表 chip_inst
Python 寫入程式（CSV → SQLite）
SQL 查詢模板（外資連買、大戶連買等）


即便技術面突破了，籌碼面不夠硬就不給過
內部大戶（大股東/董監事）：這類籌碼通常看「長線」，因為大股東不會天天買賣。>>「長線」(X)
法人大戶（三大法人）：外資、投信、自營商。這類籌碼看「波段」，股價發動的推手。「短線」
主力大戶（分點大戶）：隱藏在證券分點（例如：凱基-台北、摩根大通）後面的神祕資金。>>「短線」

操作方式：收盤後（建議在 21:30 之後，等數據完全處理完畢），腳本自動去抓這 10 支股的分點買賣紀錄。
>>監看大戶持有比例

時間,動作,目的
14:00 - 15:00,抓取「三大法人」初步數據,快速確認技術面突破是否伴隨法人買盤。
18:00 - 20:00,抓取「分點買賣明細」,確認是否有「隔日沖分點」或「關鍵分點」進場。
21:30 之後,執行「最終過濾」並存檔,這時數據最完整，適合存入您的 CSV 資料庫做長期記錄。

//=================================================================================
RB_DataMining/
  scraper_chip.py                # 只留 main + run_strategy（流程）
  core/
    __init__.py
core/finmind_client.py：  API I/O、retry、log、回傳 DataFrame
core/adapter_tw.py：      把 API dataset 抽象成 domain method（trading dates、name、daily report）
core/broker_master.py：   靜態資料載入與映射
core/price_data.py：      價格資料抽取 + 欄位 normalize（OHLCV schema 固化）
core/indicators_tv.py：   TV radar 指標（只吃 OHLCV DataFrame，不碰 API）
core/regime.py：          中期 regime（只吃 price_df，不碰 API）
core/signals_whale.py：   分點資料清洗、集中度、廣度、top15、master_trend（只吃 trading df，不碰 API）
core/pipeline.py：        組裝流程（整合 price/regime/tv/whale signals）
core/config.py :          集中所有門檻與權重
core/types.py :           把 signals/pack 的資料契約固定
core/aggregate.py :       把 aggregation 抽成


scraper_chip.py：entrypoint / orchestration（args、run_strategy、輸出 JSON/CSV）

A. 大戶短期力道（Whale）>>分數如何訂??>>>>大戶買越兇、散戶賣越快、集中度越高，分數就越高。
Strength（40%）：集中度（5日、20日）標準化後的組合
Direction（25%）：NetBuy（5日、20日）用 tanh() 壓縮後的方向性
Breadth（25%）：廣度比（買超家數占比）偏離 0.5 的程度
Divergence/Sync（10%）：外資 vs 本土是否同向且力道是否接近（協同加分，背離扣分）

所以它不是「憑空主觀打分」，而是把短期籌碼拆成：集中（誰在買）+方向（買超/賣超）+擴散（多少人買）+外本協同，綜合成 0~100。
你畫面上 Score=66.0 通常代表：集中度不低 + 方向偏多 + 廣度偏多或協同有加分，但不是「極端強」。


20 日集中度 WantGoo公式（%）>>這段時間「買超前 15 名的總量 - 賣超前 15 名的總量」佔「總成交量」的百分比。
5% 以上：代表主力有在收貨，具備觀察價值。10% 以上：代表主力積極吃貨，行情發動機率高。

TopN（預設 15）買方分點的「買進量 buy」總和 ÷ 全市場買進量 buy 總和 × 100% 注意：不是持股比例、不是大戶持有多少張，而是「期間內買方成交的集中程度」。
高集中度＝買盤更集中在少數強勢分點（常見於主力作價/控盤/收貨），低集中度＝買盤分散（可能是散戶盤、或輪動較雜）
「集中度上升 + NetBuy 為正 + Regime 不差」 才是偏高勝率組合。若集中度高但 NetBuy 轉負，常見情境是：高位換手/出貨也會集中（要靠趨勢與價格行為確認）。

NetBuy 1/5/20 日張數 >>統計這段時間內主力到底是「淨買入」還是「淨賣出」。 * 這是看主力 吃貨還是在倒貨。 1d/5d/20d 同步為正且遞增：代表主力長短期觀點一致，正在持續加碼。
期間內全市場（所有分點）net = buy - sell 的總和，再換算成張（/1000）
NetBuy > 0：整體籌碼在「淨買超」NetBuy < 0：整體籌碼在「淨賣超」
1日看短打、5日看短波段、20日看操作段（你目前 20 日 NetBuy=80 張屬於偏小，但 Top15 差值很大，代表「主要力量在 Top15 分點」）
**關鍵點：**NetBuy 是「全體合計」，不等於「主力」；你要搭配：
Top15 主力差（主力是否在收貨） 集中度（買盤是否集中） Regime（價格結構是否支持）

Breadth 买VS卖 家數 >> 這是「籌碼集中度」的終極體現。* 
買超家數：該期間 net>0 的分點數 賣超家數：該期間 net<0 的分點數  breadth_ratio：買超家數 ÷（買超+賣超）
買超家數 < 賣超家數 (差值為負)：代表「少數人在買，多數人在賣」。這才是籌碼集中，主力正在從散戶手中收回股票。
買超家數 > 賣超家數：籌碼分散，即便股價漲，也容易遇到散戶賣壓。
Breadth 高：買方「參與者多」→ 市場共識偏多（但不代表集中）Breadth 低：少數人撐盤或市場偏空
集中 vs 廣度是兩件事：
集中度＝買量集中在少數分點（控盤感）
Breadth＝買超分點多不多（共識感）
典型高勝率組合常見是：集中度上升（控盤）+ Breadth 不差（不是只有一兩家硬拉）


外資五日主力 vs 本土五日主力 >>判斷這檔股票是「外資盤」還是「內資/本土實力派盤」。
外資買、本土賣：可能是走長期趨勢，跟著外資做波段。
外資賣、本土買：可能是內資主力（如投信或特定分點）在作帳，股價波動通常比較快。

外資正、本土正：協同偏多（加分）
外資負、本土正：常見是 內資拉、外資砍（短線可能仍走得動，但波動/假突破風險提高）
外資正、本土負：可能是 外資作價、內資調節（中期需看 Regime 是否承接）
兩者皆負：偏空（或高檔派發）
你畫面：外資 -6614、本土 +6381 → 典型「外砍內接」，短期可以走，但要更依賴 Regime/價格行為做風控。


Top6 大戶十日軌跡（量化可視化）>>監控最核心的 6 個席位。實戰判斷： 看「總計（黃色虛線）」是否斜率向上。若股價跌但黃線升，就是典型的「主力護盤/背離吃貨」。
同步性：Top6 是否一起往上（真協同）斜率變化：斜率變大＝加速收貨；斜率轉平/下＝動能衰退 總計線（你那條黃虛線）：代表 Top6 合力是否擴大
如果總計線往上、且 2~3 家大戶同步推進，通常比「只剩一家硬買」更可靠。

Top15 買 / 賣（含連買/連賣） >>找出「特定贏家分點」。實戰判斷： 如果看到某分點「連買 5 天以上」，這就是所謂的 「囤貨主力」，最值得跟隨。
買方 Top15：淨買最多的 15 家 賣方 Top15：淨賣最多的 15 家
連買/連賣：連續幾天淨買/淨賣（你現在已補欄位不會再 KeyError）

怎麼用：

找「核心主力」：同一家出現在 Top15 買方且連買增加
找「對倒/洗盤」：同一集團或相關券商在買賣兩側反覆出現
找「壓力來源」：賣方 Top15 的前 3~5 名是否集中、是否連賣加速

//-------------------------------------------------------------------------------

C. Regime (中期結構底座)
趨勢判斷（ma20/60 + slope + breakout120天）

>>透過 MA20/60 的排列與 120 天高低點，判斷目前的「天氣」。實戰判斷： * Bull (多頭)：順風，主力推升容易。Transition (轉折)：小心主力開始獲利了結或盤整。
現在是不是「趨勢盤」？如果不是，最可能是盤整或轉折？你要拿它做「策略切換」：
趨勢盤（bull/bear）：順勢策略比較有效       盤整（range）：突破失敗多、要縮倉或改用區間策略    轉折（transition）：訊號可靠性低、以風控優先

ATR 風險 
>>平均真實波幅。實戰判斷： 數值越大代表波動越劇烈，你的停損位要設得更遠。
ATR 的定位是 波動/風險係數：
ATR 高：代表日內震盪大 → 停損要放寬、槓桿要降低
ATR 低：代表波動小 → 容易被小洗盤掃到，停損要更講究位置

產生 regime_score（0~100）>>regime_score：你「盤勢品質有多好」（趨勢明確、斜率一致、突破成立 → 分數高）
趨勢 regime_trend（bull/bear/range/transition）>> regime_trend：你「現在在哪種盤勢」


final_score = 0.6 × whale_score  +  0.4 × regime_score
結合了「籌碼力道」與「股價趨勢」。
Whale：短期籌碼動能（快）
Regime：中期結構（慢）
**final_score 的用途：**把「快訊號」放在「慢底座」之上，避免只靠短期籌碼追高或抄底。


實戰判斷： * > 75 (A等)：天時（趨勢）地利（籌碼）俱備，最強買入點。

45~60 (C等)：雖然有大戶在買，但趨勢還沒出來，可能還在盤整（洗盤）。
//---------------------------------------------------------------------
B. TradingView Smart Money Radar（TV 系統）
VWAP 主力成本線站上（sig_vwap）VWAP：股價站在主力成本上。
吸籌偵測（sig_accumulation）Accumulation：有吸籌跡象。
爆量換手（rotation）
HVN 關鍵區（profile zone）
結構突破（BOS）BOS：結構已經突破。

總分 tv_score 0~5
等級：weak / watch / strong  >>3分以上才買??是的。 在 TV 系統中，3 分代表「訊號共振」：
tv_score >= 3：可視為「價格行為確認」較完整（偏確認訊號）

但是否交易仍要看 Regime（盤整時 3 分也可能假突破）、以及 Whale（籌碼是否配合）
更穩健的企業級規則是「三層 gating」：
Regime 不是 bear / 或 transition 但有風控
Whale_score > 60 且集中度/Top15 差值支持
TV_score >= 3 作為進場/加碼確認

3 分 (Watch)：代表進入攻擊預備區。
4-5 分 (Strong)：主力已經發動攻擊，這是「追價型」的買點。


➜ 20 日 Top15 主力差（張） = 買前15張數 − 賣前15張數 這指標代表：
> 0 → 主力 20 日淨積極收貨
< 0 → 主力 20 日淨賣壓
越大越偏多頭主力行為
貼近 0 → 洗盤 or 中性盤整


python .\scraper_chip.py --stock_id 6239 --days 20 --debug_tv