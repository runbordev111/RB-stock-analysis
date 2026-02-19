# core/pipeline.py
import pandas as pd

from core.signals_validation import compute_validation_signals
from core.risk_rules import compute_risk_signals

from core.signals_whale import (
    standardize_columns,
    compute_concentration,
    calc_breadth,
    compute_foreign_local_net,
    build_top15_tables,
    build_breadth_series,
    compute_master_trend,
)
from core.price_data import fetch_ohlcv_20d, fetch_price_nd
from core.indicators_tv import compute_tv_radar_signals
from core.regime import compute_regime_signals

from core.config import PipelineConfig
from core.aggregate import compute_final_pack
from core.types import Insight
from core.geo_utils import compute_geo_topn_features



# --- Company HQ Geo (TSE/OTC) -------------------------------------------------
from functools import lru_cache

@lru_cache(maxsize=2)
def _load_company_geo_map(tse_csv: str = "./rawdata/TSE_Company_V2.csv", otc_csv: str = "./rawdata/OTC_Company_V2.csv") -> dict:
    """
    讀上市/上櫃公司地址與經緯度，回傳 stock_id -> {lat, lon, address, name}
    預設路徑可依你的專案：C:\ngrok\RB_DataMining\rawdata\...
    """
    out = {}
    for p in [tse_csv, otc_csv]:
        try:
            df = pd.read_csv(p, dtype=str, encoding="utf-8-sig").fillna("")
        except Exception:
            continue
        # 欄位：公司代號/公司簡稱/住址/Latitude/Longitude
        for _, r in df.iterrows():
            sid = str(r.get("公司代號", "")).strip()
            if not sid:
                continue
            out[sid] = {
                "name": str(r.get("公司簡稱", "")).strip(),
                "address": str(r.get("住址", "")).strip(),
                "lat": str(r.get("Latitude", "")).strip(),
                "lon": str(r.get("Longitude", "")).strip(),
            }
    return out
# -----------------------------------------------------------------------------
def analyze_whale_trajectory(
    frames: list[pd.DataFrame],
    target_dates: list[str],
    broker_map: dict,
    adapter,
    stock_id: str,
    debug_tv: bool = False,
    cfg: PipelineConfig | None = None,
) -> tuple[Insight | None, pd.DataFrame | None]:
    """
    - Top6 軌跡（10日）
    - signals（20日 + 5日 + 廣度序列 + TV radar + Regime + Final aggregation）
    """
    cfg = cfg or PipelineConfig()

    if not frames:
        return None, None

    combined = pd.concat(frames, ignore_index=True)
    combined = standardize_columns(combined)
    if combined.empty:
        return None, None

    combined["date"] = combined["date"].astype(str)

    # 補上 org 欄位：讓 df_1d 可以用 org 分外資/本土
    combined["broker_id"] = combined["broker_id"].astype(str).str.strip()
    combined["org"] = combined["broker_id"].map(lambda x: (broker_map.get(x, {}).get("broker_org_type", "unknown") or "unknown"))

    date_20d = list(target_dates)
    date_10d = date_20d[-10:] if len(date_20d) >= 10 else date_20d
    date_5d = date_20d[-5:] if len(date_20d) >= 5 else date_20d
    last_1d = date_20d[-1]

    df_20d = combined[combined["date"].isin(date_20d)].copy()
    df_10d = combined[combined["date"].isin(date_10d)].copy()
    df_5d = combined[combined["date"].isin(date_5d)].copy()
    df_1d = combined[combined["date"] == last_1d].copy()

    # Top6（10日用 net 買方）
    agg_10d = df_10d.groupby(["broker_id", "broker_name"], as_index=False).agg(
        buy=("buy", "sum"),
        sell=("sell", "sum"),
        net_buy=("net", "sum"),
    )
    top6 = agg_10d.sort_values("net_buy", ascending=False).head(6).copy()
    top6_ids = top6["broker_id"].astype(str).tolist()

    # 讓 Top6 也有 streak
    from core.signals_whale import compute_streaks  # 若尚未 import
    pivot10 = (
        df_10d.pivot_table(index="date", columns="broker_id", values="net", aggfunc="sum")
        .fillna(0)
        .reindex(date_10d)
        .fillna(0)
    )
    streaks10 = compute_streaks(pivot10)

    # Top6 details
    has_price = "price" in combined.columns
    top6_details = []

    # 先做一次清洗，避免型別/空白造成對不到
    combined["broker_id"] = combined["broker_id"].astype(str).str.strip()

    # 確保 streaks10 一定是 dict（避免 None / 其他型別）
    if not isinstance(streaks10, dict):
        streaks10 = {}

    for _, r in top6.iterrows():
        bid = str(r.get("broker_id", "")).strip()
        if not bid:
            continue

        bname = str(r.get("broker_name", "")).strip()

        # streaks（用 bid 查）
        st = streaks10.get(bid, {}) or {}
        sb = int(st.get("streak_buy", 0) or 0)
        ss = int(st.get("streak_sell", 0) or 0)

        bdata = combined[combined["broker_id"] == bid]

        n10d = float(r.get("net_buy", 0) or 0) / 1000.0
        n5d = float(bdata[bdata["date"].isin(date_5d)]["net"].sum()) / 1000.0
        n1d = float(bdata[bdata["date"] == last_1d]["net"].sum()) / 1000.0

        avg_p = 0.0
        if has_price and not bdata.empty:
            buy_only = bdata[bdata["buy"] > 0]
            if not buy_only.empty and float(buy_only["buy"].sum()) > 0:
                avg_p = float((buy_only["buy"] * buy_only["price"]).sum() / buy_only["buy"].sum())

        meta = broker_map.get(bid, {}) or {}
        print(f"🔎 TOP6 bid={bid} name={bname} in_broker_map={bool(meta)} meta_keys={list(meta.keys())[:6]}")

        top6_details.append({
            "broker_id": bid,
            "broker_name": bname,
            "net_10d": round(n10d, 1),
            "net_5d": round(n5d, 1),
            "net_1d": round(n1d, 1),
            "avg_price": round(avg_p, 2),
            "city": meta.get("city", "") or "",
            "broker_org_type": meta.get("broker_org_type", "unknown") or "unknown",
            "is_proprietary": meta.get("is_proprietary", "") or "",
            "seat_type": meta.get("seat_type", "") or "",
            "streak_buy": sb,
            "streak_sell": ss,
        })


    # Top6 軌跡矩陣（累積 net）
    whale_detail = df_10d[df_10d["broker_id"].isin(top6_ids)].copy()
    pivot_net = whale_detail.pivot_table(index="date", columns="broker_name", values="net", aggfunc="sum").fillna(0)
    pivot_cumsum = pivot_net.reindex(date_10d).fillna(0).cumsum()

    colors = ["#FF6384", "#36A2EB", "#FFCE56", "#4BC0C0", "#9966FF", "#FF9F40"]
    whale_data = []
    for i, name in enumerate(pivot_cumsum.columns):
        whale_data.append({
            "name": name,
            "values": (pivot_cumsum[name] / 1000.0).round(1).tolist(),
            "color": colors[i % len(colors)],
        })
    total_whale_values = (pivot_cumsum.sum(axis=1) / 1000.0).round(1).tolist()

    # signals（短期）
    c20 = compute_concentration(df_20d, top_n=15)
    c5 = compute_concentration(df_5d, top_n=15)

    net_1d_lot = round(float(combined[combined["date"] == last_1d]["net"].sum()) / 1000.0, 1)
    net_5d_lot = round(float(df_5d["net"].sum()) / 1000.0, 1)
    net_20d_lot = round(float(df_20d["net"].sum()) / 1000.0, 1)

    b20 = calc_breadth(df_20d)
    b5 = calc_breadth(df_5d)

    fl5 = compute_foreign_local_net(df_5d, broker_map)
    top15_pack = build_top15_tables(df_20d, broker_map, date_20d)
    breadth_series_pack = build_breadth_series(df_20d, date_20d)

    signals = {
        "concentration_5d": c5,
        "concentration_20d": c20,

        "netbuy_1d_lot": net_1d_lot,
        "netbuy_5d_lot": net_5d_lot,
        "netbuy_20d_lot": net_20d_lot,

        "buy_count_5d": b5["buy_count"],
        "sell_count_5d": b5["sell_count"],
        "breadth_5d": b5["breadth"],
        "breadth_ratio_5d": b5["breadth_ratio"],

        "buy_count_20d": b20["buy_count"],
        "sell_count_20d": b20["sell_count"],
        "breadth_20d": b20["breadth"],
        "breadth_ratio_20d": b20["breadth_ratio"],

        "foreign_net_5d": fl5["foreign_net"],
        "local_net_5d": fl5["local_net"],

        "top_buy_15": top15_pack["top_buy_15"],
        "top_sell_15": top15_pack["top_sell_15"],

        **breadth_series_pack,
    }

    import math

    def _clamp(x, lo, hi):
        return max(lo, min(hi, x))

    def _tanh_0_100(x):# x=0 -> 50；正向上升 -> 越接近100；負向 -> 越接近0
        return round(50.0 + 50.0 * math.tanh(x), 1)

    # === A. ΔMajorFlow20：20日買方 Top15 張數 - 賣方 Top15 張數 ===
    buy_sum_20 = 0.0
    sell_sum_20 = 0.0

    if "top_buy_15" in top15_pack and top15_pack["top_buy_15"]:
        buy_sum_20 = sum([float(r["net_lot"]) for r in top15_pack["top_buy_15"]])

    if "top_sell_15" in top15_pack and top15_pack["top_sell_15"]:
        # r["net_lot"] 是負值，需取絕對值
        sell_sum_20 = sum([abs(float(r["net_lot"])) for r in top15_pack["top_sell_15"]])

    delta_major_flow_20 = round(buy_sum_20 - sell_sum_20, 1)

    signals["delta_major_flow_20"] = delta_major_flow_20
    signals["top15_buy_sum_20"] = round(buy_sum_20, 1)
    signals["top15_sell_sum_20"] = round(sell_sum_20, 1)

    # === A2. ΔMajorFlow5：5日買方 Top15 - 賣方 Top15 ===
    top15_5 = build_top15_tables(df_5d, broker_map, date_5d)

    buy_sum_5 = 0.0
    sell_sum_5 = 0.0

    if top15_5.get("top_buy_15"):
        buy_sum_5 = sum(float(r.get("net_lot", 0) or 0) for r in top15_5["top_buy_15"])

    if top15_5.get("top_sell_15"):
        sell_sum_5 = sum(abs(float(r.get("net_lot", 0) or 0)) for r in top15_5["top_sell_15"])

    delta_major_flow_5 = round(buy_sum_5 - sell_sum_5, 1)

    signals["delta_major_flow_5"] = delta_major_flow_5
    signals["top15_buy_sum_5"] = round(buy_sum_5, 1)
    signals["top15_sell_sum_5"] = round(sell_sum_5, 1)

    # ==========================
    # (NEW) Feature Engineering
    # 1) 名單穩定度：Top15 Buy 名單滾動重疊率（Jaccard）
    # 2) 壓力比：Top15 Buy vs Sell 的壓力結構（比例 + 淨壓）
    # ==========================

    # ---- (2) 壓力比：看買方/賣方誰更主導（0~1；0.5=均衡）----
    def _safe_float(x, default=0.0):
        try:
            return float(x)
        except Exception:
            return default

    # 20D 壓力
    buy_sum_20 = _safe_float(signals.get("top15_buy_sum_20", 0.0), 0.0)
    sell_sum_20 = _safe_float(signals.get("top15_sell_sum_20", 0.0), 0.0)
    denom_20 = (buy_sum_20 + sell_sum_20) if (buy_sum_20 + sell_sum_20) > 0 else 0.0

    signals["pressure_ratio_20d"] = round((buy_sum_20 / denom_20), 4) if denom_20 > 0 else None
    # 淨壓：-1~+1（>0 買方壓力、<0 賣方壓力）
    signals["net_pressure_20d"] = round(((buy_sum_20 - sell_sum_20) / denom_20), 4) if denom_20 > 0 else None

    # 5D 壓力
    buy_sum_5 = _safe_float(signals.get("top15_buy_sum_5", 0.0), 0.0)
    sell_sum_5 = _safe_float(signals.get("top15_sell_sum_5", 0.0), 0.0)
    denom_5 = (buy_sum_5 + sell_sum_5) if (buy_sum_5 + sell_sum_5) > 0 else 0.0

    signals["pressure_ratio_5d"] = round((buy_sum_5 / denom_5), 4) if denom_5 > 0 else None
    signals["net_pressure_5d"] = round(((buy_sum_5 - sell_sum_5) / denom_5), 4) if denom_5 > 0 else None

    # ---- (1) 名單穩定度：Top15 Buy 名單滾動重疊率（越高越像同一批人持續在做）----
    # 使用 df_20d（你已經有：包含 date / broker_id / net）
    # 先做每日 broker 淨買彙總，再取 TopN 正向（買方）名單
    def _topn_buy_set_by_day(df, day, top_n=15):
        try:
            dd = df[df["date"] == day].copy()
            if dd.empty:
                return set()
            g = dd.groupby("broker_id", as_index=False)["net"].sum()
            g["net"] = pd.to_numeric(g["net"], errors="coerce").fillna(0.0)
            g = g[g["net"] > 0].sort_values("net", ascending=False).head(top_n)
            return set(g["broker_id"].astype(str).tolist())
        except Exception:
            return set()

    def _jaccard(a: set, b: set):
        if not a and not b:
            return None
        u = a.union(b)
        if not u:
            return None
        return len(a.intersection(b)) / len(u)

    # 以 date_20d 的序列做穩定度（相鄰日平均 Jaccard）
    # 20D
    try:
        sets_20 = [_topn_buy_set_by_day(df_20d, d, top_n=15) for d in date_20d]
        jac_20 = []
        for i in range(1, len(sets_20)):
            v = _jaccard(sets_20[i-1], sets_20[i])
            if v is not None:
                jac_20.append(v)
        signals["top15_buy_stability_20d"] = round(float(sum(jac_20) / len(jac_20)), 4) if jac_20 else None

        # 10D（取最後10個交易日）
        date_10 = date_20d[-10:] if len(date_20d) >= 10 else date_20d[:]
        sets_10 = [_topn_buy_set_by_day(df_20d, d, top_n=15) for d in date_10]
        jac_10 = []
        for i in range(1, len(sets_10)):
            v = _jaccard(sets_10[i-1], sets_10[i])
            if v is not None:
                jac_10.append(v)
        signals["top15_buy_stability_10d"] = round(float(sum(jac_10) / len(jac_10)), 4) if jac_10 else None

        # 可選：輔助指標（平均名單大小，避免全是空集造成誤判）
        sizes_10 = [len(s) for s in sets_10 if s is not None]
        signals["top15_buy_avg_size_10d"] = round(float(sum(sizes_10) / len(sizes_10)), 2) if sizes_10 else None

    except Exception:
        signals["top15_buy_stability_20d"] = None
        signals["top15_buy_stability_10d"] = None
        signals["top15_buy_avg_size_10d"] = None
        
    # ----------------------------------------------------------

    price_df_tail = pd.DataFrame()  # 避免 TP 參照時 NameError
    # TV Radar（短期價格行為）
    ohlcv_20d = fetch_ohlcv_20d(adapter, stock_id, date_20d, debug=debug_tv)
    price_df = ohlcv_20d if ohlcv_20d is not None else pd.DataFrame()
    price_df_tail = price_df

    # === B. 主力拐點偵測 Turning Points ===
    tp = {}

    # 1) 外資 / 本土 轉折
    foreign_5 = float(fl5.get("foreign_net", 0.0))
    local_5 = float(fl5.get("local_net", 0.0))
    foreign_today = 0.0
    local_today = 0.0

    # 1-1 外資過去 5 日為賣方（總 net <0） 且 今日 net >0
    if not df_1d.empty and "org" in df_1d.columns:
        foreign_today = float(df_1d[df_1d["org"] == "foreign"]["net"].sum())
        local_today = float(df_1d[df_1d["org"] == "local"]["net"].sum())

    tp["foreign_buy_switch"] = int(foreign_5 < 0 and foreign_today > 0)
    tp["local_buy_switch"] = int(local_5 < 0 and local_today > 0)

    # 2) Top6 大戶反轉（連賣 -> 首日轉買）
    tp["whale_reversal"] = 0
    for bid in top6_ids:
        seq = df_20d[df_20d["broker_id"] == bid].sort_values("date")["net"].tolist()
        if len(seq) >= 4:
            if seq[-4] < 0 and seq[-3] < 0 and seq[-2] < 0 and seq[-1] > 0:
                tp["whale_reversal"] = 1
                break

    # 3) 異常爆量買超（買超 >= 過去20日平均 * 3）
    mean_buy20 = df_20d["buy"].mean()
    today_buy = df_20d[df_20d["date"] == last_1d]["buy"].sum()
    tp["abnormal_buy_spike"] = int(today_buy >= mean_buy20 * 3)

    # 4) 主力成本帶（Top6 均價）防守
    avg_prices = [float(r["avg_price"]) for r in top6_details if r.get("avg_price", 0) > 0]
    if avg_prices:
        cost_low = min(avg_prices)  # ✅ 必須在這裡定義
        close_recent = None
        if price_df_tail is not None and not price_df_tail.empty and "close" in price_df_tail.columns:
            close_recent = float(price_df_tail["close"].tail(3).min())
        tp["cost_zone_defended"] = int(close_recent is not None and close_recent >= cost_low)
    else:
        tp["cost_zone_defended"] = 0

    signals["turning_points"] = tp
    # ----------------------------------------------------------
    # === C1: 群聚買超 Coherence ===
    # 今日所有大戶買超家數
    df_today = df_20d[df_20d["date"] == last_1d]
    g_today = df_today.groupby("broker_id")["net"].sum()
    buy_cnt_today = int((g_today > 0).sum())

    # 過去 20 日平均買超家數
    g_all = df_20d.groupby("date", group_keys=False).apply(lambda d: (d.groupby("broker_id")["net"].sum() > 0).sum())
    avg_buy_cnt = float(g_all.mean()) if len(g_all) > 0 else 1.0

    coherence = buy_cnt_today / avg_buy_cnt if avg_buy_cnt > 0 else 0
    coherence = round(min(max(coherence, 0.0), 3.0), 2)

    enhanced = {}
    enhanced["coherence_today"] = coherence
    enhanced["buy_cnt_today"] = buy_cnt_today
    enhanced["avg_buy_cnt_20d"] = round(avg_buy_cnt, 1)
    # ----------------------------------------------------------

    # ========= Whale Radar (0~100) =========
    c20_val = float(signals.get("concentration_20d", 0) or 0)
    c20_score = round(_clamp(c20_val, 0, 100), 1)

    net1 = float(signals.get("netbuy_1d_lot", 0) or 0)
    net5 = float(signals.get("netbuy_5d_lot", 0) or 0)
    net20 = float(signals.get("netbuy_20d_lot", 0) or 0)

    # 2) 淨買方向：用 20D 尺度做 normalize，避免量級差
    scale = max(10.0, abs(net20) / 4.0)   # 你可調：/3 /5 都行
    net_dir_score = _tanh_0_100(net5 / scale)

    # 3) 買盤廣度：breadth_ratio_5d (0~1) -> 0~100
    br5 = float(signals.get("breadth_ratio_5d", 0) or 0)
    breadth_score = round(_clamp(br5, 0, 1) * 100.0, 1)

    # 4) 外本協同：同向加分、不同向扣分，並以幅度做加權
    f5 = float(signals.get("foreign_net_5d", 0) or 0)
    l5 = float(signals.get("local_net_5d", 0) or 0)
    same_sign = (f5 == 0 and l5 == 0) or (f5 > 0 and l5 > 0) or (f5 < 0 and l5 < 0)

    mag = abs(f5) + abs(l5)
    mag_score = _clamp(mag / max(1.0, mag + 20000.0), 0, 1)  # 這個 20000 可調：越小越敏感
    base = 65.0 if same_sign else 35.0
    align_score = round(_clamp(base + 35.0 * mag_score, 0, 100), 1)

    # 5) 短期加速：1D vs 5D日均（避免 5D=0 爆炸）
    avg5 = net5 / 5.0
    den = max(5.0, abs(avg5))  # 保底
    acc_score = _tanh_0_100(net1 / den)

    signals["whale_radar"] = {
        "labels": ["集中度", "淨買方向", "買盤廣度", "外本協同", "短期加速"],
        "values": [c20_score, net_dir_score, breadth_score, align_score, acc_score],
        "debug": {
            "c20": c20_val,
            "net1": net1, "net5": net5, "net20": net20, "scale": scale,
            "breadth_ratio_5d": br5,
            "foreign_5d": f5, "local_5d": l5, "same_sign": int(same_sign),
            "avg5": avg5, "den": den
        }
    }
    print("🔎 whale_radar(pipeline)=", signals["whale_radar"])

    # === C2: 主力成本帶 Range + 乖離率 (修正版) ===
    if avg_prices and ohlcv_20d is not None and not ohlcv_20d.empty:
        cost_low = min(avg_prices)
        cost_high = max(avg_prices)
        enhanced["cost_low"] = round(cost_low, 2)
        enhanced["cost_high"] = round(cost_high, 2)

        # 取得最新收盤價
        close_last = float(ohlcv_20d.iloc[-1]["close"])
        enhanced["close_last"] = close_last

        # 乖離率計算
        cost_dev = (close_last - cost_low) / cost_low if cost_low > 0 else 0
        enhanced["cost_deviation"] = round(cost_dev, 3)

        # 區域狀態判斷
        if close_last > cost_high:
            enhanced["cost_zone_status"] = "above_zone"   # 強勢突破
        elif close_last >= cost_low:
            enhanced["cost_zone_status"] = "inside_zone"  # 成本區洗盤
        else:
            enhanced["cost_zone_status"] = "below_zone"   # 跌破(弱勢)
    else:
        enhanced["cost_zone_status"] = "unknown"

    # === C3: 連買強度 (修正版) ===
    streak_strength = 0.0
    for r in top6_details:
        sb = int(r.get("streak_buy", 0)) # 使用 get 避免 KeyError
        if sb <= 0: continue
        
        bid = r["broker_id"]
        df_b = df_5d[df_5d["broker_id"] == bid]
        net5 = float(df_b["net"].sum()) / 1000.0
        # 避免分母為 0
        total_net5 = float(df_5d["net"].sum()) / 1000.0
        avg5 = total_net5 / len(top6_ids) if len(top6_ids) > 0 else 0
        
        if avg5 > 0:
            strength = (math.log(sb + 1)) * (net5 / avg5)
            streak_strength = max(streak_strength, strength)

    enhanced["streak_strength"] = round(streak_strength, 3)
    
    # 重要：存入 signals
    signals["enhanced"] = enhanced
    # ----------------------------------------------------------

    # Regime（中期趨勢底座）
    price_250d = fetch_price_nd(adapter, stock_id, lookback_days=int(cfg.regime.lookback_days))
    regime_pack = compute_regime_signals(price_250d)
    signals.update(regime_pack)

    if debug_tv:
        print("OHLCV rows=", 0 if ohlcv_20d is None else len(ohlcv_20d))
        print("OHLCV cols=", [] if ohlcv_20d is None else list(ohlcv_20d.columns))
        print("OHLCV tail=\n", ohlcv_20d.tail(3) if ohlcv_20d is not None and not ohlcv_20d.empty else None)

    tv_pack = compute_tv_radar_signals(ohlcv_20d, debug=debug_tv)
    if debug_tv:
        print("TV_PACK=", tv_pack)

    signals.update(tv_pack)

    # 最後算主力走向（一次）
    trend_pack = compute_master_trend(signals)
    signals["trend_score"] = float(trend_pack.get("score", 0) or 0)
    signals["trend"] = trend_pack.get("trend", "")
    signals["tags"] = trend_pack.get("tags", [])
    signals["score"] = signals["trend_score"]          # 給 validation / 其它模組用
    signals["score_unified"] = signals["trend_score"]  # 給 dashboard 既有欄位用
    signals["whale_radar"] = trend_pack.get("whale_radar", {}) or {} 

    # ---------------------------------
    def norm_pct(x):
        try:
            v = float(x or 0)
        except Exception:
            return 0.0
        # 若 v <= 1，推定是 0~1，轉成 0~100
        if 0 <= v <= 1:
            v *= 100.0
        return max(0.0, min(100.0, v))
    # ---------------------------------
    # ✅ 放置位置建議：
    # 1) 先把 ohlcv_20d 抓到（fetch_ohlcv_20d 已完成）
    # 2) 並且 signals["score"] 已經算完（compute_master_trend 後）
    # 3) 在 compute_final_pack(signals, cfg) 之前插入以下區塊

    # -------------------------
    # Validation / Risk (MVP)
    validation = {
        "breakout_flag": 0,          # 價格是否突破確認
        "divergence_flag": 0,        # 籌碼強但價格未確認（背離警示）
        "confirmation_score": 0.0,   # 0~100：交易有效性總分（簡化版）
    }
    risk = {
        "atr_pct_20d": 0.0,          # 20D ATR%（波動風險）
        "avg_turnover_20d": 0.0,     # 20D 平均成交值（流動性）
        "invalid_flag": 0,           # 失效條件（簡化：跌破SMA20）
    }

    if ohlcv_20d is not None and not ohlcv_20d.empty:
        dfp = ohlcv_20d.copy()

        # --- Validation: breakout_flag ---
        if {"close", "high"}.issubset(dfp.columns):
            close = dfp["close"].astype(float)
            high = dfp["high"].astype(float)

            sma20 = close.rolling(20).mean()
            high20 = high.rolling(20).max()

            # 使用「前一日」high20，避免同日引用造成永遠突破
            if len(dfp) >= 20 and pd.notna(sma20.iloc[-1]) and len(high20) >= 2 and pd.notna(high20.iloc[-2]):
                last_close = float(close.iloc[-1])
                last_sma20 = float(sma20.iloc[-1])
                prev_high20 = float(high20.iloc[-2])

                validation["breakout_flag"] = int(last_close >= last_sma20 and last_close > prev_high20)

        # --- Validation: divergence_flag / confirmation_score ---
        # # 你的籌碼/整合分數
        score_now = float(signals.get("trend_score", 0) or 0)
        validation["divergence_flag"] = int(score_now >= 70 and validation["breakout_flag"] == 0)

        # 簡化版確認分數：breakout 加權 60%，score 加權 40%
        validation["confirmation_score"] = round((60.0 if validation["breakout_flag"] else 0.0) + min(max(score_now, 0.0), 100.0) * 0.4, 1)

        # --- Risk: ATR% ---
        if {"high", "low", "close"}.issubset(dfp.columns):
            high = dfp["high"].astype(float)
            low = dfp["low"].astype(float)
            close = dfp["close"].astype(float)
            prev_close = close.shift(1)

            tr = pd.concat(
                [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
                axis=1
            ).max(axis=1)

            atr20 = tr.rolling(20).mean()
            if len(atr20) > 0 and pd.notna(atr20.iloc[-1]) and float(close.iloc[-1]) != 0:
                risk["atr_pct_20d"] = round(float(atr20.iloc[-1] / close.iloc[-1]), 4)

        # --- Risk: avg_turnover_20d ---
        # 若有 turnover 欄位，直接用；否則用 close*volume 估（需 volume 欄位）
        if "turnover" in dfp.columns:
            risk["avg_turnover_20d"] = float(dfp["turnover"].tail(20).mean())
        elif {"close", "volume"}.issubset(dfp.columns):
            risk["avg_turnover_20d"] = float((dfp["close"].astype(float) * dfp["volume"].astype(float)).tail(20).mean())

        # --- Risk: invalid_flag（簡化：跌破SMA20） ---
        if "close" in dfp.columns:
            close = dfp["close"].astype(float)
            sma20 = close.rolling(20).mean()
            if len(dfp) >= 20 and pd.notna(sma20.iloc[-1]):
                risk["invalid_flag"] = int(float(close.iloc[-1]) < float(sma20.iloc[-1]))

    # 寫回 signals（分層）
    signals["validation"] = validation
    signals["risk"] = risk

    # （可選）過渡期：攤平到 signals，避免你 dashboard 先不用改前端
    signals["breakout_flag"] = validation["breakout_flag"]
    signals["divergence_flag"] = validation["divergence_flag"]
    signals["confirmation_score"] = validation["confirmation_score"]
    signals["atr_pct_20d"] = risk["atr_pct_20d"]
    signals["avg_turnover_20d"] = risk["avg_turnover_20d"]
    signals["invalid_flag"] = risk["invalid_flag"]
    # -------------------------
    # Aggregation（final_score / grade / weights）
    
    # -------------------------
    # Geo: 分點地緣關聯性（Top5 買超分點 vs 公司總部）
    company_map = _load_company_geo_map()
    cmeta = company_map.get(str(stock_id).strip(), {}) or {}
    hq_lat = None
    hq_lon = None
    try:
        hq_lat = float(cmeta.get("lat", "") or 0) if str(cmeta.get("lat","")).strip() else None
        hq_lon = float(cmeta.get("lon", "") or 0) if str(cmeta.get("lon","")).strip() else None
    except Exception:
        hq_lat, hq_lon = None, None

    geo_pack = compute_geo_topn_features(
        top_rows=signals.get("top_buy_15", []) or [],
        hq_lat=hq_lat,
        hq_lon=hq_lon,
        broker_map=broker_map,
        top_n=5,
    )
    # ✅ 這段直接貼在 signals.update(geo_pack) 後面（取代你原本那整段 Geo Gate try 區塊）
    signals.update(geo_pack)

    # --- fallback: build geo_top5_detail / wavg_km / affinity if missing ---
    try:
        from core.geo_utils import haversine_km

        # 1) 先拿 top_buy_15（你已確認有 15）
        top_buy = signals.get("top_buy_15") or []

        # 2) 若 compute_geo_topn_features 沒產出 detail，就自己組
        need_detail = (not isinstance(signals.get("geo_top5_detail"), list)) or (len(signals.get("geo_top5_detail") or []) == 0)

        if need_detail:
            geo_top5 = []

            # 先挑「有座標」的分點，補滿 5 個
            for r in top_buy:
                if not isinstance(r, dict):
                    continue
                bid = str(r.get("broker_id", "")).strip()
                if not bid:
                    continue

                meta = broker_map.get(bid) or {}
                blat = meta.get("lat", None)
                blon = meta.get("lon", None)

                # 沒座標就跳過（否則後面 km 全 None）
                if blat is None or blon is None:
                    continue

                city = str(meta.get("city", "") or "").strip()
                orgt = str(meta.get("broker_org_type", "") or "").strip()

                km = None
                try:
                    if hq_lat is not None and hq_lon is not None:
                        km = float(haversine_km(float(hq_lat), float(hq_lon), float(blat), float(blon)))
                except Exception:
                    km = None

                geo_top5.append({
                    "broker_id": bid,
                    "broker_name": r.get("broker_name") or meta.get("broker_name") or "",
                    "net_lot": float(r.get("net_lot", 0) or 0),
                    "city": city,
                    "broker_org_type": orgt,
                    "lat": blat,
                    "lon": blon,
                    "km_to_hq": km,
                })

                if len(geo_top5) >= 5:
                    break

            signals["geo_top5_detail"] = geo_top5

        # 3) 補 wavg_km（最小可用：平均；要加權再升級）
        if signals.get("geo_top5_wavg_km") is None:
            d = signals.get("geo_top5_detail") or []
            kms = [x.get("km_to_hq") for x in d if isinstance(x, dict) and x.get("km_to_hq") is not None]
            if kms:
                signals["geo_top5_wavg_km"] = round(sum(kms) / len(kms), 2)

        # 4) 補 affinity（最小可用：Top5 同城市佔比 * 100）
        if signals.get("geo_affinity_score") is None:
            d = signals.get("geo_top5_detail") or []
            cities = [str(x.get("city") or "").strip() for x in d if isinstance(x, dict) and str(x.get("city") or "").strip()]
            if cities:
                major = max(set(cities), key=cities.count)
                signals["geo_affinity_score"] = round(100.0 * (cities.count(major) / len(cities)), 1)

    except Exception:
        # 不讓 pipeline 因 geo fallback 失敗而中斷
        pass

    # -------------------------
    # Geo Baseline 精準化 + City Normalize + ZScore 重算 + Grade/Tag（單一版本，避免覆寫）
    # baseline 優先順序：
    # 1) 同 org_type + 同 city（樣本>=30）
    # 2) 同 org_type（樣本>=30）
    # 3) 全體券商（樣本>=30）
    try:
        import statistics
        from core.geo_utils import haversine_km

        # ---- city normalize：避免「臺北市/台北市」拆群，影響 baseline_org_city ----
        def _norm_city(s: str) -> str:
            s = (s or "").strip()
            if not s:
                return ""
            s = s.replace("臺", "台")
            s = s.replace("　", " ").replace("\u3000", " ")
            s = " ".join(s.split())
            return s

        # Top5 買超（用 net_lot 權重選 major org/city）
        top_rows = signals.get("top_buy_15", []) or []
        buys = [r for r in top_rows if float(r.get("net_lot", 0) or 0) > 0][:5]

        def _wmode(rows, key: str):
            acc = {}
            for r in rows:
                w = abs(float(r.get("net_lot", 0) or 0))
                v = str(r.get(key, "") or "").strip()
                if not v:
                    continue
                if key == "city":
                    v = _norm_city(v)
                acc[v] = acc.get(v, 0.0) + w
            if not acc:
                return ""
            return sorted(acc.items(), key=lambda x: x[1], reverse=True)[0][0]

        # 補 top5 的 org/city（若缺就從 broker_map 補；同時 normalize city）
        norm = []
        for r in buys:
            bid = str(r.get("broker_id", "")).strip()
            meta = broker_map.get(bid, {}) if bid else {}
            nr = dict(r)

            if not str(nr.get("broker_org_type", "") or "").strip():
                nr["broker_org_type"] = str(meta.get("broker_org_type", "") or "").strip()

            if not str(nr.get("city", "") or "").strip():
                nr["city"] = _norm_city(str(meta.get("city", "") or ""))
            else:
                nr["city"] = _norm_city(str(nr.get("city", "") or ""))

            norm.append(nr)

        target_org = _wmode(norm, "broker_org_type")    # foreign/local/...
        target_city = _norm_city(_wmode(norm, "city"))  # 台北市/新北市/...

        def _iter_candidates(mode: str):
            for _, meta in (broker_map or {}).items():
                meta = meta or {}
                try:
                    blat = float(str(meta.get("lat", "")).strip()) if str(meta.get("lat", "")).strip() else None
                    blon = float(str(meta.get("lon", "")).strip()) if str(meta.get("lon", "")).strip() else None
                except Exception:
                    blat, blon = None, None
                if blat is None or blon is None:
                    continue

                org = str(meta.get("broker_org_type", "") or "").strip()
                city = _norm_city(str(meta.get("city", "") or ""))

                if mode == "org_city":
                    if (target_org and org != target_org) or (target_city and city != target_city):
                        continue
                elif mode == "org":
                    if target_org and org != target_org:
                        continue

                yield (blat, blon)

        def _baseline_stats(mode: str):
            ds = []
            if hq_lat is None or hq_lon is None:
                return None
            for blat, blon in _iter_candidates(mode):
                d = haversine_km(float(hq_lat), float(hq_lon), float(blat), float(blon))
                if d is not None:
                    ds.append(float(d))
            if len(ds) < 30:
                return None
            mu = statistics.mean(ds)
            sd = statistics.pstdev(ds)
            if sd <= 1e-9:
                sd = 1.0
            return (mu, sd, len(ds))

        # 依序嘗試 baseline
        baseline_tag = "baseline_all"
        baseline_w = 0.70
        chosen = _baseline_stats("org_city")
        if chosen is not None:
            baseline_tag = f"baseline_org_city:{target_org or 'na'}_{target_city or 'na'}"
            baseline_w = 1.00
        else:
            chosen = _baseline_stats("org")
            if chosen is not None:
                baseline_tag = f"baseline_org:{target_org or 'na'}"
                baseline_w = 0.85
            else:
                chosen = _baseline_stats("all")
                baseline_tag = "baseline_all"
                baseline_w = 0.70

        # 重算 geo_zscore（觀測值=geo_top5_wavg_km）
        wavg_km = float(signals.get("geo_top5_wavg_km", 0) or 0)
        if chosen is not None and wavg_km > 0:
            mu, sd, n = chosen
            z = (wavg_km - mu) / sd
            signals["geo_zscore"] = round(z, 2)
            signals["geo_baseline_mu_km"] = round(mu, 2)
            signals["geo_baseline_sd_km"] = round(sd, 2)
            signals["geo_baseline_n"] = int(n)
        else:
            signals["geo_zscore"] = None

        signals["geo_baseline_tag"] = baseline_tag
        signals["geo_baseline_weight"] = round(float(baseline_w), 2)

        # --- Geo Grade/Tag：絕對距離主導 + z 微調（單一版本） ---
        z = signals.get("geo_zscore", None)
        try:
            z = float(z) if z is not None else None
        except Exception:
            z = None

        # A: <=8km / B: 8~15km / C: 15~30km / D: >30km
        if wavg_km > 0 and wavg_km <= 8:
            grade, tag = "A", "geo_near_core"
        elif wavg_km > 8 and wavg_km <= 15:
            grade, tag = "B", "geo_near"
        elif wavg_km > 15 and wavg_km <= 30:
            grade, tag = "C", "geo_mid"
        elif wavg_km > 30:
            grade, tag = "D", "geo_far"
        else:
            grade, tag = "NA", "geo_na"

        # 非常近 or 異常接近 → A
        if grade in ("B", "C", "D"):
            if wavg_km > 0 and wavg_km <= 5:
                grade, tag = "A", "geo_ultra_near"
            elif (wavg_km > 0 and wavg_km <= 12) and (z is not None and z <= -1.2):
                grade, tag = "A", "geo_strong_by_z"

        # z 微調（只一級）
        if z is not None and grade in ("A", "B", "C", "D"):
            if z <= -1.0 and grade != "A":
                grade = {"D": "C", "C": "B", "B": "A"}.get(grade, grade)
                tag = f"{tag}_boost_z"
            elif z >= 1.0 and grade != "D":
                grade = {"A": "B", "B": "C", "C": "D"}.get(grade, grade)
                tag = f"{tag}_penalty_z"

        # baseline tag 附註（可讀性）
        if baseline_tag:
            tag = f"{tag}|{baseline_tag}"

        signals["geo_grade"] = grade
        signals["geo_tag"] = tag

    except Exception:
        # 不擋主流程
        signals.setdefault("geo_zscore", None)
        signals.setdefault("geo_grade", None)
        signals.setdefault("geo_tag", "geo_error")
        signals.setdefault("geo_baseline_tag", "baseline_error")
        signals.setdefault("geo_baseline_weight", 0.70)
    # -------------------------

    # Top6 詳細也補上距離（給 dashboard drill-down）
    if hq_lat is not None and hq_lon is not None:
        for r in top6_details:
            bid = str(r.get("broker_id", "")).strip()
            meta = broker_map.get(bid, {}) or {}
            try:
                blat = float(str(meta.get("lat","")).strip()) if str(meta.get("lat","")).strip() else None
                blon = float(str(meta.get("lon","")).strip()) if str(meta.get("lon","")).strip() else None
            except Exception:
                blat, blon = None, None
            if blat is not None and blon is not None:
                from core.geo_utils import haversine_km
                r["km_to_hq"] = round(haversine_km(hq_lat, hq_lon, blat, blon), 1)
            else:
                r["km_to_hq"] = None
    # -------------------------

    signals.update(compute_final_pack(signals, cfg))

    insight: Insight = {
        "history_labels": [d[5:] for d in date_10d],  # MM-DD
        "whale_data": whale_data,
        "total_whale_values": total_whale_values,
        "top6_details": top6_details,
        "signals": signals,
    }

    boss_list_df = agg_10d.sort_values("net_buy", ascending=False).head(20).reset_index(drop=True)
    boss_list_df.rename(columns={"net_buy": "net"}, inplace=True)
    boss_list_df["net_lot"] = (boss_list_df["net"] / 1000.0).round(1)

    return insight, boss_list_df
