# core/pipeline.py
import pandas as pd
import math

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
def _load_company_geo_map(
    tse_csv: str = "./rawdata/TSE_Company_V2.csv",
    otc_csv: str = "./rawdata/OTC_Company_V2.csv",
) -> dict:
    """
    讀上市/上櫃公司地址與經緯度，回傳 stock_id -> {lat, lon, address, name}
    預設路徑可依你的專案：C:\\ngrok\\RB_DataMining\\rawdata\\...
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
    combined["org"] = combined["broker_id"].map(
        lambda x: (broker_map.get(x, {}).get("broker_org_type", "unknown") or "unknown")
    )

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
    from core.signals_whale import compute_streaks

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

        top6_details.append(
            {
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
            }
        )

    # Top6 軌跡矩陣（累積 net）
    whale_detail = df_10d[df_10d["broker_id"].isin(top6_ids)].copy()
    pivot_net = whale_detail.pivot_table(index="date", columns="broker_name", values="net", aggfunc="sum").fillna(0)
    pivot_cumsum = pivot_net.reindex(date_10d).fillna(0).cumsum()

    colors = ["#FF6384", "#36A2EB", "#FFCE56", "#4BC0C0", "#9966FF", "#FF9F40"]
    whale_data = []
    for i, name in enumerate(pivot_cumsum.columns):
        whale_data.append(
            {
                "name": name,
                "values": (pivot_cumsum[name] / 1000.0).round(1).tolist(),
                "color": colors[i % len(colors)],
            }
        )
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

    def _clamp(x, lo, hi):
        return max(lo, min(hi, x))

    def _tanh_0_100(x):  # x=0 -> 50；正向上升 -> 越接近100；負向 -> 越接近0
        return round(50.0 + 50.0 * math.tanh(x), 1)

    # === A. ΔMajorFlow20：20日買方 Top15 張數 - 賣方 Top15 張數 ===
    buy_sum_20 = 0.0
    sell_sum_20 = 0.0

    if "top_buy_15" in top15_pack and top15_pack["top_buy_15"]:
        buy_sum_20 = sum([float(r["net_lot"]) for r in top15_pack["top_buy_15"]])

    if "top_sell_15" in top15_pack and top15_pack["top_sell_15"]:
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

    # ---- 壓力比 ----
    def _safe_float(x, default=0.0):
        try:
            return float(x)
        except Exception:
            return default

    buy_sum_20 = _safe_float(signals.get("top15_buy_sum_20", 0.0), 0.0)
    sell_sum_20 = _safe_float(signals.get("top15_sell_sum_20", 0.0), 0.0)
    denom_20 = (buy_sum_20 + sell_sum_20) if (buy_sum_20 + sell_sum_20) > 0 else 0.0
    signals["pressure_ratio_20d"] = round((buy_sum_20 / denom_20), 4) if denom_20 > 0 else None
    signals["net_pressure_20d"] = round(((buy_sum_20 - sell_sum_20) / denom_20), 4) if denom_20 > 0 else None

    buy_sum_5 = _safe_float(signals.get("top15_buy_sum_5", 0.0), 0.0)
    sell_sum_5 = _safe_float(signals.get("top15_sell_sum_5", 0.0), 0.0)
    denom_5 = (buy_sum_5 + sell_sum_5) if (buy_sum_5 + sell_sum_5) > 0 else 0.0
    signals["pressure_ratio_5d"] = round((buy_sum_5 / denom_5), 4) if denom_5 > 0 else None
    signals["net_pressure_5d"] = round(((buy_sum_5 - sell_sum_5) / denom_5), 4) if denom_5 > 0 else None

    # ---- 名單穩定度（Jaccard）----
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

    try:
        sets_20 = [_topn_buy_set_by_day(df_20d, d, top_n=15) for d in date_20d]
        jac_20 = []
        for i in range(1, len(sets_20)):
            v = _jaccard(sets_20[i - 1], sets_20[i])
            if v is not None:
                jac_20.append(v)
        signals["top15_buy_stability_20d"] = round(float(sum(jac_20) / len(jac_20)), 4) if jac_20 else None

        date_10 = date_20d[-10:] if len(date_20d) >= 10 else date_20d[:]
        sets_10 = [_topn_buy_set_by_day(df_20d, d, top_n=15) for d in date_10]
        jac_10 = []
        for i in range(1, len(sets_10)):
            v = _jaccard(sets_10[i - 1], sets_10[i])
            if v is not None:
                jac_10.append(v)
        signals["top15_buy_stability_10d"] = round(float(sum(jac_10) / len(jac_10)), 4) if jac_10 else None

        sizes_10 = [len(s) for s in sets_10 if s is not None]
        signals["top15_buy_avg_size_10d"] = round(float(sum(sizes_10) / len(sizes_10)), 2) if sizes_10 else None

    except Exception:
        signals["top15_buy_stability_20d"] = None
        signals["top15_buy_stability_10d"] = None
        signals["top15_buy_avg_size_10d"] = None

    # ----------------------------------------------------------
    # TV Radar（短期價格行為）
    ohlcv_20d = fetch_ohlcv_20d(adapter, stock_id, date_20d, debug=debug_tv)
    price_df = ohlcv_20d if ohlcv_20d is not None else pd.DataFrame()
    price_df_tail = price_df

    # === B. 主力拐點偵測 Turning Points ===
    tp = {}

    foreign_5 = float(fl5.get("foreign_net", 0.0))
    local_5 = float(fl5.get("local_net", 0.0))
    foreign_today = 0.0
    local_today = 0.0

    if not df_1d.empty and "org" in df_1d.columns:
        foreign_today = float(df_1d[df_1d["org"] == "foreign"]["net"].sum())
        local_today = float(df_1d[df_1d["org"] == "local"]["net"].sum())

    tp["foreign_buy_switch"] = int(foreign_5 < 0 and foreign_today > 0)
    tp["local_buy_switch"] = int(local_5 < 0 and local_today > 0)

    tp["whale_reversal"] = 0
    for bid in top6_ids:
        seq = df_20d[df_20d["broker_id"] == bid].sort_values("date")["net"].tolist()
        if len(seq) >= 4:
            if seq[-4] < 0 and seq[-3] < 0 and seq[-2] < 0 and seq[-1] > 0:
                tp["whale_reversal"] = 1
                break

    mean_buy20 = df_20d["buy"].mean()
    today_buy = df_20d[df_20d["date"] == last_1d]["buy"].sum()
    tp["abnormal_buy_spike"] = int(today_buy >= mean_buy20 * 3)

    avg_prices = [float(r["avg_price"]) for r in top6_details if r.get("avg_price", 0) > 0]
    if avg_prices:
        cost_low = min(avg_prices)
        close_recent = None
        if price_df_tail is not None and not price_df_tail.empty and "close" in price_df_tail.columns:
            close_recent = float(price_df_tail["close"].tail(3).min())
        tp["cost_zone_defended"] = int(close_recent is not None and close_recent >= cost_low)
    else:
        tp["cost_zone_defended"] = 0

    signals["turning_points"] = tp

    # ----------------------------------------------------------
    # Whale Radar (0~100)
    c20_val = float(signals.get("concentration_20d", 0) or 0)
    c20_score = round(_clamp(c20_val, 0, 100), 1)

    net1 = float(signals.get("netbuy_1d_lot", 0) or 0)
    net5 = float(signals.get("netbuy_5d_lot", 0) or 0)
    net20 = float(signals.get("netbuy_20d_lot", 0) or 0)

    scale = max(10.0, abs(net20) / 4.0)
    net_dir_score = _tanh_0_100(net5 / scale)

    br5 = float(signals.get("breadth_ratio_5d", 0) or 0)
    breadth_score = round(_clamp(br5, 0, 1) * 100.0, 1)

    f5 = float(signals.get("foreign_net_5d", 0) or 0)
    l5 = float(signals.get("local_net_5d", 0) or 0)
    same_sign = (f5 == 0 and l5 == 0) or (f5 > 0 and l5 > 0) or (f5 < 0 and l5 < 0)

    mag = abs(f5) + abs(l5)
    mag_score = _clamp(mag / max(1.0, mag + 20000.0), 0, 1)
    base = 65.0 if same_sign else 35.0
    align_score = round(_clamp(base + 35.0 * mag_score, 0, 100), 1)

    avg5 = net5 / 5.0
    den = max(5.0, abs(avg5))
    acc_score = _tanh_0_100(net1 / den)

    signals["whale_radar"] = {
        "labels": ["集中度", "淨買方向", "買盤廣度", "外本協同", "短期加速"],
        "values": [c20_score, net_dir_score, breadth_score, align_score, acc_score],
        "debug": {
            "c20": c20_val,
            "net1": net1,
            "net5": net5,
            "net20": net20,
            "scale": scale,
            "breadth_ratio_5d": br5,
            "foreign_5d": f5,
            "local_5d": l5,
            "same_sign": int(same_sign),
            "avg5": avg5,
            "den": den,
        },
    }
    print("🔎 whale_radar(pipeline)=", signals["whale_radar"])

    # ----------------------------------------------------------
    # Enhanced (single source of truth) - coherence + cost zone + stats + streak
    enhanced = signals.get("enhanced", {}) or {}

    # === Coherence (upgrade to series) ===
    buy_cnt_series = []
    for d in date_20d:
        dd = df_20d[df_20d["date"] == d]
        gd = dd.groupby("broker_id")["net"].sum()
        buy_cnt_series.append(int((gd > 0).sum()))

    avg_buy_cnt = float(sum(buy_cnt_series) / len(buy_cnt_series)) if buy_cnt_series else 1.0
    avg_buy_cnt = max(avg_buy_cnt, 1.0)

    coh_series = [round(min(max((x / avg_buy_cnt), 0.0), 3.0), 2) for x in buy_cnt_series]
    coh_today = coh_series[-1] if coh_series else 0.0

    def _slope(y):
        n = len(y)
        if n < 2:
            return 0.0
        xs = list(range(n))
        xbar = sum(xs) / n
        ybar = sum(y) / n
        num = sum((xs[i] - xbar) * (y[i] - ybar) for i in range(n))
        den = sum((xs[i] - xbar) ** 2 for i in range(n)) or 1.0
        return num / den

    enhanced["coherence_today"] = round(coh_today, 2)
    enhanced["buy_cnt_today"] = int(buy_cnt_series[-1]) if buy_cnt_series else 0
    enhanced["avg_buy_cnt_20d"] = round(avg_buy_cnt, 1)
    enhanced["coherence_series_20d"] = coh_series
    enhanced["coherence_slope_5d"] = round(_slope(coh_series[-5:]), 3) if len(coh_series) >= 5 else None
    enhanced["coherence_persistence_20d"] = (
        round(sum(1 for v in coh_series if v >= 1.2) / len(coh_series), 3) if coh_series else None
    )
    enhanced["coherence_max_20d"] = max(coh_series) if coh_series else None

    # === Cost Zone Range + deviation + status ===
    avg_prices = [float(r["avg_price"]) for r in top6_details if r.get("avg_price", 0) > 0]
    if avg_prices and ohlcv_20d is not None and not ohlcv_20d.empty:
        cost_low = float(min(avg_prices))
        cost_high = float(max(avg_prices))
        enhanced["cost_low"] = round(cost_low, 2)
        enhanced["cost_high"] = round(cost_high, 2)

        close_last = float(ohlcv_20d.iloc[-1]["close"])
        enhanced["close_last"] = close_last
        enhanced["cost_deviation"] = round((close_last - cost_low) / cost_low, 3) if cost_low > 0 else 0.0

        if close_last > cost_high:
            enhanced["cost_zone_status"] = "above_zone"
        elif close_last >= cost_low:
            enhanced["cost_zone_status"] = "inside_zone"
        else:
            enhanced["cost_zone_status"] = "below_zone"
    else:
        enhanced["cost_zone_status"] = "unknown"

    # === Cost Zone statistics (20D) ===
    if avg_prices and ohlcv_20d is not None and not ohlcv_20d.empty:
        cost_low = float(min(avg_prices))
        cost_high = float(max(avg_prices))
        close = ohlcv_20d["close"].astype(float).tolist()

        st = []
        for c in close:
            if c > cost_high:
                st.append(2)
            elif c >= cost_low:
                st.append(1)
            else:
                st.append(0)

        enhanced["cz_touch_20d"] = int(sum(1 for s in st if s == 1))

        cz_up = 0
        cz_dn = 0
        for i in range(1, len(st)):
            if st[i - 1] in (0, 1) and st[i] == 2:
                cz_up += 1
            if st[i - 1] in (1, 2) and st[i] == 0:
                cz_dn += 1

        enhanced["cz_break_up_20d"] = int(cz_up)
        enhanced["cz_break_dn_20d"] = int(cz_dn)

        defense_trials = 0
        defense_wins = 0
        for i in range(len(st)):
            if st[i] == 0:
                defense_trials += 1
                horizon = st[i + 1 : i + 4]
                if any(s in (1, 2) for s in horizon):
                    defense_wins += 1

        enhanced["cz_defense_win_20d"] = int(defense_wins)
        enhanced["cz_defense_rate_20d"] = round(defense_wins / defense_trials, 3) if defense_trials > 0 else None

    # === streak_strength ===
    streak_strength = 0.0
    for r in top6_details:
        sb = int(r.get("streak_buy", 0))
        if sb <= 0:
            continue
        bid = r["broker_id"]
        df_b = df_5d[df_5d["broker_id"] == bid]
        net5_lot = float(df_b["net"].sum()) / 1000.0

        total_net5_lot = float(df_5d["net"].sum()) / 1000.0
        avg5_lot = total_net5_lot / len(top6_ids) if len(top6_ids) > 0 else 0.0

        if avg5_lot > 0:
            strength = (math.log(sb + 1)) * (net5_lot / avg5_lot)
            streak_strength = max(streak_strength, strength)

    enhanced["streak_strength"] = round(streak_strength, 3)

    # store once
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
    signals["score"] = signals["trend_score"]
    signals["score_unified"] = signals["trend_score"]

    # ---------------------------------
    def norm_pct(x):
        try:
            v = float(x or 0)
        except Exception:
            return 0.0
        if 0 <= v <= 1:
            v *= 100.0
        return max(0.0, min(100.0, v))

    # -------------------------
    # Validation / Risk (MVP)
    validation = {
        "breakout_flag": 0,
        "divergence_flag": 0,
        "confirmation_score": 0.0,
    }
    risk = {
        "atr_pct_20d": 0.0,
        "avg_turnover_20d": 0.0,
        "invalid_flag": 0,
    }

    if ohlcv_20d is not None and not ohlcv_20d.empty:
        dfp = ohlcv_20d.copy()

        if {"close", "high"}.issubset(dfp.columns):
            close = dfp["close"].astype(float)
            high = dfp["high"].astype(float)

            sma20 = close.rolling(20).mean()
            high20 = high.rolling(20).max()

            if len(dfp) >= 20 and pd.notna(sma20.iloc[-1]) and len(high20) >= 2 and pd.notna(high20.iloc[-2]):
                last_close = float(close.iloc[-1])
                last_sma20 = float(sma20.iloc[-1])
                prev_high20 = float(high20.iloc[-2])

                validation["breakout_flag"] = int(last_close >= last_sma20 and last_close > prev_high20)

        score_now = float(signals.get("trend_score", 0) or 0)
        validation["divergence_flag"] = int(score_now >= 70 and validation["breakout_flag"] == 0)
        validation["confirmation_score"] = round(
            (60.0 if validation["breakout_flag"] else 0.0) + min(max(score_now, 0.0), 100.0) * 0.4, 1
        )

        if {"high", "low", "close"}.issubset(dfp.columns):
            high = dfp["high"].astype(float)
            low = dfp["low"].astype(float)
            close = dfp["close"].astype(float)
            prev_close = close.shift(1)

            tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
            atr20 = tr.rolling(20).mean()
            if len(atr20) > 0 and pd.notna(atr20.iloc[-1]) and float(close.iloc[-1]) != 0:
                risk["atr_pct_20d"] = round(float(atr20.iloc[-1] / close.iloc[-1]), 4)

        if "turnover" in dfp.columns:
            risk["avg_turnover_20d"] = float(dfp["turnover"].tail(20).mean())
        elif {"close", "volume"}.issubset(dfp.columns):
            risk["avg_turnover_20d"] = float((dfp["close"].astype(float) * dfp["volume"].astype(float)).tail(20).mean())

        if "close" in dfp.columns:
            close = dfp["close"].astype(float)
            sma20 = close.rolling(20).mean()
            if len(dfp) >= 20 and pd.notna(sma20.iloc[-1]):
                risk["invalid_flag"] = int(float(close.iloc[-1]) < float(sma20.iloc[-1]))

    signals["validation"] = validation
    signals["risk"] = risk

    signals["breakout_flag"] = validation["breakout_flag"]
    signals["divergence_flag"] = validation["divergence_flag"]
    signals["confirmation_score"] = validation["confirmation_score"]
    signals["atr_pct_20d"] = risk["atr_pct_20d"]
    signals["avg_turnover_20d"] = risk["avg_turnover_20d"]
    signals["invalid_flag"] = risk["invalid_flag"]

    # -------------------------
    # Geo: 分點地緣關聯性（Top5 買超分點 vs 公司總部）
    company_map = _load_company_geo_map()
    cmeta = company_map.get(str(stock_id).strip(), {}) or {}
    hq_lat = None
    hq_lon = None
    try:
        hq_lat = float(cmeta.get("lat", "") or 0) if str(cmeta.get("lat", "")).strip() else None
        hq_lon = float(cmeta.get("lon", "") or 0) if str(cmeta.get("lon", "")).strip() else None
    except Exception:
        hq_lat, hq_lon = None, None

    geo_pack = compute_geo_topn_features(
        top_rows=signals.get("top_buy_15", []) or [],
        hq_lat=hq_lat,
        hq_lon=hq_lon,
        broker_map=broker_map,
        top_n=5,
    )
    signals.update(geo_pack)

    # --- fallback: build geo_top5_detail / wavg_km / affinity if missing ---
    try:
        from core.geo_utils import haversine_km

        top_buy = signals.get("top_buy_15") or []
        need_detail = (not isinstance(signals.get("geo_top5_detail"), list)) or (
            len(signals.get("geo_top5_detail") or []) == 0
        )

        if need_detail:
            geo_top5 = []
            for r in top_buy:
                if not isinstance(r, dict):
                    continue
                bid = str(r.get("broker_id", "")).strip()
                if not bid:
                    continue

                meta = broker_map.get(bid) or {}
                blat = meta.get("lat", None)
                blon = meta.get("lon", None)

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

                geo_top5.append(
                    {
                        "broker_id": bid,
                        "broker_name": r.get("broker_name") or meta.get("broker_name") or "",
                        "net_lot": float(r.get("net_lot", 0) or 0),
                        "city": city,
                        "broker_org_type": orgt,
                        "lat": blat,
                        "lon": blon,
                        "km_to_hq": km,
                    }
                )

                if len(geo_top5) >= 5:
                    break

            signals["geo_top5_detail"] = geo_top5

        if signals.get("geo_top5_wavg_km") is None:
            d = signals.get("geo_top5_detail") or []
            kms = [x.get("km_to_hq") for x in d if isinstance(x, dict) and x.get("km_to_hq") is not None]
            if kms:
                signals["geo_top5_wavg_km"] = round(sum(kms) / len(kms), 2)

        if signals.get("geo_affinity_score") is None:
            d = signals.get("geo_top5_detail") or []
            cities = [
                str(x.get("city") or "").strip()
                for x in d
                if isinstance(x, dict) and str(x.get("city") or "").strip()
            ]
            if cities:
                major = max(set(cities), key=cities.count)
                signals["geo_affinity_score"] = round(100.0 * (cities.count(major) / len(cities)), 1)

    except Exception:
        pass

    # -------------------------
    # Geo Baseline + ZScore + Grade/Tag
    try:
        import statistics
        from core.geo_utils import haversine_km

        def _norm_city(s: str) -> str:
            s = (s or "").strip()
            if not s:
                return ""
            s = s.replace("臺", "台")
            s = s.replace("　", " ").replace("\u3000", " ")
            s = " ".join(s.split())
            return s

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

        target_org = _wmode(norm, "broker_org_type")
        target_city = _norm_city(_wmode(norm, "city"))

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

        z = signals.get("geo_zscore", None)
        try:
            z = float(z) if z is not None else None
        except Exception:
            z = None

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

        if grade in ("B", "C", "D"):
            if wavg_km > 0 and wavg_km <= 5:
                grade, tag = "A", "geo_ultra_near"
            elif (wavg_km > 0 and wavg_km <= 12) and (z is not None and z <= -1.2):
                grade, tag = "A", "geo_strong_by_z"

        if z is not None and grade in ("A", "B", "C", "D"):
            if z <= -1.0 and grade != "A":
                grade = {"D": "C", "C": "B", "B": "A"}.get(grade, grade)
                tag = f"{tag}_boost_z"
            elif z >= 1.0 and grade != "D":
                grade = {"A": "B", "B": "C", "C": "D"}.get(grade, grade)
                tag = f"{tag}_penalty_z"

        if baseline_tag:
            tag = f"{tag}|{baseline_tag}"

        signals["geo_grade"] = grade
        signals["geo_tag"] = tag

    except Exception:
        signals.setdefault("geo_zscore", None)
        signals.setdefault("geo_grade", None)
        signals.setdefault("geo_tag", "geo_error")
        signals.setdefault("geo_baseline_tag", "baseline_error")
        signals.setdefault("geo_baseline_weight", 0.70)

    # Top6 詳細也補上距離（給 dashboard drill-down）
    if hq_lat is not None and hq_lon is not None:
        for r in top6_details:
            bid = str(r.get("broker_id", "")).strip()
            meta = broker_map.get(bid, {}) or {}
            try:
                blat = float(str(meta.get("lat", "")).strip()) if str(meta.get("lat", "")).strip() else None
                blon = float(str(meta.get("lon", "")).strip()) if str(meta.get("lon", "")).strip() else None
            except Exception:
                blat, blon = None, None
            if blat is not None and blon is not None:
                from core.geo_utils import haversine_km

                r["km_to_hq"] = round(haversine_km(hq_lat, hq_lon, blat, blon), 1)
            else:
                r["km_to_hq"] = None

    # -------------------------
    # HHI / Entropy + risk tag
    def _hhi_entropy(df, side="buy", top_n=15):
        if df is None or df.empty:
            return (None, None)
        g = df.groupby("broker_id", as_index=False)["net"].sum()
        g["net"] = pd.to_numeric(g["net"], errors="coerce").fillna(0.0)

        if side == "buy":
            g = g[g["net"] > 0].sort_values("net", ascending=False).head(top_n)
            w = g["net"].astype(float).tolist()
        else:
            g = g[g["net"] < 0].sort_values("net", ascending=True).head(top_n)
            w = [abs(float(x)) for x in g["net"].tolist()]

        s = sum(w)
        if s <= 0:
            return (None, None)

        p = [x / s for x in w if x > 0]
        hhi = sum(pi * pi for pi in p)
        ent = -sum(pi * math.log(pi + 1e-12) for pi in p)
        return (round(hhi, 4), round(ent, 4))

    buy_hhi, buy_ent = _hhi_entropy(df_20d, "buy", 15)
    sell_hhi, sell_ent = _hhi_entropy(df_20d, "sell", 15)

    signals["buy_hhi_20d"] = buy_hhi
    signals["buy_entropy_20d"] = buy_ent
    signals["sell_hhi_20d"] = sell_hhi
    signals["sell_entropy_20d"] = sell_ent

    net1 = float(signals.get("netbuy_1d_lot", 0) or 0)
    net5 = float(signals.get("netbuy_5d_lot", 0) or 0)

    dist_flag = 0
    reasons = []

    if buy_hhi is not None and buy_hhi >= 0.22 and net5 <= 0:
        dist_flag = 1
        reasons.append("buy_hhi_high_and_net5_weak")
    if sell_hhi is not None and sell_hhi >= 0.22 and net1 < 0:
        dist_flag = 1
        reasons.append("sell_hhi_high_and_net1_negative")

    signals["dist_risk_flag"] = int(dist_flag)
    signals["dist_risk_tag"] = "|".join(reasons) if reasons else ""

    # === Whale Trend Monitor (long-term) ===
    ez = (signals.get("enhanced") or {})
    st10 = signals.get("top15_buy_stability_10d")
    pr5  = signals.get("pressure_ratio_5d")
    coh_p = ez.get("coherence_persistence_20d")
    coh_s = ez.get("coherence_slope_5d")
    coh_t = ez.get("coherence_today")

    dist = int(signals.get("dist_risk_flag", 0) or 0)
    net5 = float(signals.get("netbuy_5d_lot", 0) or 0)

    breakout = int((signals.get("validation") or {}).get("breakout_flag", 0) or 0)
    tv_score = float(signals.get("tv_score", 0) or 0)

    def _is_num(x):
        try:
            float(x)
            return True
        except Exception:
            return False

    # normalize missing
    st10_v = float(st10) if _is_num(st10) else None
    pr5_v  = float(pr5)  if _is_num(pr5)  else None
    coh_pv = float(coh_p) if _is_num(coh_p) else None
    coh_sv = float(coh_s) if _is_num(coh_s) else None
    coh_tv = float(coh_t) if _is_num(coh_t) else None

    state = "NEUTRAL"
    reasons = []

    # Distribution first (risk overrides)
    dist_hit = False
    if dist == 1:
        dist_hit = True
        reasons.append("dist_risk_on")
    if pr5_v is not None and pr5_v <= 0.45 and net5 <= 0:
        dist_hit = True
        reasons.append("pressure_sell_dominate")
    if coh_sv is not None and coh_tv is not None and coh_sv < 0 and coh_tv < 1.0:
        dist_hit = True
        reasons.append("coherence_fading")

    if dist_hit:
        state = "DISTRIBUTION"
    else:
        acc_hit = (
            (st10_v is not None and st10_v >= 0.35) and
            (pr5_v is not None and pr5_v >= 0.55) and
            (coh_pv is not None and coh_pv >= 0.25) and
            (dist == 0)
        )
        if acc_hit:
            state = "ACCUMULATION"
            reasons += ["stability_ok", "pressure_buy", "coherence_persist"]

        mk_hit = ((breakout == 1) or (tv_score >= 3.0)) and (st10_v is not None and st10_v >= 0.25)
        if mk_hit:
            state = "MARKUP"
            reasons += ["price_confirmed"]

    signals["monitor_state"] = state
    signals["monitor_reasons"] = reasons[:6]

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