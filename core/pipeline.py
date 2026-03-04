# core/pipeline.py
import pandas as pd
import math

from core.signals.validation import compute_validation_signals
from core.signals.risk import compute_risk_signals

from core.signals.whale import (
    standardize_columns,
    compute_concentration,
    calc_breadth,
    compute_foreign_local_net,
    build_top15_tables,
    build_breadth_series,
    compute_master_trend,
)
from core.io.price_data import fetch_ohlcv_20d, fetch_price_nd
from core.signals.tv import compute_tv_radar_signals
from core.signals.regime import compute_regime_signals

from core.config import PipelineConfig
from core.aggregate import compute_final_pack
from core.types import Insight
from core.signals.geo import compute_geo_signals, load_company_geo_map as _load_company_geo_map
from core.signals.enhanced import compute_enhanced_signals
from core.signals.distribution import compute_distribution_risk
from core.signals.monitor import compute_monitor_state
from core.signals.whale_extras import compute_turning_points, compute_whale_radar


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

    signals: dict = {
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
    tp = compute_turning_points(
        df_20d=df_20d,
        df_1d=df_1d,
        fl5=fl5,
        top6_ids=top6_ids,
        top6_details=top6_details,
        date_20d=date_20d,
        last_1d=last_1d,
        price_df_tail=price_df_tail,
    )
    signals["turning_points"] = tp

    # ----------------------------------------------------------
    # Whale Radar (0~100)
    signals["whale_radar"] = compute_whale_radar(signals, debug=debug_tv)

    # ----------------------------------------------------------
    # Enhanced (single source of truth) - coherence + cost zone + stats + streak
    signals = compute_enhanced_signals(
        signals=signals,
        df_20d=df_20d,
        df_5d=df_5d,
        date_20d=date_20d,
        top6_details=top6_details,
        top6_ids=top6_ids,
        ohlcv_20d=ohlcv_20d,
    )

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
    # Validation / Risk (MVP) - 使用專用模組
    validation = compute_validation_signals(price_df=ohlcv_20d, signals=signals, cfg=cfg)
    risk = compute_risk_signals(price_df=ohlcv_20d, signals=signals, cfg=cfg)

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
    signals, top6_details = compute_geo_signals(
        signals=signals,
        broker_map=broker_map,
        stock_id=stock_id,
        top6_details=top6_details,
    )

    # -------------------------
    # HHI / Entropy + distribution risk tag
    signals = compute_distribution_risk(signals=signals, df_20d=df_20d)

    # === Whale Trend Monitor (long-term) - 5 states ===
    signals = compute_monitor_state(signals)

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