from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple

import pandas as pd


def compute_enhanced_signals(
    signals: Dict[str, Any],
    df_20d: pd.DataFrame,
    df_5d: pd.DataFrame,
    date_20d: List[str],
    top6_details: List[Dict[str, Any]],
    top6_ids: List[str],
    ohlcv_20d: pd.DataFrame | None,
) -> Dict[str, Any]:
    """
    Enhanced 指標（coherence / cost zone / streak_strength...）
    - 不改變輸出 schema，只把 pipeline 內現有邏輯搬出來
    """
    enhanced = signals.get("enhanced", {}) or {}

    # === Coherence (upgrade to series) ===
    buy_cnt_series: List[int] = []
    for d in date_20d:
        dd = df_20d[df_20d["date"] == d]
        gd = dd.groupby("broker_id")["net"].sum()
        buy_cnt_series.append(int((gd > 0).sum()))

    avg_buy_cnt = float(sum(buy_cnt_series) / len(buy_cnt_series)) if buy_cnt_series else 1.0
    avg_buy_cnt = max(avg_buy_cnt, 1.0)

    coh_series = [round(min(max((x / avg_buy_cnt), 0.0), 3.0), 2) for x in buy_cnt_series]
    coh_today = coh_series[-1] if coh_series else 0.0

    def _slope(y: List[float]) -> float:
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
        close_list = ohlcv_20d["close"].astype(float).tolist()

        st: List[int] = []
        for c in close_list:
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
        enhanced["cz_defense_rate_20d"] = (
            round(defense_wins / defense_trials, 3) if defense_trials > 0 else None
        )

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

    signals["enhanced"] = enhanced
    return signals

