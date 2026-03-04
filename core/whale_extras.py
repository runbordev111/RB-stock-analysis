from __future__ import annotations

import math
from typing import Any, Dict, List

import pandas as pd


def compute_turning_points(
    df_20d: pd.DataFrame,
    df_1d: pd.DataFrame,
    fl5: Dict[str, Any],
    top6_ids: List[str],
    top6_details: List[Dict[str, Any]],
    date_20d: List[str],
    last_1d: str,
    price_df_tail: pd.DataFrame | None,
) -> Dict[str, Any]:
    """
    主力拐點訊號：
      - foreign_buy_switch / local_buy_switch
      - whale_reversal
      - abnormal_buy_spike
      - cost_zone_defended
    """
    tp: Dict[str, Any] = {}

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

    return tp


def compute_whale_radar(signals: Dict[str, Any], debug: bool = False) -> Dict[str, Any]:
    """
    Whale Radar (0~100)：
      - 集中度、淨買方向、買盤廣度、外本協同、短期加速
    """

    def _clamp(x: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, x))

    def _tanh_0_100(x: float) -> float:
        # x=0 -> 50；正向上升 -> 越接近100；負向 -> 越接近0
        return round(50.0 + 50.0 * math.tanh(x), 1)

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

    radar = {
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

    if debug:
        print("🔎 whale_radar=", radar)

    return radar

