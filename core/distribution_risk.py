from __future__ import annotations

import math
from typing import Any, Dict, Tuple

import pandas as pd


def _hhi_entropy(df: pd.DataFrame | None, side: str = "buy", top_n: int = 15) -> Tuple[Any, Any]:
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


def compute_distribution_risk(signals: Dict[str, Any], df_20d: pd.DataFrame) -> Dict[str, Any]:
    """
    計算 20D 集中度 HHI / Entropy 與派發風險 dist_risk_flag/dist_risk_tag
    """
    buy_hhi, buy_ent = _hhi_entropy(df_20d, "buy", 15)
    sell_hhi, sell_ent = _hhi_entropy(df_20d, "sell", 15)

    signals["buy_hhi_20d"] = buy_hhi
    signals["buy_entropy_20d"] = buy_ent
    signals["sell_hhi_20d"] = sell_hhi
    signals["sell_entropy_20d"] = sell_ent

    net1 = float(signals.get("netbuy_1d_lot", 0) or 0)
    net5 = float(signals.get("netbuy_5d_lot", 0) or 0)

    dist_flag = 0
    reasons: list[str] = []

    if buy_hhi is not None and buy_hhi >= 0.22 and net5 <= 0:
        dist_flag = 1
        reasons.append("buy_hhi_high_and_net5_weak")
    if sell_hhi is not None and sell_hhi >= 0.22 and net1 < 0:
        dist_flag = 1
        reasons.append("sell_hhi_high_and_net1_negative")

    signals["dist_risk_flag"] = int(dist_flag)
    signals["dist_risk_tag"] = "|".join(reasons) if reasons else ""
    return signals

