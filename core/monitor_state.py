from __future__ import annotations

from typing import Any, Dict, List


def compute_monitor_state(signals: Dict[str, Any]) -> Dict[str, Any]:
    """
    Whale Trend Monitor - 5 狀態機制
    """
    ez = (signals.get("enhanced") or {})
    st10 = signals.get("top15_buy_stability_10d")
    pr5 = signals.get("pressure_ratio_5d")
    coh_p = ez.get("coherence_persistence_20d")
    coh_s = ez.get("coherence_slope_5d")
    coh_t = ez.get("coherence_today")

    dist = int(signals.get("dist_risk_flag", 0) or 0)
    net5 = float(signals.get("netbuy_5d_lot", 0) or 0)

    breakout = int((signals.get("validation") or {}).get("breakout_flag", 0) or 0)
    tv_score = float(signals.get("tv_score", 0) or 0)

    def _to_float(x):
        try:
            return float(x)
        except Exception:
            return None

    st10_v = _to_float(st10)
    pr5_v = _to_float(pr5)
    coh_pv = _to_float(coh_p)
    coh_sv = _to_float(coh_s)
    coh_tv = _to_float(coh_t)

    # 綜合分數與 Regime，用來判斷「強多 / 弱多」
    score_val = _to_float(
        signals.get(
            "final_score",
            signals.get("score", signals.get("trend_score", 0)),
        )
    ) or 0.0
    regime_score = _to_float(signals.get("regime_score", 50)) or 50.0

    # 失效條件（跌破 SMA20 等）
    risk_pack = signals.get("risk") or {}
    invalid_flag = int(risk_pack.get("invalid_flag", signals.get("invalid_flag", 0)) or 0)

    state = "NEUTRAL"
    reasons: List[str] = []

    # ---- Signals ----
    pressure_sell = (pr5_v is not None and pr5_v <= 0.48 and net5 <= 0)
    coherence_fade = (
        (coh_sv is not None and coh_sv < 0)
        or (coh_tv is not None and coh_tv < 0.9)
    )

    acc_hit = (
        (st10_v is not None and st10_v >= 0.25)
        and (pr5_v is not None and pr5_v >= 0.52)
        and (coh_pv is not None and coh_pv >= 0.20)
        and (dist == 0)
    )

    mk_hit = (
        ((breakout == 1) or (tv_score >= 2.5))
        and (st10_v is not None and st10_v >= 0.20)
        and (score_val >= 60)
    )

    # 強多環境：高分 + regime 強 + 未觸發失效條件
    is_strong_bull = (score_val >= 70 and regime_score >= 60 and invalid_flag == 0)

    # ---- Priority order ----
    # 1) DISTRIBUTION: 必須 dist_risk_on 才叫「派發」
    if dist == 1 and (pressure_sell or net5 <= 0 or invalid_flag == 1):
        state = "DISTRIBUTION"
        reasons.append("dist_risk_on")
        if pressure_sell:
            reasons.append("pressure_sell_dominate")
        if coherence_fade:
            reasons.append("coherence_fading")

    # 2) FADING: 無 dist_risk，但出現退潮訊號，且不是強多環境
    elif (pressure_sell or coherence_fade) and not is_strong_bull:
        state = "FADING"
        if pressure_sell:
            reasons.append("pressure_sell_dominate")
        if coherence_fade:
            reasons.append("coherence_fading")

    # 3) MARKUP: 價格行為確認（但沒有退潮/派發）
    elif mk_hit:
        state = "MARKUP"
        reasons.append("price_confirmed")

    # 4) ACCUMULATION: 同一批人持續累積（但未進入 markup）
    elif acc_hit:
        state = "ACCUMULATION"
        reasons += ["stability_ok", "pressure_buy", "coherence_persist"]

    # 5) NEUTRAL
    else:
        state = "NEUTRAL"
        reasons.append("neutral")

    signals["monitor_state"] = state
    signals["monitor_reasons"] = reasons[:6] if reasons else ["neutral"]
    return signals


