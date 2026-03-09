from typing import Any, Dict, List


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def apply_broker_archetype(top6_details: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    根據 Top6 近 10 日淨買與連買/連賣特徵，給每個券商一個簡單的「波段主力傾向」分數。

    - archetype_wave_score: 0~1，愈高代表愈像「波段主力」：
        - 近 10 日累積 net_10d 愈大（>0）愈高
        - 連買天數 streak_buy 愈長愈高
        - 連賣天數 streak_sell 愈短愈高
    - archetype_label:
        - swing_leader: 波段主力型（wave_score >= 0.7）
        - distributor: 連賣+大幅賣超
        - flipper: 進出頻繁但淨額接近 0 的短線型
        - mixed: 其他情況

    回傳：
      {
        "signals": {
          "top6_avg_wave_score": float | None,
          "top6_max_wave_score": float | None,
          "top6_wave_leader_id": str | None,
          "top6_wave_leader_name": str | None,
        },
        "top6_details": [... 更新後的列表 ...],
      }
    """
    if not top6_details:
        return {"signals": {}, "top6_details": top6_details}

    scores: List[float] = []

    for r in top6_details:
        try:
            net10 = float(r.get("net_10d", 0) or 0.0)
        except Exception:
            net10 = 0.0
        try:
            sb = int(r.get("streak_buy", 0) or 0)
        except Exception:
            sb = 0
        try:
            ss = int(r.get("streak_sell", 0) or 0)
        except Exception:
            ss = 0

        # 只對買超部分給正向分數；淨賣超不加分
        net_score = 0.0
        if net10 > 0:
            # 假設 10k 張以上屬於非常強的累積；以千張為單位
            net_score = _clamp(net10 / 10.0, 0.0, 1.0)

        streak_buy_norm = _clamp(sb / 10.0, 0.0, 1.0)
        streak_sell_norm = _clamp(ss / 5.0, 0.0, 1.0)

        wave_score = 0.5 * net_score + 0.3 * streak_buy_norm + 0.2 * (1.0 - streak_sell_norm)
        wave_score = round(_clamp(wave_score, 0.0, 1.0), 3)

        # 簡單的 archetype 標籤
        if wave_score >= 0.7:
            label = "swing_leader"
        elif net10 < 0 and ss >= 3:
            label = "distributor"
        elif abs(net10) < 1 and sb <= 1 and ss <= 1:
            label = "flipper"
        else:
            label = "mixed"

        r["archetype_wave_score"] = wave_score
        r["archetype_label"] = label
        scores.append(wave_score)

    if scores:
        avg_wave = round(sum(scores) / len(scores), 3)
        max_wave = round(max(scores), 3)
        best = max(top6_details, key=lambda x: float(x.get("archetype_wave_score", 0.0) or 0.0))
        top_broker_id = str(best.get("broker_id") or "") or None
        top_broker_name = str(best.get("broker_name") or "") or None
    else:
        avg_wave = None
        max_wave = None
        top_broker_id = None
        top_broker_name = None

    signals = {
        "top6_avg_wave_score": avg_wave,
        "top6_max_wave_score": max_wave,
        "top6_wave_leader_id": top_broker_id,
        "top6_wave_leader_name": top_broker_name,
    }

    return {"signals": signals, "top6_details": top6_details}

