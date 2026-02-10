# core/signals_validation.py
# MVP: 交易有效性驗證（只吃 price_df + signals，不碰 API）
# 輸出：signals["validation"] = {breakout_flag, divergence_flag, confirmation_score}

from __future__ import annotations
from typing import Any, Dict

import pandas as pd


def compute_validation_signals(
    price_df: pd.DataFrame | None,
    signals: Dict[str, Any],
    cfg: Any | None = None,
) -> Dict[str, Any]:
    """
    MVP validation:
      - breakout_flag: close >= SMA20 且 close > 前一日 20D high
      - divergence_flag: score>=70 但 breakout_flag=0
      - confirmation_score: breakout(60%) + score(40%) => 0~100
    """
    out = {
        "breakout_flag": 0,
        "divergence_flag": 0,
        "confirmation_score": 0.0,
    }

    # 需要先有 pipeline 的 score（沒有也可跑，但 divergence/confirmation 會退化）
    score_now = float(signals.get("score", 0) or 0)

    if price_df is None or price_df.empty:
        # 沒價格資料：只能用 score 做弱化輸出
        out["divergence_flag"] = int(score_now >= 70)
        out["confirmation_score"] = round(min(max(score_now, 0.0), 100.0) * 0.4, 1)
        return out

    dfp = price_df.copy()

    # 欄位檢查
    if not {"close", "high"}.issubset(dfp.columns):
        out["divergence_flag"] = int(score_now >= 70)
        out["confirmation_score"] = round(min(max(score_now, 0.0), 100.0) * 0.4, 1)
        return out

    close = dfp["close"].astype(float)
    high = dfp["high"].astype(float)

    sma20 = close.rolling(20).mean()
    high20 = high.rolling(20).max()

    breakout = 0
    if len(dfp) >= 20 and pd.notna(sma20.iloc[-1]) and len(high20) >= 2 and pd.notna(high20.iloc[-2]):
        last_close = float(close.iloc[-1])
        last_sma20 = float(sma20.iloc[-1])
        prev_high20 = float(high20.iloc[-2])  # 前一日 20D high，避免同日引用

        breakout = int(last_close >= last_sma20 and last_close > prev_high20)

    out["breakout_flag"] = breakout
    out["divergence_flag"] = int(score_now >= 70 and breakout == 0)

    # breakout 加權 60%，score 加權 40%
    out["confirmation_score"] = round(
        (60.0 if breakout else 0.0) + min(max(score_now, 0.0), 100.0) * 0.4,
        1,
    )
    return out

