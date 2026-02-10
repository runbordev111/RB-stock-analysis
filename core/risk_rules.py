# core/risk_rules.py
# MVP: 風控層（只吃 price_df，不碰 API）
# 輸出：signals["risk"] = {atr_pct_20d, avg_turnover_20d, invalid_flag}

from __future__ import annotations
from typing import Any, Dict

import pandas as pd


def compute_risk_signals(
    price_df: pd.DataFrame | None,
    signals: Dict[str, Any] | None = None,
    cfg: Any | None = None,
) -> Dict[str, Any]:
    """
    MVP risk:
      - atr_pct_20d: ATR20 / last_close（波動風險）
      - avg_turnover_20d: 20D 平均成交值（有 turnover 用 turnover，否則用 close*volume）
      - invalid_flag: last_close < SMA20（簡化失效條件）
    """
    out = {
        "atr_pct_20d": 0.0,
        "avg_turnover_20d": 0.0,
        "invalid_flag": 0,
    }

    if price_df is None or price_df.empty:
        return out

    dfp = price_df.copy()

    # --- ATR% (20D) ---
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
        if len(atr20) > 0 and pd.notna(atr20.iloc[-1]):
            last_close = float(close.iloc[-1]) if len(close) > 0 else 0.0
            if last_close != 0.0:
                out["atr_pct_20d"] = round(float(atr20.iloc[-1] / last_close), 4)

    # --- Avg Turnover (20D) ---
    if "turnover" in dfp.columns:
        out["avg_turnover_20d"] = float(dfp["turnover"].tail(20).mean())
    elif {"close", "volume"}.issubset(dfp.columns):
        out["avg_turnover_20d"] = float(
            (dfp["close"].astype(float) * dfp["volume"].astype(float)).tail(20).mean()
        )

    # --- Invalid flag (close < SMA20) ---
    if "close" in dfp.columns:
        close = dfp["close"].astype(float)
        sma20 = close.rolling(20).mean()
        if len(dfp) >= 20 and pd.notna(sma20.iloc[-1]):
            out["invalid_flag"] = int(float(close.iloc[-1]) < float(sma20.iloc[-1]))

    return out

