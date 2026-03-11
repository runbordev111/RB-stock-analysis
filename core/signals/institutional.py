from __future__ import annotations

from datetime import datetime, timedelta
import os
from typing import Any, Dict

import numpy as np
import pandas as pd

from core.finmind_client import FinMindClient

SignalsDict = Dict[str, Any]


def _to_date_str(d: str) -> str:
    return datetime.strptime(d, "%Y-%m-%d").strftime("%Y-%m-%d")


def _numeric_col(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    for c in candidates:
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return pd.Series([0.0] * len(df), index=df.index)


def compute_institutional_and_margin_signals(
    client: FinMindClient,
    stock_id: str,
    last_trade_date: str,
    lookback_days: int = 60,
) -> SignalsDict:
    """
    從 FinMind 抓三大法人 + 融資資料，產出簡單的 inst_*/margin_* signals。
    - time window: [last_trade_date - lookback_days, last_trade_date]
    - 以 calendar days 粗略拉取，再由 FinMind dataset 自身的 date 篩選
    """
    try:
        end_date = _to_date_str(last_trade_date)
    except Exception:
        end_date = last_trade_date

    start_date = (
        datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=lookback_days)
    ).strftime("%Y-%m-%d")

    # --- 三大法人：使用 v4 個股三大法人買賣表 TaiwanStockInstitutionalInvestorsBuySell ---
    inst_df = client.request_data(
        "TaiwanStockInstitutionalInvestorsBuySell",
        data_id=stock_id,
        start_date=start_date,
        end_date=end_date,
    )
    margin_df = client.request_data(
        "TaiwanStockMarginPurchaseShortSale",
        data_id=stock_id,
        start_date=start_date,
        end_date=end_date,
    )
    sbl_df = client.request_data(
        "TaiwanStockSecuritiesLending",
        data_id=stock_id,
        start_date=start_date,
        end_date=end_date,
    )

    out: SignalsDict = {}

    # --- 三大法人 ---
    if inst_df is not None and not inst_df.empty and "date" in inst_df.columns:
        df = inst_df.copy()
        df["date"] = df["date"].astype(str)
        df = df.sort_values("date").reset_index(drop=True)

        # Debug dump：把前幾列 schema 寫到 data/debug/inst_{stock_id}.json 方便檢查欄位
        try:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            debug_dir = os.path.join(project_root, "data", "debug")
            os.makedirs(debug_dir, exist_ok=True)
            debug_path = os.path.join(debug_dir, f"inst_{stock_id}.json")
            head_dict = df.head(10).to_dict(orient="list")
            import json

            with open(debug_path, "w", encoding="utf-8") as f:
                json.dump(head_dict, f, ensure_ascii=False, indent=2)
        except Exception:
            # debug 失敗不影響主流程
            pass

        # v4 TaiwanStockInstitutionalInvestorsBuySell 結構：
        # date, stock_id, name, buy, sell
        if "name" in df.columns and "buy" in df.columns and "sell" in df.columns:

            def _net_series(names: list[str]) -> pd.Series:
                sub = df[df["name"].isin(names)].copy()
                if sub.empty:
                    return pd.Series(dtype=float)
                sub["buy"] = pd.to_numeric(sub["buy"], errors="coerce").fillna(0.0)
                sub["sell"] = pd.to_numeric(sub["sell"], errors="coerce").fillna(0.0)
                sub["net"] = sub["buy"] - sub["sell"]
                g = (
                    sub.groupby("date", as_index=False)["net"]
                    .sum()
                    .sort_values("date")
                    .reset_index(drop=True)
                )
                g["date"] = g["date"].astype(str)
                return g.set_index("date")["net"]

            all_dates = sorted(df["date"].astype(str).unique().tolist())
            idx = pd.Index(all_dates, name="date")

            # 外資
            foreign = _net_series(["Foreign_Investor"]).reindex(idx).fillna(0.0)
            # 投信
            trust = _net_series(["Investment_Trust"]).reindex(idx).fillna(0.0)
            # 自營商（含自營、自營避險）
            dealer = _net_series(["Dealer_self", "Dealer_Hedging"]).reindex(idx).fillna(0.0)

            three = foreign + trust + dealer

            def _last_n_sum(s: pd.Series, n: int) -> float:
                if s is None or s.empty:
                    return 0.0
                return float(s.tail(n).sum())

            out["inst_foreign_net_5d"] = _last_n_sum(foreign, 5)
            out["inst_trust_net_5d"] = _last_n_sum(trust, 5)
            out["inst_three_net_5d"] = _last_n_sum(three, 5)

            out["inst_foreign_net_20d"] = _last_n_sum(foreign, 20)
            out["inst_trust_net_20d"] = _last_n_sum(trust, 20)
            out["inst_three_net_20d"] = _last_n_sum(three, 20)

            out["inst_foreign_net_60d"] = _last_n_sum(foreign, 60)
            out["inst_trust_net_60d"] = _last_n_sum(trust, 60)
            out["inst_three_net_60d"] = _last_n_sum(three, 60)

            f5 = out["inst_foreign_net_5d"]
            t5 = out["inst_trust_net_5d"]
            d5 = _last_n_sum(dealer, 5)

            def _sign(x: float) -> int:
                if x > 0:
                    return 1
                if x < 0:
                    return -1
                return 0

            sf, st, sd = _sign(f5), _sign(t5), _sign(d5)
            out["inst_three_align_5d"] = sf if sf == st == sd and sf != 0 else 0

        else:
            # 若欄位結構非預期，退回舊的欄位猜測（可能全部為 0，但至少不中斷）
            foreign = _numeric_col(
                df,
                [
                    "ForeignInvestorsDiff",
                    "foreign_investor_diff",
                    "ForeignInvestorDiff",
                ],
            )
            trust = _numeric_col(
                df,
                [
                    "InvestmentTrustDiff",
                    "investment_trust_diff",
                    "InvestTrustDiff",
                ],
            )
            dealer = _numeric_col(
                df,
                [
                    "DealerDiff",
                    "dealer_diff",
                    "SecuritiesDealerDiff",
                ],
            )

            three = foreign + trust + dealer

            def _last_n_sum(s: pd.Series, n: int) -> float:
                if s.empty:
                    return 0.0
                return float(s.tail(n).sum())

            out["inst_foreign_net_5d"] = _last_n_sum(foreign, 5)
            out["inst_trust_net_5d"] = _last_n_sum(trust, 5)
            out["inst_three_net_5d"] = _last_n_sum(three, 5)

            out["inst_foreign_net_20d"] = _last_n_sum(foreign, 20)
            out["inst_trust_net_20d"] = _last_n_sum(trust, 20)
            out["inst_three_net_20d"] = _last_n_sum(three, 20)

            out["inst_foreign_net_60d"] = _last_n_sum(foreign, 60)
            out["inst_trust_net_60d"] = _last_n_sum(trust, 60)
            out["inst_three_net_60d"] = _last_n_sum(three, 60)

            f5 = out["inst_foreign_net_5d"]
            t5 = out["inst_trust_net_5d"]
            d5 = _last_n_sum(dealer, 5)

            def _sign(x: float) -> int:
                if x > 0:
                    return 1
                if x < 0:
                    return -1
                return 0

            sf, st, sd = _sign(f5), _sign(t5), _sign(d5)
            out["inst_three_align_5d"] = sf if sf == st == sd and sf != 0 else 0

        def _last_n_sum(s: pd.Series, n: int) -> float:
            if s.empty:
                return 0.0
            return float(s.tail(n).sum())

        out["inst_foreign_net_5d"] = _last_n_sum(foreign, 5)
        out["inst_trust_net_5d"] = _last_n_sum(trust, 5)
        out["inst_three_net_5d"] = _last_n_sum(three, 5)

        out["inst_foreign_net_20d"] = _last_n_sum(foreign, 20)
        out["inst_trust_net_20d"] = _last_n_sum(trust, 20)
        out["inst_three_net_20d"] = _last_n_sum(three, 20)

        out["inst_foreign_net_60d"] = _last_n_sum(foreign, 60)
        out["inst_trust_net_60d"] = _last_n_sum(trust, 60)
        out["inst_three_net_60d"] = _last_n_sum(three, 60)

        f5 = out["inst_foreign_net_5d"]
        t5 = out["inst_trust_net_5d"]
        d5 = _last_n_sum(dealer, 5)

        def _sign(x: float) -> int:
            if x > 0:
                return 1
            if x < 0:
                return -1
            return 0

        sf, st, sd = _sign(f5), _sign(t5), _sign(d5)
        if sf == st == sd and sf != 0:
            out["inst_three_align_5d"] = sf
        else:
            out["inst_three_align_5d"] = 0
    else:
        # Graceful defaults
        out.update(
            {
                "inst_foreign_net_5d": 0.0,
                "inst_trust_net_5d": 0.0,
                "inst_three_net_5d": 0.0,
                "inst_foreign_net_20d": 0.0,
                "inst_trust_net_20d": 0.0,
                "inst_three_net_20d": 0.0,
                "inst_foreign_net_60d": 0.0,
                "inst_trust_net_60d": 0.0,
                "inst_three_net_60d": 0.0,
                "inst_three_align_5d": 0,
            }
        )

    # --- 融資（餘額變化） ---
    if margin_df is not None and not margin_df.empty and "date" in margin_df.columns:
        dfm = margin_df.copy()
        dfm["date"] = dfm["date"].astype(str)
        dfm = dfm.sort_values("date").reset_index(drop=True)

        margin_bal = _numeric_col(
            dfm,
            ["MarginPurchaseTodayBalance", "margin_purchase_today_balance", "FinancingBalance"],
        )

        if len(margin_bal) >= 2:
            last = float(margin_bal.iloc[-1])
            base_20 = float(margin_bal.iloc[max(0, len(margin_bal) - 20)])
            if base_20 <= 0:
                change_20 = 0.0
            else:
                change_20 = (last - base_20) / base_20
        else:
            change_20 = 0.0

        out["margin_balance_20d_change"] = float(np.round(change_20, 4))
        out["margin_risk_flag"] = 1 if change_20 >= 0.3 else 0
    else:
        out["margin_balance_20d_change"] = 0.0
        out["margin_risk_flag"] = 0

    # --- 借券（SBL） ---
    if sbl_df is not None and not sbl_df.empty and "date" in sbl_df.columns:
        dfs = sbl_df.copy()
        dfs["date"] = dfs["date"].astype(str)
        dfs = dfs.sort_values("date").reset_index(drop=True)

        # 成交量（張），所有交易方式合計
        vol = _numeric_col(dfs, ["volume"])

        def _last_n_sum_safe(s: pd.Series, n: int) -> float:
            if s.empty:
                return 0.0
            return float(s.tail(n).sum())

        out["sbl_volume_5d"] = _last_n_sum_safe(vol, 5)
        out["sbl_volume_20d"] = _last_n_sum_safe(vol, 20)
        out["sbl_volume_60d"] = _last_n_sum_safe(vol, 60)

        # 以 60 日平均為 baseline，最近 20 日是否明顯放大
        if len(vol) >= 20:
            avg_60 = float(vol.tail(60).mean()) if len(vol) >= 60 else float(vol.mean())
            sum_20 = out["sbl_volume_20d"]
            if avg_60 <= 0:
                sbl_ratio = 0.0
            else:
                sbl_ratio = sum_20 / (avg_60 * 20.0)
        else:
            sbl_ratio = 0.0

        out["sbl_short_pressure_ratio_20d"] = float(np.round(sbl_ratio, 3))
        out["sbl_short_pressure_flag"] = 1 if sbl_ratio >= 1.5 else 0
    else:
        out["sbl_volume_5d"] = 0.0
        out["sbl_volume_20d"] = 0.0
        out["sbl_volume_60d"] = 0.0
        out["sbl_short_pressure_ratio_20d"] = 0.0
        out["sbl_short_pressure_flag"] = 0

    # --- 綜合 regime flag（給策略池 / 風控用） ---
    inst_three_20 = float(out.get("inst_three_net_20d") or 0.0)
    margin_flag = int(out.get("margin_risk_flag") or 0)
    sbl_flag = int(out.get("sbl_short_pressure_flag") or 0)

    # 條件 1：三大法人 20 日合計偏多，且融資/借券壓力都不明顯
    out["inst_bull_no_short_pressure_flag"] = 1 if (
        inst_three_20 > 0 and margin_flag <= 0 and sbl_flag <= 0
    ) else 0

    # 條件 2：三大法人 20 日偏空，且借券壓力放大
    out["inst_bear_with_short_pressure_flag"] = 1 if (
        inst_three_20 < 0 and sbl_flag >= 1
    ) else 0

    return out

