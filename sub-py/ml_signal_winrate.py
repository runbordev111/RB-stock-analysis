"""
Phase 3：簡單 ML — 從 backtest_signals_60d.csv 估計 pattern 的歷史勝率

功能：
- 從 backtest_signals_60d.csv 載入樣本（含 signals + ret_5d/ret_10d/ret_20d）
- 建立二元標籤：未來 N 日報酬 > 0 → 1，否則 0
- 使用簡單的樹模型（RandomForestClassifier）建 baseline 模型
- 輸出：
    - 訓練/驗證集的 accuracy / ROC-AUC / win_rate（終端 + HTML）
    - 模型存成 data/models/ml_winrate_ret{N}d.pkl
    - 特徵重要度存成 data/ml_feature_importance_ret{N}d.csv 與 HTML 報表

使用方式：
    python sub-py/ml_signal_winrate.py --csv data/backtest_signals_60d.csv --horizon 10
    python sub-py/ml_signal_winrate.py --horizons 5,10,20   # 一次跑多個 horizon
"""

import argparse
import os
from typing import List, Tuple

import numpy as np
import pandas as pd

try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score, roc_auc_score
    from sklearn.model_selection import train_test_split
    import joblib
except ImportError:
    RandomForestClassifier = None  # type: ignore[assignment]
    accuracy_score = None  # type: ignore[assignment]
    roc_auc_score = None  # type: ignore[assignment]
    train_test_split = None  # type: ignore[assignment]
    joblib = None  # type: ignore[assignment]


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data")
MODELS_PATH = os.path.join(DATA_PATH, "models")
DEFAULT_CSV = os.path.join(DATA_PATH, "backtest_signals_60d.csv")

def _time_split_df(
    df: pd.DataFrame,
    test_size: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Time-based split by trade_date (ascending) to avoid look-ahead from random shuffle.
    If trade_date is missing, fallback to simple tail split by row order.
    """
    if df is None or df.empty:
        return df.iloc[0:0].copy(), df.iloc[0:0].copy()

    df = df.copy()
    if "trade_date" in df.columns:
        df["trade_date"] = df["trade_date"].astype(str)
        if "stock_id" in df.columns:
            df["stock_id"] = df["stock_id"].astype(str)
            df = df.sort_values(["trade_date", "stock_id"]).reset_index(drop=True)
        else:
            df = df.sort_values(["trade_date"]).reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    n = len(df)
    n_test = int(round(n * float(test_size)))
    n_test = max(1, min(n - 1, n_test))
    cut = n - n_test
    return df.iloc[:cut].copy(), df.iloc[cut:].copy()


def _safe_auc(y_true: np.ndarray, prob: np.ndarray) -> float:
    try:
        if len(np.unique(y_true)) < 2:
            return float("nan")
        return float(roc_auc_score(y_true, prob))
    except Exception:
        return float("nan")


def _select_feature_columns(df: pd.DataFrame) -> List[str]:
    """
    選出可用的特徵欄位：
    - 排除 ID/日期欄位與未來報酬欄位
    - 包含 Phase 2 新增的 *_pctile 欄位，以及主要 signals 欄位
    """
    exclude_prefixes = ["ret_"]
    exclude_exact = {"stock_id", "trade_date"}

    cols: List[str] = []
    for c in df.columns:
        if c in exclude_exact:
            continue
        if any(c.startswith(p) for p in exclude_prefixes):
            continue
        # 嘗試轉成數值，非數值欄位略過
        try:
            pd.to_numeric(df[c].dropna().head(20), errors="raise")
        except Exception:
            continue
        cols.append(c)
    return cols


def _prepare_xy(
    df: pd.DataFrame,
    horizon: int,
    feat_cols: List[str] | None = None,
) -> Tuple[np.ndarray, np.ndarray, List[str]]:
    col_ret = f"ret_{horizon}d"
    if col_ret not in df.columns:
        raise ValueError(f"CSV 中沒有欄位 {col_ret}，請先在 backtest 時產出此 horizon。")

    df = df.copy()
    df[col_ret] = pd.to_numeric(df[col_ret], errors="coerce")
    df = df.dropna(subset=[col_ret])
    if df.empty:
        raise ValueError(f"{col_ret} 沒有有效樣本。")

    # 建立二元標籤：未來報酬 > 0 → 1，<=0 → 0
    y = (df[col_ret] > 0).astype(int).values

    if feat_cols is None:
        feat_cols = _select_feature_columns(df)
        if not feat_cols:
            raise ValueError("找不到可用的數值特徵欄位。")

    # Ensure consistent feature set/order across train/test
    work = df.reindex(columns=feat_cols, fill_value=0.0).copy()
    X = work.apply(pd.to_numeric, errors="coerce").fillna(0.0).values
    return X, y, feat_cols


def _save_artifacts(
    clf,
    feat_cols: List[str],
    importances: np.ndarray,
    horizon: int,
    acc_train: float,
    acc_test: float,
    auc_train: float,
    auc_test: float,
    split_method: str,
    rule_stats: List[dict] | None = None,
) -> None:
    """存模型、特徵重要度 CSV、簡易 HTML 報表到 data/models 與 data/。"""
    os.makedirs(MODELS_PATH, exist_ok=True)

    # 模型
    if joblib is not None:
        pkl_path = os.path.join(MODELS_PATH, f"ml_winrate_ret{horizon}d.pkl")
        joblib.dump(clf, pkl_path)
        print(f"  Model saved: {pkl_path}")

    # 特徵重要度 CSV（完整排序，HTML 只展示前 10 名）
    imp_df = pd.DataFrame(
        {"feature": feat_cols, "importance": importances}
    ).sort_values("importance", ascending=False)
    csv_path = os.path.join(DATA_PATH, f"ml_feature_importance_ret{horizon}d.csv")
    imp_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"  Feature importance CSV: {csv_path}")

    # 簡易 HTML 報表（聚焦前 10 名關鍵特徵）
    html_path = os.path.join(DATA_PATH, f"ml_winrate_report_ret{horizon}d.html")
    # 中文化常見關鍵特徵名稱，方便閱讀
    label_map = {
        # Geo / 成本帶相關
        "enh_cost_low": "主力成本帶下緣：主力這段期間多數買進量，大致不會低於此價",
        "enh_cost_high": "主力成本帶上緣：主力成本帶的上緣價位，長期站上代表相對強勢",
        # 三大法人相關
        "inst_foreign_net_5d": "外資 5日買賣超",
        "inst_foreign_net_20d": "外資 20日買賣超",
        "inst_foreign_net_60d": "外資 60日買賣超",
        "inst_trust_net_5d": "投信 5日買賣超",
        "inst_trust_net_20d": "投信 20日買賣超",
        "inst_trust_net_60d": "投信 60日買賣超",
        "inst_three_net_5d": "三大法人 5日買賣超",
        "inst_three_net_20d": "三大法人 20日買賣超",
        "inst_three_net_60d": "三大法人 60日買賣超",
        # 買賣壓 / 借券 / 融資相關
        "sbl_volume_5d": "借券成交量 5日",
        "sbl_volume_60d": "借券成交量 60日",
        "margin_balance_20d_change": "融資餘額 20日變化",
        # 分數 / 百分位相關
        "final_score_pctile": "最終分數百分位",
        "regime_score": "趨勢底座分數",
        "regime_score_pctile": "趨勢底座分數百分位",
    }

    def _pretty_feature(name: str) -> str:
        desc = label_map.get(name)
        if desc:
            return f"{desc}（{name}）"
        return name

    # 主清單：只顯示前 10 名
    rows = imp_df.head(10).to_dict("records")
    rows_html = "".join(
        f'<tr><td>{_pretty_feature(r["feature"])}</td><td class="text-end">{r["importance"]:.4f}</td></tr>'
        for r in rows
    )
    # 聚焦顯示 Inst / Margin / SBL 相關特徵（前 10 名）
    focus_df = imp_df[
        imp_df["feature"].str.startswith(("inst_", "margin_", "sbl_"))
    ].copy()
    focus_rows = focus_df.head(10).to_dict("records")
    focus_html = "".join(
        f'<tr><td>{_pretty_feature(r["feature"])}</td><td class="text-end">{r["importance"]:.4f}</td></tr>'
        for r in focus_rows
    )

    # 自訂條件 winrate（test set）
    rule_html = ""
    if rule_stats:
        trs = []
        for r in rule_stats:
            name = r.get("name", "")
            n = r.get("n", 0)
            win = r.get("win_rate")
            w_txt = "NaN"
            try:
                if win is not None and not (np.isnan(win)):
                    w_txt = f"{win:.3f}"
            except Exception:
                pass
            trs.append(
                f'<tr><td>{name}</td><td class="text-end">{n}</td><td class="text-end">{w_txt}</td></tr>'
            )
        rule_html = "".join(trs)
    html = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head><meta charset="UTF-8"><title>勝率評估表（ret_{horizon}d）</title>
<style>body{{font-family:Segoe UI,sans-serif;background:#1a1a1a;color:#e0e0e0;padding:20px;}}
table{{border-collapse:collapse;width:100%;max-width:760px;}} th,td{{border:1px solid #444;padding:8px;text-align:left;}}
th{{background:#333;color:#f1c40f;}} .metric{{margin:12px 0;}}</style></head>
<body>
<h1>勝率評估表（未來 {horizon} 日報酬 &gt; 0）</h1>
<div class="metric">說明：ret_5d = 短線警示燈（看是否很快出現反轉或加速）；ret_10d = 波段輔助燈（觀察半個波段內勝率）；ret_20d = 主要決策燈（完整一個波段的勝率表現）。</div>
<div class="metric">資料切分方式：{split_method}</div>
<div class="metric">訓練集正確率 = {acc_train:.3f} &nbsp;|&nbsp; 測試集正確率 = {acc_test:.3f}</div>
<div class="metric">訓練集 AUC = {auc_train:.3f} &nbsp;|&nbsp; 測試集 AUC = {auc_test:.3f}</div>
<h2>前 10 名影響勝率的特徵</h2>
<table><thead><tr><th>特徵名稱</th><th class="text-end">重要度</th></tr></thead><tbody>{rows_html}</tbody></table>
<h2>三大法人 / 融資 / 借券 相關特徵（前 10 名）</h2>
<table><thead><tr><th>特徵名稱</th><th class="text-end">重要度</th></tr></thead><tbody>{focus_html or '<tr><td colspan="2">尚無 inst_ / margin_ / sbl_ 相關特徵</td></tr>'}</tbody></table>
<h2>自訂情境下的勝率（測試集）</h2>
<table><thead><tr><th>條件名稱</th><th class="text-end">樣本數</th><th class="text-end">勝率（win_rate）</th></tr></thead><tbody>{rule_html or '<tr><td colspan="3">尚未定義條件或樣本數不足</td></tr>'}</tbody></table>
</body></html>"""
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Report HTML: {html_path}")


def run_ml(
    csv_path: str,
    horizon: int,
    test_size: float,
    random_state: int,
    split: str,
    save_artifacts: bool = True,
) -> None:
    if RandomForestClassifier is None:
        print("❌ 未安裝 scikit-learn，請先執行：pip install scikit-learn")
        return

    if not os.path.exists(csv_path):
        print(f"❌ 找不到 CSV：{csv_path}")
        return

    df = pd.read_csv(csv_path, dtype={"stock_id": str}, encoding="utf-8-sig")

    # Filter valid labels first, then split (avoid empty test after dropna)
    col_ret = f"ret_{horizon}d"
    if col_ret not in df.columns:
        print(f"❌ CSV 中沒有欄位 {col_ret}，請先在 backtest 時產出此 horizon。")
        return
    df[col_ret] = pd.to_numeric(df[col_ret], errors="coerce")
    df = df.dropna(subset=[col_ret]).copy()
    if df.empty:
        print(f"❌ {col_ret} 沒有有效樣本。")
        return

    if split == "time":
        df_train, df_test = _time_split_df(df, test_size=test_size)
    else:
        # random split (legacy behavior; avoid stratify failures on edge cases)
        df_train, df_test = train_test_split(
            df, test_size=test_size, random_state=random_state, shuffle=True
        )

    # Pick a stable feature set from full filtered df
    feat_cols = _select_feature_columns(df)
    if not feat_cols:
        print("❌ 找不到可用的數值特徵欄位。")
        return

    X_train, y_train, _ = _prepare_xy(df_train, horizon=horizon, feat_cols=feat_cols)
    X_test, y_test, _ = _prepare_xy(df_test, horizon=horizon, feat_cols=feat_cols)

    clf = RandomForestClassifier(
        n_estimators=200,
        max_depth=6,
        random_state=random_state,
        n_jobs=-1,
        class_weight="balanced",
    )
    clf.fit(X_train, y_train)

    prob_train = clf.predict_proba(X_train)[:, 1]
    prob_test = clf.predict_proba(X_test)[:, 1]

    pred_train = (prob_train >= 0.5).astype(int)
    pred_test = (prob_test >= 0.5).astype(int)

    acc_train = accuracy_score(y_train, pred_train)
    acc_test = accuracy_score(y_test, pred_test)

    auc_train = _safe_auc(y_train, prob_train)
    auc_test = _safe_auc(y_test, prob_test)

    print(f"\n===== Phase 3：ML 勝率估計（ret_{horizon}d）=====")
    print(f"樣本數：total={len(y_train)+len(y_test)}, train={len(y_train)}, test={len(y_test)}")
    print(f"Split = {split}")
    print(f"Train accuracy = {acc_train:.3f}, AUC = {auc_train:.3f}")
    print(f"Test  accuracy = {acc_test:.3f}, AUC = {auc_test:.3f}")
    print(f"Train win_rate(pred=1) = {y_train[pred_train == 1].mean() if (pred_train == 1).any() else float('nan'):.3f}")
    print(f"Test  win_rate(pred=1) = {y_test[pred_test == 1].mean() if (pred_test == 1).any() else float('nan'):.3f}")

    importances = clf.feature_importances_
    order = np.argsort(importances)[::-1]
    print("\nTop 20 feature importance:")
    for idx in order[:20]:
        print(f"{feat_cols[idx]:40s}  {importances[idx]:.4f}")

    # 自訂條件 winrate（專注在 Inst / Margin / SBL cross-check）
    rule_stats: List[dict] = []
    try:
        df_test_num = df_test.copy()
        for col in [
            "inst_three_net_20d",
            "margin_risk_flag",
            "sbl_short_pressure_flag",
        ]:
            if col in df_test_num.columns:
                df_test_num[col] = pd.to_numeric(
                    df_test_num[col], errors="coerce"
                )

        # 條件 1：三大法人 20 日合計偏多，且融資/借券壓力都不明顯
        cond1_cols = [
            "inst_three_net_20d",
            "margin_risk_flag",
            "sbl_short_pressure_flag",
        ]
        if all(c in df_test_num.columns for c in cond1_cols):
            c1 = (
                (df_test_num["inst_three_net_20d"] > 0)
                & (df_test_num["margin_risk_flag"].fillna(0) <= 0)
                & (df_test_num["sbl_short_pressure_flag"].fillna(0) <= 0)
            )
            idx1 = c1.values
            n1 = int(idx1.sum())
            win1 = float(y_test[idx1].mean()) if n1 > 0 else float("nan")
            rule_stats.append(
                {
                    "name": "三大法人20日合計>0 且 融資/借券壓力正常（inst_three_net_20d>0 & no_margin_sbl_pressure）",
                    "n": n1,
                    "win_rate": win1,
                }
            )

        # 條件 2：三大法人 20 日偏空，且借券壓力放大
        if all(c in df_test_num.columns for c in cond1_cols):
            c2 = (
                (df_test_num["inst_three_net_20d"] < 0)
                & (df_test_num["sbl_short_pressure_flag"].fillna(0) >= 1)
            )
            idx2 = c2.values
            n2 = int(idx2.sum())
            win2 = float(y_test[idx2].mean()) if n2 > 0 else float("nan")
            rule_stats.append(
                {
                    "name": "三大法人20日合計<0 且 借券壓力放大（inst_three_net_20d<0 & sbl_short_pressure_flag==1）",
                    "n": n2,
                    "win_rate": win2,
                }
            )
    except Exception:
        # 自訂條件失敗時，不影響主流程
        rule_stats = []

    if save_artifacts:
        print("\nSaving artifacts...")
        _save_artifacts(
            clf,
            feat_cols,
            importances,
            horizon,
            acc_train,
            acc_test,
            auc_train,
            auc_test,
            split_method=split,
            rule_stats=rule_stats,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 3：簡單 ML 估計 signals 勝率")
    parser.add_argument(
        "--csv",
        type=str,
        default=DEFAULT_CSV,
        help="backtest CSV 路徑（預設 data/backtest_signals_60d.csv）",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=None,
        help="單一 horizon（例如 10 → ret_10d）",
    )
    parser.add_argument(
        "--horizons",
        type=str,
        default=None,
        help="多個 horizon，逗號分隔（例如 5,10,20），與 --horizon 二擇一",
    )
    parser.add_argument(
        "--test_size",
        type=float,
        default=0.3,
        help="測試集比例（預設 0.3）",
    )
    parser.add_argument(
        "--split",
        type=str,
        default="time",
        choices=["time", "random"],
        help="資料切分方式：time=依 trade_date 時序切分（預設）；random=隨機切分",
    )
    parser.add_argument(
        "--random_state",
        type=int,
        default=42,
        help="隨機種子（預設 42）",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="不寫出模型與報表（僅終端輸出）",
    )

    args = parser.parse_args()
    csv_path = args.csv
    if not os.path.isabs(csv_path):
        csv_path = os.path.normpath(os.path.join(PROJECT_ROOT, csv_path))

    if args.horizons:
        horizons = [int(h.strip()) for h in args.horizons.split(",") if h.strip()]
    elif args.horizon is not None:
        horizons = [args.horizon]
    else:
        horizons = [10]

    for h in horizons:
        run_ml(
            csv_path=csv_path,
            horizon=h,
            test_size=args.test_size,
            random_state=args.random_state,
            split=args.split,
            save_artifacts=not args.no_save,
        )


if __name__ == "__main__":
    main()

