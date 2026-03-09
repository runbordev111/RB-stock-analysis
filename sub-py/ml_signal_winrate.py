"""
Phase 3：簡單 ML — 從 backtest_signals_60d.csv 估計 pattern 的歷史勝率

功能：
- 從 backtest_signals_60d.csv 載入樣本（含 signals + ret_5d/ret_10d/ret_20d）
- 建立二元標籤：未來 N 日報酬 > 0 → 1，否則 0
- 使用簡單的樹模型（RandomForestClassifier）或 LogisticRegression 建一個 baseline 模型
- 輸出：
    - 訓練/驗證集的 accuracy / ROC-AUC / win_rate
    - 重要特徵排序（feature importance 或係數）

使用方式：
    python SubPY/ml_signal_winrate.py --csv data/backtest_signals_60d.csv --horizon 10
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
except ImportError:
    RandomForestClassifier = None  # type: ignore[assignment]
    accuracy_score = None  # type: ignore[assignment]
    roc_auc_score = None  # type: ignore[assignment]
    train_test_split = None  # type: ignore[assignment]


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data")
DEFAULT_CSV = os.path.join(DATA_PATH, "backtest_signals_60d.csv")


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

    feat_cols = _select_feature_columns(df)
    if not feat_cols:
        raise ValueError("找不到可用的數值特徵欄位。")

    X = df[feat_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0).values
    return X, y, feat_cols


def run_ml(
    csv_path: str,
    horizon: int,
    test_size: float,
    random_state: int,
) -> None:
    if RandomForestClassifier is None:
        print("❌ 未安裝 scikit-learn，請先執行：pip install scikit-learn")
        return

    if not os.path.exists(csv_path):
        print(f"❌ 找不到 CSV：{csv_path}")
        return

    df = pd.read_csv(csv_path, dtype={"stock_id": str}, encoding="utf-8-sig")
    X, y, feat_cols = _prepare_xy(df, horizon=horizon)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )

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

    try:
        auc_train = roc_auc_score(y_train, prob_train)
        auc_test = roc_auc_score(y_test, prob_test)
    except Exception:
        auc_train = float("nan")
        auc_test = float("nan")

    print(f"\n===== Phase 3：ML 勝率估計（ret_{horizon}d）=====")
    print(f"樣本數：total={len(y)}, train={len(y_train)}, test={len(y_test)}")
    print(f"Train accuracy = {acc_train:.3f}, AUC = {auc_train:.3f}")
    print(f"Test  accuracy = {acc_test:.3f}, AUC = {auc_test:.3f}")
    print(f"Train win_rate(pred=1) = {y_train[pred_train == 1].mean() if (pred_train == 1).any() else float('nan'):.3f}")
    print(f"Test  win_rate(pred=1) = {y_test[pred_test == 1].mean() if (pred_test == 1).any() else float('nan'):.3f}")

    # 特徵重要度
    importances = clf.feature_importances_
    order = np.argsort(importances)[::-1]
    print("\nTop 20 feature importance:")
    for idx in order[:20]:
        print(f"{feat_cols[idx]:40s}  {importances[idx]:.4f}")


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
        default=10,
        help="使用哪一個未來報酬欄位（例如 5/10/20 → 使用 ret_10d）",
    )
    parser.add_argument(
        "--test_size",
        type=float,
        default=0.3,
        help="測試集比例（預設 0.3）",
    )
    parser.add_argument(
        "--random_state",
        type=int,
        default=42,
        help="隨機種子（預設 42）",
    )

    args = parser.parse_args()
    csv_path = args.csv
    if not os.path.isabs(csv_path):
        csv_path = os.path.join(DATA_PATH, csv_path)

    run_ml(
        csv_path=csv_path,
        horizon=args.horizon,
        test_size=args.test_size,
        random_state=args.random_state,
    )


if __name__ == "__main__":
    main()

