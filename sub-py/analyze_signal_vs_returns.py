"""
Phase 1：Signal vs 未來報酬 — 分析腳本

讀取 backtest_signals_60d.csv，產出：
- Score 區間（0-40 / 40-60 / 60-80 / 80+）的未來 5/10/20 日報酬統計與分佈
- Monitor state（ACCUMULATION / MARKUP / ...）的報酬統計與分佈
- HTML 報告（表格 + 圖表），方便判斷哪些訊號有 predictive power。

使用方式：
  python SubPY/analyze_signal_vs_returns.py
  python SubPY/analyze_signal_vs_returns.py --csv data/backtest_signals_60d.csv --output data/signal_vs_returns_report.html
"""

import os
import sys
import argparse
from datetime import datetime

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

DATA_PATH = os.path.join(PROJECT_ROOT, "data")
DEFAULT_CSV = os.path.join(DATA_PATH, "backtest_signals_60d.csv")
FIG_DIR = os.path.join(DATA_PATH, "signal_vs_returns_figures")

SCORE_BUCKETS = [(0, 40, "0-40"), (40, 60, "40-60"), (60, 80, "60-80"), (80, 101, "80+")]
RET_COLS = ["ret_5d", "ret_10d", "ret_20d"]


def _score_bucket(score_series):
    """將 score 分桶，回傳 bucket 標籤。"""
    def bucket(x):
        if pd.isna(x):
            return None
        x = float(x)
        for lo, hi, label in SCORE_BUCKETS:
            if lo <= x < hi:
                return label
        return "80+"
    return score_series.map(bucket)


def load_and_prepare(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype={"stock_id": str}, encoding="utf-8-sig")
    for c in RET_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    score_col = "final_score" if "final_score" in df.columns else "score"
    if score_col in df.columns:
        df["score_bucket"] = _score_bucket(df[score_col])
    else:
        df["score_bucket"] = None
    if "monitor_state" in df.columns:
        df["monitor_state"] = df["monitor_state"].fillna("NEUTRAL").astype(str)
    else:
        df["monitor_state"] = "NEUTRAL"
    return df


def stats_by_group(df: pd.DataFrame, group_col: str, value_col: str) -> pd.DataFrame:
    """對 group_col 分組，算 value_col 的 n, mean, median, std, win_rate%。"""
    valid = df[[group_col, value_col]].dropna()
    if valid.empty:
        return pd.DataFrame()
    g = valid.groupby(group_col)[value_col]
    n = g.count()
    mean = g.mean()
    median = g.median()
    std = g.std().fillna(0)
    win_rate = (g.apply(lambda x: (x > 0).mean() * 100))
    out = pd.DataFrame({"n": n, "mean": mean, "median": median, "std": std, "win_rate%": win_rate})
    return out.round(4)


def run_analysis(df: pd.DataFrame, ret_cols: list) -> dict:
    results = {"by_score": {}, "by_state": {}, "summary": []}
    df_clean = df.dropna(subset=ret_cols, how="all").copy()
    if df_clean.empty:
        return results

    for col in ret_cols:
        if col not in df_clean.columns:
            continue
        if df_clean["score_bucket"].notna().any():
            by_score = stats_by_group(df_clean.dropna(subset=["score_bucket"]), "score_bucket", col)
            by_score = by_score.reindex([lb for _, _, lb in SCORE_BUCKETS])
            results["by_score"][col] = by_score
        by_state = stats_by_group(df_clean, "monitor_state", col)
        order = ["ACCUMULATION", "MARKUP", "FADING", "DISTRIBUTION", "NEUTRAL"]
        by_state = by_state.reindex([s for s in order if s in by_state.index])
        results["by_state"][col] = by_state

    results["n_total"] = len(df_clean)
    results["n_with_returns"] = df_clean[ret_cols].notna().any(axis=1).sum()
    return results


def write_html_report(
    results: dict, output_path: str, figures_dir: str, figure_files: list = None
) -> None:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    os.makedirs(figures_dir, exist_ok=True)
    figure_files = figure_files or []
    out_dir = os.path.dirname(output_path)
    rel_fig = os.path.relpath(figures_dir, out_dir) if out_dir else figures_dir
    rel_fig = rel_fig.replace("\\", "/")

    html = []
    html.append("<!DOCTYPE html><html lang=\"zh-TW\"><head><meta charset=\"UTF-8\">")
    html.append("<title>Phase 1: Signal vs 未來報酬</title>")
    html.append("<style>body{font-family:Segoe UI,Microsoft JhengHei,sans-serif;background:#1a1a1a;color:#e0e0e0;padding:24px;} h1,h2{color:#f1c40f;} h3{color:#ddd;} table{border-collapse:collapse;margin:12px 0;} th,td{border:1px solid #444;padding:8px 12px;text-align:right;} th{background:#333;color:#f1c40f;} .n{text-align:center;} .meta{color:#888;font-size:0.9em;} .fig{max-width:90%;margin:16px 0;border:1px solid #444;}</style></head><body>")
    html.append("<h1>Phase 1：Signal vs 未來報酬</h1>")
    html.append(f"<p class=\"meta\">報告產生時間：{datetime.now().strftime('%Y-%m-%d %H:%M')} | 有效樣本數：{results.get('n_total', 0)}</p>")

    # By Score
    html.append("<h2>一、依 Score 區間（final_score / score）</h2>")
    for col in RET_COLS:
        if col not in results.get("by_score", {}):
            continue
        tb = results["by_score"][col]
        if tb.empty:
            continue
        html.append(f"<h3>{col}（未來報酬）</h3>")
        html.append(tb.to_html(classes="n", float_format="%.4f").replace("<th>", "<th class=\"n\">"))
        html.append("<br/>")
    html.append("<p class=\"meta\">解讀：mean/median 為該區間平均/中位報酬；win_rate% 為報酬&gt;0 的比例。可依此調整策略門檻（例如 70 分以上才做多）。</p>")
    for fn in figure_files:
        if "ret_by_score_" in fn:
            name = os.path.basename(fn)
            html.append(f"<p><img class=\"fig\" src=\"{rel_fig}/{name}\" alt=\"{name}\"/></p>")

    # By Monitor state
    html.append("<h2>二、依 Monitor state（Whale Trend Monitor）</h2>")
    for col in RET_COLS:
        if col not in results.get("by_state", {}):
            continue
        tb = results["by_state"][col]
        if tb.empty:
            continue
        html.append(f"<h3>{col}</h3>")
        html.append(tb.to_html(classes="n", float_format="%.4f").replace("<th>", "<th class=\"n\">"))
        html.append("<br/>")
    html.append("<p class=\"meta\">解讀：比較 ACCUMULATION / MARKUP 與 NEUTRAL / DISTRIBUTION 的報酬差異，判斷哪些狀態值得當作進場/出場條件。</p>")
    for fn in figure_files:
        if "ret_by_state_" in fn:
            name = os.path.basename(fn)
            html.append(f"<p><img class=\"fig\" src=\"{rel_fig}/{name}\" alt=\"{name}\"/></p>")

    html.append("</body></html>")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(html))
    print(f"📄 HTML 報告已寫入：{output_path}")


def try_plot_figures(df: pd.DataFrame, figures_dir: str, ret_cols: list) -> list:
    """若 matplotlib 可用，產出分佈圖並回傳檔名列表。"""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("⚠️ 未安裝 matplotlib，略過圖表。可執行：pip install matplotlib")
        return []

    os.makedirs(figures_dir, exist_ok=True)
    plt.rcParams["figure.facecolor"] = "#1a1a1a"
    plt.rcParams["axes.facecolor"] = "#252525"
    plt.rcParams["axes.edgecolor"] = "#444"
    plt.rcParams["axes.labelcolor"] = "#e0e0e0"
    plt.rcParams["xtick.color"] = "#aaa"
    plt.rcParams["ytick.color"] = "#aaa"
    plt.rcParams["font.family"] = ["Microsoft JhengHei", "sans-serif"]
    files = []

    df_clean = df.dropna(subset=ret_cols, how="all").copy()
    if df_clean.empty or not df_clean["score_bucket"].notna().any():
        return files

    order_bucket = [lb for _, _, lb in SCORE_BUCKETS]
    order_state = ["ACCUMULATION", "MARKUP", "FADING", "DISTRIBUTION", "NEUTRAL"]

    for col in ret_cols:
        if col not in df_clean.columns:
            continue
        sub = df_clean[["score_bucket", col]].dropna()
        sub = sub[sub["score_bucket"].isin(order_bucket)]
        if sub.empty:
            continue
        pairs = [(lb, sub[sub["score_bucket"] == lb][col].values) for lb in order_bucket]
        data_by_bucket = [a for _, a in pairs if len(a) > 0]
        labels_bucket = [lb for lb, a in pairs if len(a) > 0]
        if not data_by_bucket:
            continue
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.boxplot(data_by_bucket, labels=labels_bucket, patch_artist=True)
        ax.set_xlabel("Score 區間")
        ax.set_ylabel(col + " (報酬)")
        ax.axhline(0, color="#666", linestyle="--")
        ax.set_title(f"{col} 依 Score 區間分佈")
        fpath = os.path.join(figures_dir, f"ret_by_score_{col}.png")
        plt.savefig(fpath, bbox_inches="tight")
        plt.close()
        files.append(fpath)

    for col in ret_cols:
        if col not in df_clean.columns:
            continue
        sub = df_clean[["monitor_state", col]].dropna()
        sub = sub[sub["monitor_state"].isin(order_state)]
        if sub.empty:
            continue
        pairs = [(s, sub[sub["monitor_state"] == s][col].values) for s in order_state]
        data_by_state = [a for _, a in pairs if len(a) > 0]
        labels_used = [s for s, a in pairs if len(a) > 0]
        if not data_by_state:
            continue
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.boxplot(data_by_state, labels=labels_used, patch_artist=True)
        ax.set_xlabel("Monitor state")
        ax.set_ylabel(col + " (報酬)")
        ax.axhline(0, color="#666", linestyle="--")
        ax.set_title(f"{col} 依 Monitor state 分佈")
        fpath = os.path.join(figures_dir, f"ret_by_state_{col}.png")
        plt.savefig(fpath, bbox_inches="tight")
        plt.close()
        files.append(fpath)

    if files:
        print(f"📊 圖表已儲存至：{figures_dir}")
    return files


def main():
    parser = argparse.ArgumentParser(description="Phase 1: Signal vs 未來報酬分析")
    parser.add_argument("--csv", type=str, default=DEFAULT_CSV, help="backtest CSV 路徑")
    parser.add_argument("--output", type=str, default=os.path.join(DATA_PATH, "signal_vs_returns_report.html"), help="HTML 報告輸出路徑")
    parser.add_argument("--figures", type=str, default=FIG_DIR, help="圖表輸出目錄")
    parser.add_argument("--no-plot", action="store_true", help="不產出圖表（僅表格）")
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        print(f"❌ 找不到 CSV：{args.csv}")
        print("請先執行：python SubPY/backtest_signals_60d.py --stock_ids 2317,2454,6239 --days 60")
        sys.exit(1)

    df = load_and_prepare(args.csv)
    ret_cols = [c for c in RET_COLS if c in df.columns]
    if not ret_cols:
        print("❌ CSV 中沒有 ret_5d / ret_10d / ret_20d 欄位。")
        sys.exit(1)

    results = run_analysis(df, ret_cols)
    if results["n_total"] == 0:
        print("⚠️ 沒有同時具備 signals 與未來報酬的樣本。")
        sys.exit(0)

    # Console 簡表
    print("\n===== Score 區間 vs 未來報酬（摘要）=====")
    for col in ret_cols:
        if col in results.get("by_score", {}):
            print(f"\n{col}:")
            print(results["by_score"][col].to_string())
    print("\n===== Monitor state vs 未來報酬（摘要）=====")
    for col in ret_cols:
        if col in results.get("by_state", {}):
            print(f"\n{col}:")
            print(results["by_state"][col].to_string())

    fig_files = []
    if not args.no_plot:
        fig_files = try_plot_figures(df, args.figures, ret_cols)
    write_html_report(results, args.output, args.figures, figure_files=fig_files)

    print("\n✅ Phase 1 分析完成。請開啟 HTML 報告檢視：")
    print(f"   {os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()
