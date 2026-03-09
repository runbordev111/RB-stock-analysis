# scrape_wantgoo_brokers_pw.py
# 用 Playwright 真瀏覽器抓 WantGoo，並用 FinMind 券商主檔白名單過濾，避免抓到導覽列垃圾

import os
import re
import argparse
from datetime import datetime
from difflib import SequenceMatcher

import pandas as pd
from playwright.sync_api import sync_playwright

WANTGOO_URL = "https://www.wantgoo.com/stock/major-investors/broker-buy-sell-rank"

FOREIGN_KEYWORDS = [
    "港商", "美商", "星洲", "瑞士", "法銀", "巴黎", "巴克萊", "匯豐", "花旗",
    "摩根", "高盛", "美林", "德意志", "野村", "法興", "瑞銀", "麥格理",
    "摩根士丹利", "摩根大通", "JP", "UBS", "HSBC", "Citi", "Goldman", "Morgan",
]

def norm(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    s = s.replace("　", " ")
    s = re.sub(r"\s+", "", s)
    s = s.replace("（", "(").replace("）", ")")
    s = s.replace("股份有限公司", "").replace("有限公司", "")
    return s

def sim(a: str, b: str) -> float:
    return SequenceMatcher(None, norm(a), norm(b)).ratio()

def is_foreign_broker(name: str) -> bool:
    return isinstance(name, str) and any(k in name for k in FOREIGN_KEYWORDS)

def load_finmind_groups(finmind_csv: str) -> list[str]:
    df = pd.read_csv(finmind_csv, dtype=str, encoding="utf-8-sig")
    # 你可能給 broker_master_core_finmind.csv（有 broker_group）
    if "broker_group" in df.columns:
        groups = df["broker_group"].dropna().astype(str).tolist()
    # 或你給 broker_info_finmind.csv（只有 securities_trader）
    elif "securities_trader" in df.columns:
        groups = df["securities_trader"].dropna().astype(str).tolist()
        # 取 group（合庫-台中 -> 合庫）
        groups = [g.split("-", 1)[0].strip() for g in groups]
    else:
        raise RuntimeError("FinMind CSV 欄位找不到 broker_group / securities_trader，請確認你輸入的檔案。")

    # 去重保序
    out = []
    seen = set()
    for g in groups:
        ng = norm(g)
        if ng and ng not in seen:
            seen.add(ng)
            out.append(g.strip())
    return out

def extract_text_candidates(page) -> list[str]:
    """
    從頁面抓可能的清單文字：
    - 所有 <option>（下拉選單最常存券商/分行）
    - 所有按鈕/標籤類元素（有些站不用 select）
    """
    candidates = []

    # 1) option texts
    options = page.locator("option")
    for i in range(options.count()):
        t = options.nth(i).inner_text().strip()
        if t:
            candidates.append(t)

    # 2) buttons / chips / labels (保底)
    # 只抓較短的中文字串，避免整段文章
    nodes = page.locator("button, a, span, div")
    max_nodes = min(nodes.count(), 2000)
    for i in range(max_nodes):
        t = nodes.nth(i).inner_text().strip()
        if 2 <= len(t) <= 12:
            # 避免抓到太多分行/導覽列：先粗篩
            if re.search(r"[\u4e00-\u9fff]", t):
                candidates.append(t)

    # 去重保序
    seen = set()
    out = []
    for x in candidates:
        nx = norm(x)
        if nx and nx not in seen:
            seen.add(nx)
            out.append(x)
    return out

def filter_as_brokers(candidates: list[str], finmind_groups: list[str], threshold: float) -> list[str]:
    """
    用 FinMind 券商群組做白名單比對：只留下相似度高者
    """
    result = []
    for c in candidates:
        # 跳過明顯不是券商的詞
        if any(k in c for k in ["首頁","社團","台股","期權","新聞","商城","排序","排名","買超","賣超","張數","金額","近1日","近5日","近10日","近20日"]):
            continue

        # 找最佳匹配
        best_s = 0.0
        for g in finmind_groups:
            s = sim(c, g)
            if s > best_s:
                best_s = s
        if best_s >= threshold:
            result.append(c)

    # 去重保序
    seen = set()
    out = []
    for x in result:
        nx = norm(x)
        if nx and nx not in seen:
            seen.add(nx)
            out.append(x)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out_dir", default="./data", help="輸出資料夾")
    ap.add_argument("--finmind_csv", required=True, help="FinMind 券商主檔（建議 broker_master_core_finmind.csv 或 broker_info_finmind.csv）")
    ap.add_argument("--threshold", type=float, default=0.78, help="與 FinMind 券商群組相似度門檻（0~1），太嚴會抓不到，太鬆會混雜")
    ap.add_argument("--headless", action="store_true", help="無頭模式（預設會開瀏覽器方便你目視）")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    finmind_groups = load_finmind_groups(args.finmind_csv)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        page = browser.new_page(locale="zh-TW", user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ))

        page.goto("https://www.wantgoo.com/", wait_until="domcontentloaded", timeout=30000)
        page.goto(WANTGOO_URL, wait_until="networkidle", timeout=45000)

        # 把 HTML 存起來，方便 debug
        with open(os.path.join(args.out_dir, "wantgoo_dom.html"), "w", encoding="utf-8") as f:
            f.write(page.content())

        candidates = extract_text_candidates(page)
        browser.close()

    brokers = filter_as_brokers(candidates, finmind_groups, args.threshold)

    if not brokers:
        # 存候選清單方便調 threshold
        pd.DataFrame({"candidate": candidates}).to_csv(os.path.join(args.out_dir, "wantgoo_candidates.csv"), index=False, encoding="utf-8-sig")
        raise RuntimeError(
            "抓不到券商清單（可能門檻太嚴或頁面結構變更）。\n"
            f"已輸出 wantgoo_dom.html 與 wantgoo_candidates.csv 到 {args.out_dir}，請調整 --threshold（例如 0.72）再試。"
        )

    today = datetime.now().strftime("%Y-%m-%d")

    df_brokers = pd.DataFrame({
        "wantgoo_broker_name": brokers,
        "seen_date": today,
        "source_url": WANTGOO_URL
    })
    broker_list_path = os.path.join(args.out_dir, "wantgoo_broker_list.csv")
    df_brokers.to_csv(broker_list_path, index=False, encoding="utf-8-sig")

    df_foreign = df_brokers.copy()
    df_foreign["is_foreign"] = df_foreign["wantgoo_broker_name"].apply(lambda x: "Y" if is_foreign_broker(x) else "N")
    df_foreign["reason"] = df_foreign["is_foreign"].apply(lambda x: "keyword" if x == "Y" else "")
    foreign_flag_path = os.path.join(args.out_dir, "wantgoo_foreign_flag.csv")
    df_foreign.to_csv(foreign_flag_path, index=False, encoding="utf-8-sig")

    print(f"✅ brokers={len(brokers)} -> {broker_list_path}")
    print(f"✅ foreign_flags={len(df_foreign)} -> {foreign_flag_path}")

if __name__ == "__main__":
    main()
