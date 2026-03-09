# 從現有 data/*_whale_track.json 建立 data/manifest.json（供靜態 dashboard / GitHub Pages 使用）
# 若已用 scraper_chip.py 跑過，scraper 會自動更新 manifest，不一定要執行本腳本。

import os
import json
import glob
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data")
MANIFEST_PATH = os.path.join(DATA_PATH, "manifest.json")

def main():
    os.makedirs(DATA_PATH, exist_ok=True)
    pattern = os.path.join(DATA_PATH, "*_whale_track.json")
    ids = []
    stocks = []
    for path in sorted(glob.glob(pattern)):
        base = os.path.basename(path)
        sid = base.replace("_whale_track.json", "").strip()
        if not sid.isdigit():
            continue
        ids.append(sid)
        name = sid
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            name = data.get("stock_name") or data.get("stock_id") or sid
        except Exception:
            pass
        stocks.append({"id": sid, "name": name})
    ids = sorted(set(ids), key=lambda x: int(x))
    manifest = {
        "stock_ids": ids,
        "stocks": stocks,
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"✅ 已寫入 {MANIFEST_PATH}，共 {len(ids)} 檔: {ids}")

if __name__ == "__main__":
    main()
