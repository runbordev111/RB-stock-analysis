import os
import pandas as pd

BASE_DIR = r"C:\ngrok\RB_DataMining\data"
MASTER = os.path.join(BASE_DIR, "broker_master_city.csv")
JSON_FILE = os.path.join(BASE_DIR, "6239_whale_track.json")  # 你可換股票代號
OUT = os.path.join(BASE_DIR, "6239_city_heatmap.csv")

def main():
    # 主檔：broker_id -> city
    mdf = pd.read_csv(MASTER, dtype=str).fillna("")
    city_map = {r["broker_id"].strip(): r["city"].strip() for _, r in mdf.iterrows()}

    # 讀 top6_details（直接用你產出的 boss_list CSV 也可以）
    import json
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        js = json.load(f)

    top6 = pd.DataFrame(js.get("top6_details", []))
    if top6.empty:
        print("❌ top6_details 為空，請先跑 scraper_chip.py 產生 JSON")
        return

    top6["broker_id"] = top6["broker_id"].astype(str).str.strip()
    top6["city"] = top6["broker_id"].map(city_map).fillna("")

    # 熱區彙總（以 Top6 為例）
    agg = (top6.groupby("city", dropna=False)
               .agg(
                    brokers=("broker_id", "count"),
                    sum_net_10d=("net_10d", "sum"),
                    sum_net_5d=("net_5d", "sum"),
                    sum_net_1d=("net_1d", "sum"),
               )
               .reset_index()
          )

    # 空 city 改成 Unknown
    agg["city"] = agg["city"].replace("", "Unknown")

    # 排序：先看 10d 吸籌力道
    agg = agg.sort_values(["sum_net_10d", "sum_net_5d"], ascending=False)

    agg.to_csv(OUT, index=False, encoding="utf-8-sig")
    print(f"✅ 輸出完成：{OUT}")

if __name__ == "__main__":
    main()
