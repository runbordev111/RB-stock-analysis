import os
import re
import pandas as pd

DATA_PATH = r"C:\ngrok\RB_DataMining\data"
XLS_PATH  = os.path.join(DATA_PATH, "Securities Firm_2026.xls")
OUT_PATH  = os.path.join(DATA_PATH, "broker_master_city.csv")

def extract_city(addr: str) -> str:
    """
    從地址抓縣市：例如 台北市 / 新北市 / 桃園市 / 台中市 / 台南市 / 高雄市 / 新竹縣 / 新竹市...
    若地址格式不完整則回傳空字串。
    """
    if not isinstance(addr, str):
        return ""
    a = addr.strip().replace("臺", "台")
    m = re.match(r"^(.{1,4}[縣市])", a)
    return m.group(1) if m else ""

def main():
    df = pd.read_excel(XLS_PATH)

    # 下面欄位名稱以「常見版本」為主，若你 xls 欄位名不一樣，改這三個 key 即可
    id_col = "證券商代號"
    name_col = "證券商名稱"
    addr_col = "地址"

    df["broker_id"] = df[id_col].astype(str).str.strip()
    df["broker_name_master"] = df[name_col].astype(str).str.strip()
    df["city"] = df[addr_col].astype(str).apply(extract_city)

    out = df[["broker_id", "broker_name_master", "city"]].copy()
    out = out[out["broker_id"] != ""]
    out.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
    print(f"✅ 產出完成：{OUT_PATH} | rows={len(out)}")

if __name__ == "__main__":
    main()
