import os
import pandas as pd


def _read_broker_master_any(data_path: str) -> pd.DataFrame:
    """
    優先順序：
      1) broker_dimensions_master_V3.xlsx (sheet: broker_master_enriched)  ← 含 Latitude/Longitude
      2) broker_master_enriched.csv
    """
    xlsx_path = os.path.join(data_path, "broker_dimensions_master_V3.xlsx")
    if os.path.exists(xlsx_path):
        try:
            df = pd.read_excel(
                xlsx_path,
                sheet_name="broker_master_enriched",
                dtype=str,
            ).fillna("")
            return df
        except Exception as e:
            print(f"⚠️ 讀取 {xlsx_path} 失敗: {repr(e)}")

    csv_path = os.path.join(data_path, "broker_master_enriched.csv")
    if os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig").fillna("")
            return df
        except Exception as e:
            print(f"⚠️ 讀取 {csv_path} 失敗: {repr(e)}")

    return pd.DataFrame()


def load_broker_master_enriched(data_path: str) -> dict:
    """
    回傳：
      broker_id -> {
        city, broker_org_type, is_proprietary, seat_type,
        broker_name(optional),
        address(optional), phone(optional),
        lat(optional), lon(optional)
      }
    """
    df = _read_broker_master_any(data_path)
    if df is None or df.empty:
        print("⚠️ 找不到券商主檔（broker_dimensions_master_V3.xlsx 或 broker_master_enriched.csv），券商資訊將顯示為空")
        return {}

    keep_cols = set(df.columns)
    out = {}

    # Latitude/Longitude 欄名可能是 Latitude/Longitude 或 lat/lon
    lat_col = "Latitude" if "Latitude" in keep_cols else ("lat" if "lat" in keep_cols else "")
    lon_col = "Longitude" if "Longitude" in keep_cols else ("lon" if "lon" in keep_cols else "")

    for _, r in df.iterrows():
        bid = str(r.get("broker_id", "")).strip()
        if not bid:
            continue

        meta = {
            "city": str(r.get("city", "")).strip(),
            "broker_org_type": str(r.get("broker_org_type", "")).strip() or "unknown",  # foreign/local/unknown
            "is_proprietary": str(r.get("is_proprietary", "")).strip(),
            "seat_type": str(r.get("seat_type", "")).strip(),
        }

        if "broker_name" in keep_cols:
            meta["broker_name"] = str(r.get("broker_name", "")).strip()

        if "address" in keep_cols:
            meta["address"] = str(r.get("address", "")).strip()

        if "phone" in keep_cols:
            meta["phone"] = str(r.get("phone", "")).strip()

        if lat_col:
            meta["lat"] = str(r.get(lat_col, "")).strip()

        if lon_col:
            meta["lon"] = str(r.get(lon_col, "")).strip()

        out[bid] = meta

    return out
