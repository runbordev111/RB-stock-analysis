# geocode_tse_company_csv.py
# Input : TSE_Company_V1.csv  (address in column D)
# Output: TSE_Company_V2.csv  (Latitude in H, Longitude in I)
#
# API Key: load from .env at C:\ngrok\RB_DataMining\.env (GOOGLE_MAPS_API_KEY=xxxx)
#
# Install:
#   pip install requests python-dotenv
#
# Run:
#   python geocode_tse_company_csv.py

import os
import csv
import json
import time
import argparse
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

import requests
from dotenv import load_dotenv

GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
DEFAULT_ENV_PATH = r"C:\ngrok\RB_DataMining\.env"


def geocode_google(address: str, api_key: str, session: requests.Session,
                   region: str = "tw", language: str = "zh-TW") -> Dict[str, Any]:
    params = {"address": address, "key": api_key, "region": region, "language": language}
    r = session.get(GEOCODE_URL, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def extract_lat_lng(resp: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], str]:
    status = resp.get("status", "ERROR")
    if status == "OK" and resp.get("results"):
        loc = resp["results"][0]["geometry"]["location"]
        return float(loc["lat"]), float(loc["lng"]), status
    return None, None, status


def load_cache(cache_path: Path) -> Dict[str, Dict[str, Any]]:
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_cache(cache_path: Path, cache: Dict[str, Dict[str, Any]]) -> None:
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_addr(s: str) -> str:
    return " ".join((s or "").strip().split())


def ensure_len(row: list, n: int) -> list:
    if len(row) < n:
        row.extend([""] * (n - len(row)))
    return row


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="in_path", default=r"C:\ngrok\RB_DataMining\rawdata\TSE_Company_V1.csv")
    parser.add_argument("--out", dest="out_path", default=r"C:\ngrok\RB_DataMining\rawdata\TSE_Company_V2.csv")
    parser.add_argument(
        "--env",
        dest="env_path",
        default=DEFAULT_ENV_PATH,
        help=r"Path to .env (default: C:\ngrok\RB_DataMining\.env)"
    )
    parser.add_argument("--sleep", type=float, default=0.12)
    parser.add_argument("--max_retries", type=int, default=5)
    parser.add_argument("--retry_backoff", type=float, default=1.6)
    parser.add_argument("--force", action="store_true", help="Force re-geocode even if H/I already filled")
    args = parser.parse_args()

    load_dotenv(dotenv_path=args.env_path, override=False)

    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        raise SystemExit(f"Missing GOOGLE_MAPS_API_KEY. Put it in {args.env_path} or set env var.")

    in_path = Path(args.in_path)
    if not in_path.exists():
        raise SystemExit(f"Input file not found: {in_path}")

    out_path = Path(args.out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cache_path = out_path.with_suffix(".geocode_cache.json")
    cache = load_cache(cache_path)

    # D=4 address, H=8 lat, I=9 lng (1-based)
    IDX_ADDR = 4 - 1
    IDX_LAT = 8 - 1
    IDX_LNG = 9 - 1

    session = requests.Session()

    updated = 0
    skipped = 0
    failed = 0

    with in_path.open("r", encoding="utf-8-sig", newline="") as f_in:
        rows = list(csv.reader(f_in))

    if not rows:
        raise SystemExit("Input CSV is empty.")

    header = ensure_len(rows[0], 9)
    data_rows = rows[1:]

    if header[IDX_LAT] in (None, ""):
        header[IDX_LAT] = "Latitude"
    if header[IDX_LNG] in (None, ""):
        header[IDX_LNG] = "Longitude"

    out_rows = [header]

    for row in data_rows:
        row = ensure_len(row, 9)

        raw_addr = row[IDX_ADDR] if IDX_ADDR < len(row) else ""
        if raw_addr is None or str(raw_addr).strip() == "":
            skipped += 1
            out_rows.append(row)
            continue

        addr = normalize_addr(str(raw_addr))

        has_lat = str(row[IDX_LAT]).strip() != ""
        has_lng = str(row[IDX_LNG]).strip() != ""
        if (has_lat and has_lng) and (not args.force):
            skipped += 1
            out_rows.append(row)
            continue

        if addr in cache and cache[addr].get("status") == "OK" and (not args.force):
            row[IDX_LAT] = cache[addr]["lat"]
            row[IDX_LNG] = cache[addr]["lng"]
            updated += 1
            out_rows.append(row)
            continue

        attempt = 0
        while True:
            attempt += 1
            try:
                resp = geocode_google(addr, api_key, session=session)
                lat, lng, status = extract_lat_lng(resp)

                cache[addr] = {
                    "status": status,
                    "lat": lat,
                    "lng": lng,
                    "timestamp": time.time(),
                }

                if status == "OK" and lat is not None and lng is not None:
                    row[IDX_LAT] = lat
                    row[IDX_LNG] = lng
                    updated += 1
                else:
                    failed += 1
                break

            except requests.HTTPError as e:
                if attempt >= args.max_retries:
                    cache[addr] = {"status": "HTTP_ERROR", "error": str(e), "timestamp": time.time()}
                    failed += 1
                    break
                time.sleep(args.retry_backoff ** attempt)

            except Exception as e:
                if attempt >= args.max_retries:
                    cache[addr] = {"status": "ERROR", "error": str(e), "timestamp": time.time()}
                    failed += 1
                    break
                time.sleep(args.retry_backoff ** attempt)

        if (updated + failed) % 50 == 0:
            save_cache(cache_path, cache)

        time.sleep(args.sleep)
        out_rows.append(row)

    save_cache(cache_path, cache)

    with out_path.open("w", encoding="utf-8-sig", newline="") as f_out:
        csv.writer(f_out).writerows(out_rows)

    print(f"Done. Saved: {out_path}")
    print(f"Updated: {updated}, Skipped: {skipped}, Failed: {failed}")
    print(f"Cache: {cache_path}")


if __name__ == "__main__":
    main()
