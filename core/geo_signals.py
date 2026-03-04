import pandas as pd
import statistics
from functools import lru_cache
from typing import Any, Dict, List, Tuple

from core.geo_utils import compute_geo_topn_features, haversine_km


SignalsDict = Dict[str, Any]


@lru_cache(maxsize=2)
def load_company_geo_map(
    tse_csv: str = "./rawdata/TSE_Company_V2.csv",
    otc_csv: str = "./rawdata/OTC_Company_V2.csv",
) -> Dict[str, Dict[str, str]]:
    """
    讀上市/上櫃公司地址與經緯度，回傳 stock_id -> {lat, lon, address, name}
    預設路徑可依你的專案：C:\\ngrok\\RB_DataMining\\rawdata\\...
    """
    out: Dict[str, Dict[str, str]] = {}
    for p in [tse_csv, otc_csv]:
        try:
            df = pd.read_csv(p, dtype=str, encoding="utf-8-sig").fillna("")
        except Exception:
            continue
        # 欄位：公司代號/公司簡稱/住址/Latitude/Longitude
        for _, r in df.iterrows():
            sid = str(r.get("公司代號", "")).strip()
            if not sid:
                continue
            out[sid] = {
                "name": str(r.get("公司簡稱", "")).strip(),
                "address": str(r.get("住址", "")).strip(),
                "lat": str(r.get("Latitude", "")).strip(),
                "lon": str(r.get("Longitude", "")).strip(),
            }
    return out


def _norm_city(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s = s.replace("臺", "台")
    s = s.replace("　", " ").replace("\u3000", " ")
    s = " ".join(s.split())
    return s


def _to_float_or_none(x: Any) -> float | None:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def compute_geo_signals(
    signals: SignalsDict,
    broker_map: Dict[str, Dict[str, Any]],
    stock_id: str,
    top6_details: List[Dict[str, Any]],
) -> Tuple[SignalsDict, List[Dict[str, Any]]]:
    """
    封裝 Geo 相關計算：
    - 依據公司 HQ 與 top_buy_15 計算 geo_top5_* 與 geo_affinity_score
    - 若必要時補 geo_top5_detail / geo_top5_wavg_km / geo_affinity_score
    - 計算 geo_zscore / geo_grade / geo_tag / geo_baseline_* 等
    - 幫 top6_details 補上 km_to_hq
    """
    # 1) 讀取公司 HQ geo
    company_map = load_company_geo_map()
    cmeta = company_map.get(str(stock_id).strip(), {}) or {}
    hq_lat: float | None = None
    hq_lon: float | None = None
    try:
        hq_lat = (
            float(cmeta.get("lat", "") or 0)
            if str(cmeta.get("lat", "")).strip()
            else None
        )
        hq_lon = (
            float(cmeta.get("lon", "") or 0)
            if str(cmeta.get("lon", "")).strip()
            else None
        )
    except Exception:
        hq_lat, hq_lon = None, None

    # 2) 主 Geo 特徵（TopN 買超 vs HQ）
    geo_pack = compute_geo_topn_features(
        top_rows=signals.get("top_buy_15", []) or [],
        hq_lat=hq_lat,
        hq_lon=hq_lon,
        broker_map=broker_map,
        top_n=5,
    )
    signals.update(geo_pack)

    # 3) fallback: 若缺少 geo_top5_detail / wavg_km / affinity 則補
    try:
        top_buy = signals.get("top_buy_15") or []
        need_detail = (not isinstance(signals.get("geo_top5_detail"), list)) or (
            len(signals.get("geo_top5_detail") or []) == 0
        )

        if need_detail:
            geo_top5: List[Dict[str, Any]] = []
            for r in top_buy:
                if not isinstance(r, dict):
                    continue
                bid = str(r.get("broker_id", "")).strip()
                if not bid:
                    continue

                meta = broker_map.get(bid) or {}
                blat = meta.get("lat", None)
                blon = meta.get("lon", None)

                if blat is None or blon is None:
                    continue

                city = str(meta.get("city", "") or "").strip()
                orgt = str(meta.get("broker_org_type", "") or "").strip()

                km = None
                try:
                    if hq_lat is not None and hq_lon is not None:
                        km = float(
                            haversine_km(
                                float(hq_lat),
                                float(hq_lon),
                                float(blat),
                                float(blon),
                            )
                        )
                except Exception:
                    km = None

                geo_top5.append(
                    {
                        "broker_id": bid,
                        "broker_name": r.get("broker_name") or meta.get("broker_name") or "",
                        "net_lot": float(r.get("net_lot", 0) or 0),
                        "city": city,
                        "broker_org_type": orgt,
                        "lat": blat,
                        "lon": blon,
                        "km_to_hq": km,
                    }
                )

                if len(geo_top5) >= 5:
                    break

            signals["geo_top5_detail"] = geo_top5

        if signals.get("geo_top5_wavg_km") is None:
            d = signals.get("geo_top5_detail") or []
            kms = [
                x.get("km_to_hq")
                for x in d
                if isinstance(x, dict) and x.get("km_to_hq") is not None
            ]
            if kms:
                signals["geo_top5_wavg_km"] = round(sum(kms) / len(kms), 2)

        if signals.get("geo_affinity_score") is None:
            d = signals.get("geo_top5_detail") or []
            cities = [
                str(x.get("city") or "").strip()
                for x in d
                if isinstance(x, dict) and str(x.get("city") or "").strip()
            ]
            if cities:
                major = max(set(cities), key=cities.count)
                signals["geo_affinity_score"] = round(
                    100.0 * (cities.count(major) / len(cities)), 1
                )

    except Exception:
        # 出錯時盡量保持既有欄位，不中斷主流程
        pass

    # 4) Geo baseline + ZScore + Grade/Tag
    try:
        top_rows = signals.get("top_buy_15", []) or []
        buys = [r for r in top_rows if float(r.get("net_lot", 0) or 0) > 0][:5]

        def _wmode(rows: List[Dict[str, Any]], key: str) -> str:
            acc: Dict[str, float] = {}
            for r in rows:
                w = abs(float(r.get("net_lot", 0) or 0))
                v = str(r.get(key, "") or "").strip()
                if not v:
                    continue
                if key == "city":
                    v = _norm_city(v)
                acc[v] = acc.get(v, 0.0) + w
            if not acc:
                return ""
            return sorted(acc.items(), key=lambda x: x[1], reverse=True)[0][0]

        norm: List[Dict[str, Any]] = []
        for r in buys:
            bid = str(r.get("broker_id", "")).strip()
            meta = broker_map.get(bid, {}) if bid else {}
            nr = dict(r)

            if not str(nr.get("broker_org_type", "") or "").strip():
                nr["broker_org_type"] = str(meta.get("broker_org_type", "") or "").strip()

            if not str(nr.get("city", "") or "").strip():
                nr["city"] = _norm_city(str(meta.get("city", "") or ""))
            else:
                nr["city"] = _norm_city(str(nr.get("city", "") or ""))

            norm.append(nr)

        target_org = _wmode(norm, "broker_org_type")
        target_city = _norm_city(_wmode(norm, "city"))

        def _iter_candidates(mode: str):
            for _, meta in (broker_map or {}).items():
                meta = meta or {}
                blat = _to_float_or_none(meta.get("lat"))
                blon = _to_float_or_none(meta.get("lon"))
                if blat is None or blon is None:
                    continue

                org = str(meta.get("broker_org_type", "") or "").strip()
                city = _norm_city(str(meta.get("city", "") or ""))

                if mode == "org_city":
                    if (target_org and org != target_org) or (
                        target_city and city != target_city
                    ):
                        continue
                elif mode == "org":
                    if target_org and org != target_org:
                        continue

                yield (blat, blon)

        def _baseline_stats(mode: str):
            ds: List[float] = []
            if hq_lat is None or hq_lon is None:
                return None
            for blat, blon in _iter_candidates(mode):
                d = haversine_km(float(hq_lat), float(hq_lon), float(blat), float(blon))
                if d is not None:
                    ds.append(float(d))
            if len(ds) < 30:
                return None
            mu = statistics.mean(ds)
            sd = statistics.pstdev(ds)
            if sd <= 1e-9:
                sd = 1.0
            return (mu, sd, len(ds))

        baseline_tag = "baseline_all"
        baseline_w = 0.70

        chosen = _baseline_stats("org_city")
        if chosen is not None:
            baseline_tag = f"baseline_org_city:{target_org or 'na'}_{target_city or 'na'}"
            baseline_w = 1.00
        else:
            chosen = _baseline_stats("org")
            if chosen is not None:
                baseline_tag = f"baseline_org:{target_org or 'na'}"
                baseline_w = 0.85
            else:
                chosen = _baseline_stats("all")
                baseline_tag = "baseline_all"
                baseline_w = 0.70

        wavg_km = float(signals.get("geo_top5_wavg_km", 0) or 0)
        if chosen is not None and wavg_km > 0:
            mu, sd, n = chosen
            z = (wavg_km - mu) / sd
            signals["geo_zscore"] = round(z, 2)
            signals["geo_baseline_mu_km"] = round(mu, 2)
            signals["geo_baseline_sd_km"] = round(sd, 2)
            signals["geo_baseline_n"] = int(n)
        else:
            signals["geo_zscore"] = None

        signals["geo_baseline_tag"] = baseline_tag
        signals["geo_baseline_weight"] = round(float(baseline_w), 2)

        z_val = signals.get("geo_zscore", None)
        try:
            z_val = float(z_val) if z_val is not None else None
        except Exception:
            z_val = None

        if wavg_km > 0 and wavg_km <= 8:
            grade, tag = "A", "geo_near_core"
        elif wavg_km > 8 and wavg_km <= 15:
            grade, tag = "B", "geo_near"
        elif wavg_km > 15 and wavg_km <= 30:
            grade, tag = "C", "geo_mid"
        elif wavg_km > 30:
            grade, tag = "D", "geo_far"
        else:
            grade, tag = "NA", "geo_na"

        if grade in ("B", "C", "D"):
            if wavg_km > 0 and wavg_km <= 5:
                grade, tag = "A", "geo_ultra_near"
            elif (wavg_km > 0 and wavg_km <= 12) and (
                z_val is not None and z_val <= -1.2
            ):
                grade, tag = "A", "geo_strong_by_z"

        if z_val is not None and grade in ("A", "B", "C", "D"):
            if z_val <= -1.0 and grade != "A":
                grade = {"D": "C", "C": "B", "B": "A"}.get(grade, grade)
                tag = f"{tag}_boost_z"
            elif z_val >= 1.0 and grade != "D":
                grade = {"A": "B", "B": "C", "C": "D"}.get(grade, grade)
                tag = f"{tag}_penalty_z"

        if baseline_tag:
            tag = f"{tag}|{baseline_tag}"

        signals["geo_grade"] = grade
        signals["geo_tag"] = tag

    except Exception:
        signals.setdefault("geo_zscore", None)
        signals.setdefault("geo_grade", None)
        signals.setdefault("geo_tag", "geo_error")
        signals.setdefault("geo_baseline_tag", "baseline_error")
        signals.setdefault("geo_baseline_weight", 0.70)

    # 5) Top6 詳細也補上距離（給 dashboard drill-down）
    if hq_lat is not None and hq_lon is not None:
        for r in top6_details:
            bid = str(r.get("broker_id", "")).strip()
            meta = broker_map.get(bid, {}) or {}
            blat = _to_float_or_none(meta.get("lat"))
            blon = _to_float_or_none(meta.get("lon"))
            if blat is not None and blon is not None:
                r["km_to_hq"] = round(
                    haversine_km(hq_lat, hq_lon, blat, blon),
                    1,
                )
            else:
                r["km_to_hq"] = None

    return signals, top6_details

