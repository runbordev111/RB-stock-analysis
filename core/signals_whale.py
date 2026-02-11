import math
import pandas as pd


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()

    if "securities_trader_id" in df.columns and "broker_id" not in df.columns:
        df.rename(columns={"securities_trader_id": "broker_id"}, inplace=True)
    if "securities_trader" in df.columns and "broker_name" not in df.columns:
        df.rename(columns={"securities_trader": "broker_name"}, inplace=True)

    need = {"date", "broker_id", "broker_name", "buy", "sell"}
    if not need.issubset(set(df.columns)):
        return pd.DataFrame()

    df["broker_id"] = df["broker_id"].astype(str)
    df["broker_name"] = df["broker_name"].astype(str)

    df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0)
    df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0)
    df["net"] = df["buy"] - df["sell"]

    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)

    return df


def calc_breadth(df_period: pd.DataFrame) -> dict:
    """
    df_period: 必須包含 broker_id, broker_name, net
    回傳買超家數/賣超家數/廣度比
    """
    if df_period is None or df_period.empty:
        return {"buy_count": 0, "sell_count": 0, "breadth": 0, "breadth_ratio": 0.0}

    g = df_period.groupby(["broker_id", "broker_name"], as_index=False)["net"].sum()
    buy_cnt = int((g["net"] > 0).sum())
    sell_cnt = int((g["net"] < 0).sum())
    total = buy_cnt + sell_cnt
    ratio = round(buy_cnt / total, 4) if total > 0 else 0.0
    return {"buy_count": buy_cnt, "sell_count": sell_cnt, "breadth": buy_cnt - sell_cnt, "breadth_ratio": ratio}


def compute_concentration(df_period: pd.DataFrame, top_n: int = 15) -> float:
    """
    集中度：TopN（以 net 彙總排序的買方）之 buy 量 / 全市場 buy 量
    """
    if df_period is None or df_period.empty:
        return 0.0
    total_buy = float(df_period["buy"].sum())
    if total_buy <= 0:
        return 0.0

    agg = df_period.groupby(["broker_id", "broker_name"], as_index=False).agg(
        net=("net", "sum"),
        buy=("buy", "sum"),
        sell=("sell", "sum"),
    )
    top_buyers = agg.sort_values("net", ascending=False).head(top_n)
    top_buy = float(top_buyers["buy"].sum())
    return round((top_buy / total_buy) * 100, 2)


def compute_streaks(net_matrix: pd.DataFrame) -> dict:
    """
    net_matrix: index=date, columns=broker_id, values=net (股數)
    回傳 dict broker_id -> (streak_buy, streak_sell) 連續到最後一日
    """
    if net_matrix is None or net_matrix.empty:
        return {}

    dates = list(net_matrix.index)
    last_i = len(dates) - 1
    out = {}

    for bid in net_matrix.columns:
        s = net_matrix[bid].fillna(0).astype(float).tolist()

        sb = 0
        for i in range(last_i, -1, -1):
            if s[i] > 0:
                sb += 1
            else:
                break

        ss = 0
        for i in range(last_i, -1, -1):
            if s[i] < 0:
                ss += 1
            else:
                break

        out[str(bid)] = {"streak_buy": sb, "streak_sell": ss}

    return out


def build_top15_tables(df_20d: pd.DataFrame, broker_map: dict, date_list: list[str]) -> dict:
    """
    產生 signals.top_buy_15 / top_sell_15（含連買/連賣、城市、外本）
    """
    if df_20d is None or df_20d.empty:
        return {"top_buy_15": [], "top_sell_15": []}

    agg = df_20d.groupby(["broker_id", "broker_name"], as_index=False).agg(
        buy=("buy", "sum"),
        sell=("sell", "sum"),
        net=("net", "sum"),
    )
    agg["net_lot"] = (agg["net"] / 1000.0).round(1)

    pivot = df_20d.pivot_table(index="date", columns="broker_id", values="net", aggfunc="sum").fillna(0)
    pivot = pivot.reindex(date_list).fillna(0)
    streaks = compute_streaks(pivot)

    def enrich_row(r: pd.Series) -> dict:
        bid = str(r["broker_id"])
        meta = broker_map.get(bid, {})
        org = meta.get("broker_org_type", "unknown") or "unknown"
        city = meta.get("city", "")
        st = streaks.get(bid, {"streak_buy": 0, "streak_sell": 0})
        return {
            "broker_id": bid,
            "broker_name": str(r["broker_name"]),
            "net_lot": float(r["net_lot"]),
            "city": city,
            "broker_org_type": org,
            "streak_buy": int(st.get("streak_buy", 0)),
            "streak_sell": int(st.get("streak_sell", 0)),
        }

    top_buy = agg.sort_values("net", ascending=False).head(15).copy()
    top_sell = agg.sort_values("net", ascending=True).head(15).copy()

    top_buy_15 = [enrich_row(r) for _, r in top_buy.iterrows()]
    top_sell_15 = [enrich_row(r) for _, r in top_sell.iterrows()]

    for x in top_sell_15:
        x["net_lot"] = -abs(float(x["net_lot"]))

    return {"top_buy_15": top_buy_15, "top_sell_15": top_sell_15}


def compute_foreign_local_net(df_period: pd.DataFrame, broker_map: dict) -> dict:
    """
    以 broker_org_type 分 foreign/local，計算期間 net（股數）
    """
    if df_period is None or df_period.empty:
        return {"foreign_net": 0.0, "local_net": 0.0}

    tmp = df_period[["broker_id", "net"]].copy()
    tmp["broker_id"] = tmp["broker_id"].astype(str)
    tmp["org"] = tmp["broker_id"].map(lambda x: (broker_map.get(x, {}).get("broker_org_type", "unknown") or "unknown"))

    foreign_net = float(tmp[tmp["org"] == "foreign"]["net"].sum())
    local_net = float(tmp[tmp["org"] == "local"]["net"].sum())
    return {"foreign_net": foreign_net, "local_net": local_net}


def build_breadth_series(df_all: pd.DataFrame, date_list: list[str]) -> dict:
    labels, buy_series, sell_series, breadth_series, ratio_series = [], [], [], [], []

    if df_all is None or df_all.empty or not date_list:
        return {
            "labels_20d": [],
            "buy_count_series_20d": [],
            "sell_count_series_20d": [],
            "breadth_series_20d": [],
            "breadth_ratio_series_20d": [],
        }

    for d in date_list:
        df_day = df_all[df_all["date"] == d]
        b = calc_breadth(df_day)
        labels.append(d[5:])  # MM-DD
        buy_series.append(int(b["buy_count"]))
        sell_series.append(int(b["sell_count"]))
        breadth_series.append(int(b["breadth"]))
        ratio_series.append(float(b["breadth_ratio"]))

    return {
        "labels_20d": labels,
        "buy_count_series_20d": buy_series,
        "sell_count_series_20d": sell_series,
        "breadth_series_20d": breadth_series,
        "breadth_ratio_series_20d": ratio_series,
    }


def compute_master_trend(signals: dict) -> dict:
    """
    主力走向新規則（集中度 + 廣度 + 外/本分歧）
    回傳：score(0~100), trend(文字), tags(list)
    """
    c5 = float(signals.get("concentration_5d", 0) or 0)
    c20 = float(signals.get("concentration_20d", 0) or 0)
    d5 = float(signals.get("delta_major_flow_5", 0) or 0)
    d20 = float(signals.get("delta_major_flow_20", 0) or 0)
    br5 = float(signals.get("breadth_ratio_5d", 0) or 0)
    br20 = float(signals.get("breadth_ratio_20d", 0) or 0)
    f5 = float(signals.get("foreign_net_5d", 0) or 0)
    l5 = float(signals.get("local_net_5d", 0) or 0)

    s_c5 = clamp(c5 / 6.0, 0.0, 1.0)
    s_c20 = clamp(c20 / 10.0, 0.0, 1.0)
    strength = 0.55 * s_c5 + 0.45 * s_c20

    # 動態尺度：跟股票主力量級走，避免永遠很小
    scale5 = max(50.0, abs(d20) / 6.0)
    scale20 = max(80.0, abs(d20) / 3.0)
    dir5 = math.tanh(d5 / scale5)
    dir20 = math.tanh(d20 / scale20)
    direction = 0.6 * dir5 + 0.4 * dir20

    b5 = clamp((br5 - 0.5) * 2.0, -1.0, 1.0)
    b20 = clamp((br20 - 0.5) * 2.0, -1.0, 1.0)
    breadth = 0.6 * b5 + 0.4 * b20

    if f5 == 0 and l5 == 0:
        div = 0.0
    else:
        same = (f5 >= 0 and l5 >= 0) or (f5 <= 0 and l5 <= 0)
        denom = abs(f5) + abs(l5)
        gap = abs(abs(f5) - abs(l5)) / denom if denom > 0 else 0.0
        div = (1.0 - gap) if same else -(0.5 + 0.5 * gap)

    score = (
        40.0 * strength +
        25.0 * ((direction + 1.0) / 2.0) +
        25.0 * ((breadth + 1.0) / 2.0) +
        10.0 * ((div + 1.0) / 2.0)
    )
    score = int(round(clamp(score, 0.0, 100.0)))

    # -----------------------------------------------------------------
    # === Radar components (0~100) ===  # strength: already 0~1
    rad_strength = int(round(clamp(strength, 0.0, 1.0) * 100))

    # direction/breadth/div: -1~1 -> 0~100
    rad_direction = int(round(clamp((direction + 1.0) / 2.0, 0.0, 1.0) * 100))
    rad_breadth   = int(round(clamp((breadth   + 1.0) / 2.0, 0.0, 1.0) * 100))
    rad_align     = int(round(clamp((div       + 1.0) / 2.0, 0.0, 1.0) * 100))

    # acceleration: short-term vs mid-term
    acc_raw = clamp(dir5 - dir20, -1.0, 1.0)  # dir5/dir20 在上面已算
    rad_acc = int(round(((acc_raw + 1.0) / 2.0) * 100))

    radar = {
        "labels": ["集中度", "淨買方向", "買盤廣度", "外本協同", "短期加速"],
        "values": [rad_strength, rad_direction, rad_breadth, rad_align, rad_acc],
        # 可選：保留原始值方便 debug
        "raw": {
            "strength": strength,
            "direction": direction,
            "breadth": breadth,
            "align": div,
            "acc": acc_raw,
        }
    }
    # -----------------------------------------------------------------

    if c20 < 4.0 and c5 < 4.0:
        return {"score": score, "trend": "觀察中", "tags": ["集中度偏低"], "whale_radar": radar}

    tags = []
    if div > 0.3:
        tags.append("外/本協同")
    elif div < -0.3:
        tags.append("外/本分歧")

    if breadth > 0.25:
        tags.append("買盤擴散")
    elif breadth < -0.25:
        tags.append("賣盤擴散")

    if direction > 0.25:
        tags.append("淨買主導")
    elif direction < -0.25:
        tags.append("淨賣主導")

    if strength >= 0.70 and direction > 0.20 and breadth > 0.10:
        trend = "多" if div > 0.10 else "偏多"
        tags.append("強度高")
    elif strength >= 0.70 and direction < -0.20 and breadth < -0.10:
        trend = "空" if div > 0.10 else "偏空"
        tags.append("強度高")
    elif direction > 0.10 and breadth > 0.10 and strength >= 0.45:
        trend = "吸籌"
        tags.append("中等強度")
    elif direction < -0.10 and breadth < -0.10 and strength >= 0.45:
        trend = "派發"
        tags.append("中等強度")
    else:
        trend = "震盪"
        tags.append("方向拉扯")

    return {"score": score, "trend": trend, "tags": tags, "whale_radar": radar}

