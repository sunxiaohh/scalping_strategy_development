#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Event-Driven Scalper – No Time Buckets
======================================
• DuckDB → Event stream (depth + quotes) → L1/Lk snapshots (event granularity)
• Pure event-driven triggers (BOE/VAC/AGG), no fixed time windows
• Queue-advance execution: fill = same-price trade-through + same-price cancels
• Hybrid entry: taker only when edge is very large; otherwise maker/pullback
• Exits: STOP / TRAIL / TP / VI_REV (opt) / TIME_STOP / TIME
• Storm protection & RTH filter
• Per-trade edge logging for "edge − cost" diagnostics

Deps: duckdb, pandas, numpy, matplotlib
"""

from __future__ import annotations
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
import argparse
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
import json
from zoneinfo import ZoneInfo
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("gt-dom-enhanced")

# ===== constants (ES default) =====
TICK_SIZE = 0.25
MULTIPLIER = 50.0
RT_COST = 2.8

# DomType (from your enum)
DOM_ASK = 1
DOM_BID = 2
DOM_BESTASK = 3
DOM_BESTBID = 4
DOM_TRADE = 5
DOM_RESET = 6
DOM_NEW_BID = 9
DOM_NEW_ASK = 10

# ----------------------------------
# Data loading
# ----------------------------------
def load_topstep_data(db_path: str, start_time: str = None, end_time: str = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load raw 'depth' and 'quotes' (Topstep DuckDB). Returns (depth_df, quotes_df) in UTC."""
    logger.info(f"加载Topstep数据: {db_path}")
    conn = duckdb.connect(db_path)

    tf = []
    if start_time:
        tf.append(f"src_ts_utc >= '{start_time}'")
    if end_time:
        tf.append(f"src_ts_utc <= '{end_time}'")
    where_t = (" AND " + " AND ".join(tf)) if tf else ""

    depth_sql = f"""
    SELECT
        CAST(src_ts_utc AS TIMESTAMP) AS ts,
        price::DOUBLE AS price,
        volume::DOUBLE AS volume,
        dom_type::INTEGER AS dom_type,
        current_volume::DOUBLE AS current_volume
    FROM depth
    WHERE price > 0 {where_t}
    ORDER BY src_ts_utc
    """
    quotes_sql = f"""
    SELECT
        CAST(src_ts_utc AS TIMESTAMP) AS ts,
        best_bid::DOUBLE AS best_bid,
        best_ask::DOUBLE AS best_ask,
        last_price::DOUBLE AS last_price,
        volume::DOUBLE AS volume
    FROM quotes
    WHERE 1=1 {where_t}
    ORDER BY src_ts_utc
    """
    depth_df = conn.execute(depth_sql).df()
    quotes_df = conn.execute(quotes_sql).df()
    conn.close()

    depth_df["ts"] = pd.to_datetime(depth_df["ts"], utc=True)
    quotes_df["ts"] = pd.to_datetime(quotes_df["ts"], utc=True)
    logger.info(f"深度数据: {len(depth_df)} 条, 报价数据: {len(quotes_df)} 条")
    return depth_df, quotes_df


# ----------------------------------
# Orderbook reconstruction (event-driven snapshots)
# ----------------------------------
def reconstruct_order_book_with_depth(
    depth_df: pd.DataFrame,
    quotes_df: pd.DataFrame,
    max_depth: int = 10,
    quote_default_l1_size: float = 2.0,
) -> pd.DataFrame:
    """
    Event-driven reconstruction of L1/Lk with per-snapshot:
      - best_bid / best_ask, spread, mid, L1 sizes, Lk ladders (counts)
      - trade_through_bid/ask (within snapshot)
      - L1 cancel at best (same-price drop not explained by trades)
      - microprice, vi, holds
      - flags: bid_l1_from_quote / ask_l1_from_quote
    """
    logger.info(f"开始重建订单簿(事件驱动，最大深度 {max_depth})")

    bids: Dict[float, float] = {}
    asks: Dict[float, float] = {}
    best_bid = np.nan
    best_ask = np.nan

    # source flags per price level
    bid_from_quote: Dict[float, bool] = {}
    ask_from_quote: Dict[float, bool] = {}

    # per-snapshot accumulators
    ttb_acc = 0.0  # trades at best bid
    tta_acc = 0.0  # trades at best ask

    prev_bid_top = np.nan
    prev_ask_top = np.nan
    prev_best_bid = np.nan
    prev_best_ask = np.nan
    bid_hold = 0
    ask_hold = 0

    depth_df = depth_df.copy(); depth_df["etype"] = "depth"
    quotes_df = quotes_df.copy(); quotes_df["etype"] = "quote"

    # pad missing cols to unify
    depth_df["best_bid"] = np.nan; depth_df["best_ask"] = np.nan; depth_df["last_price"] = np.nan
    quotes_df["price"] = np.nan; quotes_df["dom_type"] = np.nan; quotes_df["current_volume"] = np.nan

    cols = ["ts","etype","price","volume","dom_type","current_volume","best_bid","best_ask","last_price"]
    all_e = pd.concat([depth_df[cols].fillna(np.nan), quotes_df[cols].fillna(np.nan)], ignore_index=True)
    all_e = all_e.sort_values("ts").reset_index(drop=True)

    def prune_sides():
        nonlocal bids, asks, bid_from_quote, ask_from_quote
        if best_bid==best_bid:
            # Only prune if we have many levels to avoid overhead
            if len(bids) > max_depth * 2:
                to_remove = [p for p in bids.keys() if p < best_bid - 1e-12]
                for p in to_remove:
                    bids.pop(p, None)
                    bid_from_quote.pop(p, None)
        if best_ask==best_ask:
            if len(asks) > max_depth * 2:
                to_remove = [p for p in asks.keys() if p > best_ask + 1e-12]
                for p in to_remove:
                    asks.pop(p, None)
                    ask_from_quote.pop(p, None)

    snapshots = []
    last_snapshot_ts = None

    def snapshot(now_ts: pd.Timestamp):
        nonlocal prev_bid_top, prev_ask_top, prev_best_bid, prev_best_ask, bid_hold, ask_hold
        nonlocal ttb_acc, tta_acc, last_snapshot_ts
        if not (best_bid==best_bid and best_ask==best_ask):
            return
        if best_bid <= 0 or best_ask <= 0:
            return
        spread = best_ask - best_bid
        if spread < TICK_SIZE - 1e-12:
            return
        # ladders - optimized sorting
        # Only sort non-zero volumes within reasonable price range
        bid_candidates = [(p,v) for p,v in bids.items() if v>0 and p<=best_bid and p>=best_bid-10*TICK_SIZE]
        ask_candidates = [(p,v) for p,v in asks.items() if v>0 and p>=best_ask and p<=best_ask+10*TICK_SIZE]
        
        bid_lvls = sorted(bid_candidates, key=lambda x: -x[0])[:max_depth]
        ask_lvls = sorted(ask_candidates, key=lambda x: x[0])[:max_depth]
        bid_vols = [v for _,v in bid_lvls] + [0.0]*(max_depth - len(bid_lvls))
        ask_vols = [v for _,v in ask_lvls] + [0.0]*(max_depth - len(ask_lvls))
        # Strict L1 volume must match best price exactly; otherwise treat as 0.0
        bid_vol_top = bids.get(best_bid, 0.0)
        ask_vol_top = asks.get(best_ask, 0.0)

        bid_same = (best_bid == prev_best_bid)
        ask_same = (best_ask == prev_best_ask)

        bid_cxl = 0.0; ask_cxl = 0.0
        if last_snapshot_ts is not None:
            if bid_same and not pd.isna(prev_bid_top):
                drop = max(prev_bid_top - bid_vol_top, 0.0)
                bid_cxl = max(drop - ttb_acc, 0.0)
            if ask_same and not pd.isna(prev_ask_top):
                drop = max(prev_ask_top - ask_vol_top, 0.0)
                ask_cxl = max(drop - tta_acc, 0.0)

        bid_hold = bid_hold + 1 if bid_same else 1
        ask_hold = ask_hold + 1 if ask_same else 1

        mid = (best_bid + best_ask) / 2.0
        denom = (bid_vol_top + ask_vol_top) if (bid_vol_top + ask_vol_top) > 0 else np.nan
        micro = (best_bid * ask_vol_top + best_ask * bid_vol_top) / denom if denom==denom else mid
        vi = 0.0 if (denom!=denom or denom==0) else (bid_vol_top - ask_vol_top)/denom

        snap = {
            "ts": now_ts,
            "best_bid": best_bid, "best_ask": best_ask, "spread": spread, "mid_price": mid,
            "bid_vol_top": bid_vol_top, "ask_vol_top": ask_vol_top,
            "trade_through_bid": ttb_acc, "trade_through_ask": tta_acc,
            "bid_cxl_at_best": bid_cxl, "ask_cxl_at_best": ask_cxl,
            "prev_best_bid": prev_best_bid, "prev_best_ask": prev_best_ask,
            "bid_hold": bid_hold, "ask_hold": ask_hold,
            "microprice": micro, "vi": vi,
            "bid_l1_from_quote": bool(bid_from_quote.get(best_bid, False)),
            "ask_l1_from_quote": bool(ask_from_quote.get(best_ask, False)),
        }
        for i,(p,v) in enumerate(bid_lvls[:max_depth], start=1): snap[f"bid_vol_depth_{i}"] = v
        for i,(p,v) in enumerate(ask_lvls[:max_depth], start=1): snap[f"ask_vol_depth_{i}"] = v
        snapshots.append(snap)

        prev_bid_top = bid_vol_top
        prev_ask_top = ask_vol_top
        prev_best_bid = best_bid
        prev_best_ask = best_ask
        ttb_acc = 0.0; tta_acc = 0.0
        last_snapshot_ts = now_ts

    # Convert to numpy arrays for faster iteration
    all_e_values = all_e.values
    col_indices = {col: idx for idx, col in enumerate(all_e.columns)}
    ts_idx = col_indices["ts"]
    etype_idx = col_indices["etype"]
    dom_type_idx = col_indices["dom_type"]
    price_idx = col_indices["price"]
    volume_idx = col_indices["volume"]
    best_bid_idx = col_indices["best_bid"]
    best_ask_idx = col_indices["best_ask"]
    
    for i in range(len(all_e_values)):
        ev = all_e_values[i]
        ts = all_e.iloc[i]["ts"]  # Keep timestamp as pandas timestamp
        if ev[etype_idx] == "depth":
            dom_type = int(ev[dom_type_idx]) if not pd.isna(ev[dom_type_idx]) else -1
            price = float(ev[price_idx]) if not pd.isna(ev[price_idx]) else np.nan
            vol = float(ev[volume_idx]) if not pd.isna(ev[volume_idx]) else np.nan

            if dom_type == DOM_RESET:
                bids.clear(); asks.clear(); bid_from_quote.clear(); ask_from_quote.clear()
                best_bid = np.nan; best_ask = np.nan

            elif dom_type == DOM_BESTBID:
                best_bid = price
                if vol>0:
                    bids[price] = vol; bid_from_quote[price] = False
                prune_sides()

            elif dom_type == DOM_BESTASK:
                best_ask = price
                if vol>0:
                    asks[price] = vol; ask_from_quote[price] = False
                prune_sides()

            elif dom_type == DOM_BID:
                if vol>0:
                    bids[price] = vol; bid_from_quote[price] = False
                else:
                    bids.pop(price, None); bid_from_quote.pop(price, None)

            elif dom_type == DOM_ASK:
                if vol>0:
                    asks[price] = vol; ask_from_quote[price] = False
                else:
                    asks.pop(price, None); ask_from_quote.pop(price, None)

            elif dom_type == DOM_TRADE:
                if best_bid==best_bid and abs(price - best_bid) < 1e-12:
                    ttb_acc += max(vol, 0.0)
                    if price in bids:
                        bids[price] = max(bids[price] - vol, 0.0)
                        if bids[price] <= 0: bids.pop(price, None); bid_from_quote.pop(price, None)
                if best_ask==best_ask and abs(price - best_ask) < 1e-12:
                    tta_acc += max(vol, 0.0)
                    if price in asks:
                        asks[price] = max(asks[price] - vol, 0.0)
                        if asks[price] <= 0: asks.pop(price, None); ask_from_quote.pop(price, None)

            snapshot(ts)

        else:  # quote
            bb = float(ev[best_bid_idx]) if not pd.isna(ev[best_bid_idx]) else np.nan
            ba = float(ev[best_ask_idx]) if not pd.isna(ev[best_ask_idx]) else np.nan
            if bb==bb and bb>0:
                best_bid = bb
                if bb not in bids:
                    bids[bb] = float(quote_default_l1_size)
                    bid_from_quote[bb] = True
                prune_sides()
            if ba==ba and ba>0:
                best_ask = ba
                if ba not in asks:
                    asks[ba] = float(quote_default_l1_size)
                    ask_from_quote[ba] = True
                prune_sides()
            snapshot(ts)

    df = pd.DataFrame(snapshots)
    if df.empty:
        logger.warning("没有有效的订单簿快照")
        return df

    df = df.sort_values("ts").reset_index(drop=True)
    df["dspread"] = df["spread"].diff().fillna(0.0)
    df["prev_mid"] = df["mid_price"].shift(1)
    df["dt_ms"] = (df["ts"].astype("int64").diff()/1e6).fillna(0.0)

    wins = 10
    tta_roll = df["trade_through_ask"].rolling(wins).sum()
    ttb_roll = df["trade_through_bid"].rolling(wins).sum()
    denom = (tta_roll + ttb_roll).replace(0, np.nan)
    df["buy_ratio"] = (tta_roll / denom).fillna(0.5)

    df["bid_cancel_ratio"] = df["bid_cxl_at_best"] / (df["bid_cxl_at_best"] + df["trade_through_bid"] + 1e-9)
    df["ask_cancel_ratio"] = df["ask_cxl_at_best"] / (df["ask_cxl_at_best"] + df["trade_through_ask"] + 1e-9)

    logger.info(f"订单簿重建完成: {len(df)} 个事件快照")
    df["tick_size"] = TICK_SIZE
    return df


# ----------------------------------
# Event-driven triggers (no time buckets)
# ----------------------------------
def _rolling_zmad(s: pd.Series, win: int, minp: int = 10, eps: float = 1e-6) -> pd.Series:
    """Rolling robust z-score using median/MAD."""
    med = s.rolling(win, min_periods=minp).median()
    mad = (s - med).abs().rolling(win, min_periods=minp).median()
    z = (s - med) / (mad * 1.4826 + eps)
    return z.fillna(0.0)

def derive_event_triggers(
    events_df: pd.DataFrame,
    *,
    min_spread_ticks: int = 1,
    min_l1: float = 2.0,
    imb_win: int = 20,
    imb_thr: float = 0.25,
    cancel_z_win: int = 50,
    cancel_z_thr: float = 2.0,
    exhaust_frac: float = 0.6,
    vac_drop_frac: float = 0.5,
    micro_tilt_thr: float = 0.2,
    min_gap_events: int = 30,
    storm_skip: bool = True,
    storm_threshold_ms: float = 5.0,
    storm_lookback: int = 20,
    edge_margin_ticks: float = 0.5,
    w_imb: float = 2.0,
    w_cancel: float = 1.0,
    w_tilt: float = 1.0,
    w_breakout: float = 2.5,
) -> pd.DataFrame:
    """
    Build event-level triggers directly on the snapshot stream.
    Output columns: signal (±1/0), trigger {'BOE','VAC','AGG',''}, edge_est_ticks (float).
    """
    df = events_df.copy()
    if df.empty:
        return df

    tick = TICK_SIZE
    tick_value = MULTIPLIER * tick
    cost_ticks = RT_COST / max(tick_value, 1e-9)
    gate = cost_ticks + float(edge_margin_ticks)

    # Pre-compute features
    df["tilt_ticks"] = (df["microprice"] - df["mid_price"]) / tick
    # Rolling aggressor imbalance on events
    tta_roll = df["trade_through_ask"].fillna(0.0).rolling(imb_win, min_periods=max(3, int(imb_win/3))).sum()
    ttb_roll = df["trade_through_bid"].fillna(0.0).rolling(imb_win, min_periods=max(3, int(imb_win/3))).sum()
    denom = (tta_roll + ttb_roll).replace(0, np.nan)
    df["imb"] = ((tta_roll - ttb_roll) / denom).fillna(0.0)

    # Cancel z-scores & skew
    df["z_ask_cxl"] = _rolling_zmad(df["ask_cxl_at_best"].fillna(0.0), cancel_z_win)
    df["z_bid_cxl"] = _rolling_zmad(df["bid_cxl_at_best"].fillna(0.0), cancel_z_win)
    df["cancel_skew"] = (df["ask_cxl_at_best"].fillna(0.0) - df["bid_cxl_at_best"].fillna(0.0)) / \
                        (df["ask_cxl_at_best"].fillna(0.0) + df["bid_cxl_at_best"].fillna(0.0) + 1e-9)

    n = len(df)
    signal = np.zeros(n, dtype=np.int8)
    trigger = np.array([""] * n, dtype=object)
    edge_est = np.full(n, np.nan, dtype=np.float64)

    last_fire_idx = -10_000
    spread_min = min_spread_ticks * tick

    # Ensure dt_ms exists for storm protection
    if "dt_ms" not in df.columns:
        df["dt_ms"] = (df["ts"].astype("int64").diff() / 1e6).fillna(np.inf)

    for i in range(1, n):
        # Basic gates
        if not (df["spread"].iat[i] >= spread_min):
            continue
        if not ((df["bid_vol_top"].iat[i] >= min_l1) or (df["ask_vol_top"].iat[i] >= min_l1)):
            continue
        if (i - last_fire_idx) < int(min_gap_events):
            continue
        if storm_skip and i > storm_lookback:
            recent = df["dt_ms"].iloc[i - storm_lookback:i].median()
            if recent < float(storm_threshold_ms):
                continue

        # Shorthands (use i and i-1 where needed)
        bb, ba = df["best_bid"].iat[i], df["best_ask"].iat[i]
        bbp, bap = df["best_bid"].iat[i-1], df["best_ask"].iat[i-1]
        ask_l1, bid_l1 = df["ask_vol_top"].iat[i], df["bid_vol_top"].iat[i]
        ask_l1p, bid_l1p = df["ask_vol_top"].iat[i-1], df["bid_vol_top"].iat[i-1]
        tta_p = df["trade_through_ask"].iat[i-1]
        ttb_p = df["trade_through_bid"].iat[i-1]
        cxa_p = df["ask_cxl_at_best"].iat[i-1]
        cxb_p = df["bid_cxl_at_best"].iat[i-1]
        tilt = df["tilt_ticks"].iat[i]
        imb = df["imb"].iat[i]
        z_ask = df["z_ask_cxl"].iat[i]
        z_bid = df["z_bid_cxl"].iat[i]
        csk = df["cancel_skew"].iat[i]

        # Event primitives
        # 1) BOE – previous L1 exhausted, best price steps
        up_exhaust = ((tta_p + cxa_p) / (ask_l1p + 1e-9)) >= exhaust_frac
        dn_exhaust = ((ttb_p + cxb_p) / (bid_l1p + 1e-9)) >= exhaust_frac
        bo_up = (ba > bap) and up_exhaust
        bo_dn = (bb < bbp) and dn_exhaust

        # 2) VAC – abnormal same-price cancels + L1 drop + tilt agree, same best price
        vac_up = (ba == bap) and (z_ask >= cancel_z_thr) and (ask_l1 < (1.0 - vac_drop_frac) * max(ask_l1p, 1e-9)) and (tilt >= micro_tilt_thr)
        vac_dn = (bb == bbp) and (z_bid <= -cancel_z_thr) and (bid_l1 < (1.0 - vac_drop_frac) * max(bid_l1p, 1e-9)) and (tilt <= -micro_tilt_thr)

        # 3) AGG – aggressor dominance + tilt agree
        agg_up = (imb >= imb_thr) and (tilt >= micro_tilt_thr)
        agg_dn = (imb <= -imb_thr) and (tilt <= -micro_tilt_thr)

        fire = False
        dirn = 0
        kind = ""

        # Priority: BOE > VAC > AGG
        if bo_up or bo_dn:
            dirn = +1 if bo_up else -1
            kind = "BOE"
            fire = True
        elif vac_up or vac_dn:
            dirn = +1 if vac_up else -1
            kind = "VAC"
            fire = True
        elif agg_up or agg_dn:
            dirn = +1 if agg_up else -1
            kind = "AGG"
            fire = True

        if not fire:
            continue

        # Edge estimate (signed to direction)
        raw = (w_imb * imb) + (w_cancel * csk) + (w_tilt * tilt) + (w_breakout * (1.0 if kind == "BOE" else 0.0))
        est = dirn * raw

        if est >= gate:
            signal[i] = dirn
            trigger[i] = kind
            edge_est[i] = est
            last_fire_idx = i

    df["signal"] = signal
    df["trigger"] = trigger
    df["edge_est_ticks"] = edge_est
    return df


# ----------------------------------
# Time-bucketed signal frame (robust stats)
# ----------------------------------
def build_signal_buckets(events_df: pd.DataFrame, window_ms: int = 100) -> pd.DataFrame:
    if events_df.empty: return events_df
    df = events_df.copy()
    df["bucket"] = df["ts"].dt.floor(f"{window_ms}ms")

    agg = {
        "best_bid": "last",
        "best_ask": "last",
        "spread": "last",
        "mid_price": "last",
        "bid_vol_top": "last",
        "ask_vol_top": "last",
        "trade_through_bid": "sum",
        "trade_through_ask": "sum",
        "bid_cxl_at_best": "sum",
        "ask_cxl_at_best": "sum",
        "bid_l1_from_quote": "last",
        "ask_l1_from_quote": "last",
    }
    b = df.groupby("bucket", as_index=False).agg(agg).rename(columns={"bucket":"ts"})
    b = b.sort_values("ts").reset_index(drop=True)
    b["prev_best_bid"] = b["best_bid"].shift(1)
    b["prev_best_ask"] = b["best_ask"].shift(1)
    b["spread"] = b["best_ask"] - b["best_bid"]
    b["mid_price"] = (b["best_bid"] + b["best_ask"])/2.0

    denom = (b["bid_vol_top"] + b["ask_vol_top"]).replace(0, np.nan)
    b["microprice"] = (b["best_bid"] * b["ask_vol_top"] + b["best_ask"] * b["bid_vol_top"]) / denom
    b["microprice"] = b["microprice"].fillna(b["mid_price"])
    b["vi"] = ((b["bid_vol_top"] - b["ask_vol_top"]) / denom).fillna(0.0)

    b["dspread"] = b["spread"].diff().fillna(0.0)
    b["prev_mid"] = b["mid_price"].shift(1)

    wins = max(3, int(500/window_ms))
    tta_roll = b["trade_through_ask"].rolling(wins).sum()
    ttb_roll = b["trade_through_bid"].rolling(wins).sum()
    denom2 = (tta_roll + ttb_roll).replace(0, np.nan)
    b["buy_ratio"] = (tta_roll / denom2).fillna(0.5)

    b["bid_cancel_ratio"] = b["bid_cxl_at_best"] / (b["bid_cxl_at_best"] + b["trade_through_bid"] + 1e-9)
    b["ask_cancel_ratio"] = b["ask_cxl_at_best"] / (b["ask_cxl_at_best"] + b["trade_through_ask"] + 1e-9)

    b["bid_hold"] = (b["best_bid"] == b["prev_best_bid"]).astype(int).groupby((b["best_bid"] != b["prev_best_bid"]).cumsum()).cumsum()
    b["ask_hold"] = (b["best_ask"] == b["prev_best_ask"]).astype(int).groupby((b["best_ask"] != b["prev_best_ask"]).cumsum()).cumsum()

    logger.info(f"信号时间桶完成: {len(b)} 个桶（{window_ms}ms）")
    return b


# ----------------------------------
# Micro-games (atomic signals)
# Each returns dir (+1/-1/0) and score (>=0)
# ----------------------------------
def game_queue_hazard(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    """G1: Queue-run hazard at L1: (hits + same-price cancels) / L1 size"""
    hb = (df["trade_through_bid"] + df["bid_cxl_at_best"]) / (df["bid_vol_top"] + 1e-9)
    ha = (df["trade_through_ask"] + df["ask_cxl_at_best"]) / (df["ask_vol_top"] + 1e-9)
    d = np.where(ha>hb, +1, np.where(hb>ha, -1, 0)).astype(np.int8)
    s = np.abs(ha - hb).astype(np.float64)
    return d, s

def game_commit_vs_spoof(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    """G2: Commitment vs spoof at best: hold + refill vs flash cancels."""
    bid_same = (df["best_bid"] == df["prev_best_bid"])
    ask_same = (df["best_ask"] == df["prev_best_ask"])
    bid_refill = ( (df["bid_vol_top"] - df["bid_vol_top"].shift(1)) > 0 ) & bid_same
    ask_refill = ( (df["ask_vol_top"] - df["ask_vol_top"].shift(1)) > 0 ) & ask_same

    bid_drop = (df["bid_vol_top"].shift(1) - df["bid_vol_top"]).clip(lower=0)
    ask_drop = (df["ask_vol_top"].shift(1) - df["ask_vol_top"]).clip(lower=0)
    bid_flash = (bid_drop > (df["trade_through_bid"] + df["bid_cxl_at_best"]) * 0.5) & bid_same
    ask_flash = (ask_drop > (df["trade_through_ask"] + df["ask_cxl_at_best"]) * 0.5) & ask_same

    commit_bid = (df["bid_hold"].astype(float)/5.0).clip(0, 2.0) + bid_refill.rolling(5).sum().fillna(0.0)
    commit_ask = (df["ask_hold"].astype(float)/5.0).clip(0, 2.0) + ask_refill.rolling(5).sum().fillna(0.0)
    spoof_bid = bid_flash.rolling(5).sum().fillna(0.0)
    spoof_ask = ask_flash.rolling(5).sum().fillna(0.0)

    up_score   = commit_bid + spoof_ask
    down_score = commit_ask + spoof_bid
    d, s = np.zeros(len(df), dtype=np.int8), np.zeros(len(df), dtype=np.float64)
    mask_up = (up_score > down_score)
    mask_dn = (down_score > up_score)
    d[mask_up] = +1; s[mask_up] = (up_score - down_score)[mask_up]
    d[mask_dn] = -1; s[mask_dn] = (down_score - up_score)[mask_dn]
    return d, s

def game_mm_retreat(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    """G3: Market-maker retreat under toxicity: higher buy_ratio + ask-side cancels => up."""
    r_ask = (df["buy_ratio"] * 2.0) * (df["ask_cancel_ratio"] + 1e-3)
    r_bid = ((1.0 - df["buy_ratio"]) * 2.0) * (df["bid_cancel_ratio"] + 1e-3)
    d = np.where(r_ask>r_bid, +1, np.where(r_bid>r_ask, -1, 0)).astype(np.int8)
    s = np.abs(r_ask - r_bid)
    return d, s

def game_sweep_cluster(df: pd.DataFrame, wins:int=5) -> Tuple[np.ndarray, np.ndarray]:
    """G4: Sweeper/followers cluster: short-window clustering of trade-throughs."""
    ema = lambda x, a: x.ewm(alpha=a, adjust=False).mean()
    a = 2.0/(wins+1.0)
    cl_ask = ema(df["trade_through_ask"].fillna(0.0), a)
    cl_bid = ema(df["trade_through_bid"].fillna(0.0), a)
    d = np.where(cl_ask>cl_bid, +1, np.where(cl_bid>cl_ask, -1, 0)).astype(np.int8)
    s = (cl_ask - cl_bid).abs()
    return d, s

def game_refill_trap(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    """G5: Refill/iceberg trap – repeated refills at L1 imply absorption then revert."""
    bid_refill_amt = (df["bid_vol_top"] - df["bid_vol_top"].shift(1)).clip(lower=0) * (df["best_bid"] == df["prev_best_bid"])
    ask_refill_amt = (df["ask_vol_top"] - df["ask_vol_top"].shift(1)).clip(lower=0) * (df["best_ask"] == df["prev_best_ask"])
    rb = bid_refill_amt.rolling(8).sum().fillna(0.0)
    ra = ask_refill_amt.rolling(8).sum().fillna(0.0)
    d = np.where(rb>ra, +1, np.where(ra>rb, -1, 0)).astype(np.int8)
    s = (rb - ra).abs()
    return d, s

def game_symmetry_break(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    """G6: Spread blowout + side dominance -> move with active side."""
    asym = (df["buy_ratio"] - 0.5).abs()
    raw = (df["dspread"].clip(lower=0.0)) * asym
    d = np.sign(df["buy_ratio"] - 0.5).astype(np.int8)
    s = raw.fillna(0.0).values
    s = np.where(df["dspread"].values>0, s, 0.0)
    d = np.where(s>0, d, 0)
    return d, s

def game_resilience(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    """G7: Impact-resilience – longer time to revert implies persistence."""
    dv = (df["mid_price"] - df["prev_mid"]).fillna(0.0)
    sign = np.sign(dv).astype(np.int8)
    streak = (sign.groupby( (sign != sign.shift(1)).cumsum() ).cumcount() + 1).where(sign!=0, other=0)
    mag = dv.abs().ewm(alpha=0.2, adjust=False).mean()
    s = (streak * mag).fillna(0.0).values
    d = sign.values
    return d, s


# ----------------------------------
# Probabilistic blend (logit mixing)
# ----------------------------------
def robust_scale(x: pd.Series, eps: float=1e-6) -> float:
    med = np.nanmedian(x)
    mad = np.nanmedian(np.abs(x - med))
    return float(mad*1.4826 + eps)

def to_prob_from_dir_score(dir_arr: np.ndarray, score_arr: np.ndarray, k: float) -> np.ndarray:
    """
    Map (dir,score) -> P(up) via logistic on signed score: p = sigmoid(k * dir * score).
    k acts like a temperature: larger k -> sharper probabilities.
    """
    z = k * (dir_arr.astype(np.float64) * score_arr.astype(np.float64))
    z = np.clip(z, -12.0, 12.0)
    return 1.0/(1.0 + np.exp(-z))

def blend_probs_and_contribs(probs: List[np.ndarray], weights: List[float]) -> Tuple[np.ndarray, List[np.ndarray], List[np.ndarray]]:
    """
    Logit-weighted mixing with contributions.
    Returns blended p_up, list of individual logits, list of contributions (w_i * logit_i).
    """
    logits = []
    for p in probs:
        p = np.clip(p, 1e-6, 1-1e-6)
        logits.append(np.log(p/(1.0-p)))
    logits = np.vstack(logits)  # [n_games, n]
    w = np.array(weights, dtype=np.float64).reshape(-1,1)
    contribs = (w * logits)
    mix = contribs.sum(axis=0)
    mix = np.clip(mix, -20, 20)
    p_up = 1.0/(1.0 + np.exp(-mix))
    # split back per game
    logits_list = [logits[i,:] for i in range(logits.shape[0])]
    contribs_list = [contribs[i,:] for i in range(contribs.shape[0])]
    return p_up, logits_list, contribs_list


# ----------------------------------
# Features + signals on bucket frame
# ----------------------------------
def compute_game_theory_features_and_signals(
    bdf: pd.DataFrame,
    weights: Dict[str,float],
    prob_margin: float = 0.55,
    min_spread_ticks: int = 1,
    min_volume_top: int = 2,
    min_signal_gap_ms: int = 250,
    confirm_bins: int = 1,
    min_micro_tilt_ticks: float = 0.0,
    quote_default_l1_size: float = 2.0,
    debug_gating: bool = False,
) -> pd.DataFrame:
    logger.info("计算微博弈特征与组合概率（时间桶）")
    out = bdf.copy()

    # games
    g1_d, g1_s = game_queue_hazard(out)
    g2_d, g2_s = game_commit_vs_spoof(out)
    g3_d, g3_s = game_mm_retreat(out)
    g4_d, g4_s = game_sweep_cluster(out)
    g5_d, g5_s = game_refill_trap(out)
    g6_d, g6_s = game_symmetry_break(out)
    g7_d, g7_s = game_resilience(out)

    def k_of(arr: np.ndarray) -> float:
        s = robust_scale(pd.Series(arr))
        return 1.0/(s + 1e-3)
    ks = [k_of(x) for x in (g1_s,g2_s,g3_s,g4_s,g5_s,g6_s,g7_s)]

    probs = [to_prob_from_dir_score(d,s,k) for (d,s),k in zip([(g1_d,g1_s),(g2_d,g2_s),(g3_d,g3_s),(g4_d,g4_s),(g5_d,g5_s),(g6_d,g6_s),(g7_d,g7_s)], ks)]

    wlist = np.array([
        weights.get("queue_hazard", 1.2),
        weights.get("commit_spoof", 1.0),
        weights.get("mm_retreat", 1.0),
        weights.get("sweep_cluster", 1.0),
        weights.get("refill_trap", 0.8),
        weights.get("sym_break", 1.0),
        weights.get("resilience", 0.8),
    ], dtype=np.float64)
    if wlist.sum() <= 0: wlist = np.ones_like(wlist)
    wlist = wlist / wlist.sum()

    p_up, logits_list, contribs_list = blend_probs_and_contribs(probs, list(wlist))
    out["p_up"] = p_up
    out["p_down"] = 1.0 - p_up
    for i,p in enumerate(probs, start=1): out[f"p_g{i}"] = p
    for i,c in enumerate(contribs_list, start=1): out[f"contrib_g{i}"] = c

    # gating
    out["signal_raw"] = 0
    out.loc[out["p_up"] >= prob_margin, "signal_raw"]  = +1
    out.loc[out["p_up"] <= (1.0 - prob_margin), "signal_raw"]  = -1

    spread_ok = (out["spread"] >= min_spread_ticks * TICK_SIZE - 1e-12)

    # volume gating, aware of quote-introduced default size
    # If L1 came from quote and equals the quote_default_l1_size, use that as effective threshold
    bid_eff_thr = np.where((out.get("bid_l1_from_quote", False).astype(bool)) & (np.isclose(out["bid_vol_top"], quote_default_l1_size)), quote_default_l1_size, min_volume_top)
    ask_eff_thr = np.where((out.get("ask_l1_from_quote", False).astype(bool)) & (np.isclose(out["ask_vol_top"], quote_default_l1_size)), quote_default_l1_size, min_volume_top)
    vol_ok = (out["bid_vol_top"].values >= bid_eff_thr) | (out["ask_vol_top"].values >= ask_eff_thr)
    vol_ok = pd.Series(vol_ok, index=out.index)

    if min_micro_tilt_ticks > 0:
        tilt_ticks = (out["microprice"] - out["mid_price"]) / TICK_SIZE
        micro_ok_long  = tilt_ticks >=  min_micro_tilt_ticks
        micro_ok_short = tilt_ticks <= -min_micro_tilt_ticks
    else:
        micro_ok_long  = pd.Series(True, index=out.index)
        micro_ok_short = pd.Series(True, index=out.index)

    out["signal"] = 0
    last_sig_ts = pd.Timestamp(0, tz=timezone.utc)
    gap_ms = float(min_signal_gap_ms)
    conf = int(max(1, confirm_bins))

    passes = {"raw":0,"spread":0,"vol":0,"tilt":0,"gap":0,"confirm":0,"final":0}

    for i in range(len(out)):
        s = int(out.at[i, "signal_raw"]) ; ts = out.at[i, "ts"]
        if s == 0: continue
        passes["raw"] += 1
        if not spread_ok.iat[i]: continue
        passes["spread"] += 1
        if not vol_ok.iat[i]: continue
        passes["vol"] += 1
        if s==+1 and not micro_ok_long.iat[i]:  continue
        if s==-1 and not micro_ok_short.iat[i]: continue
        passes["tilt"] += 1
        if (ts - last_sig_ts).total_seconds()*1000.0 < gap_ms: continue
        passes["gap"] += 1
        if conf > 1:
            lo = max(0, i-conf+1)
            if not np.all(out["signal_raw"].iloc[lo:i+1].values == s): continue
        passes["confirm"] += 1
        out.at[i, "signal"] = s
        last_sig_ts = ts
        passes["final"] += 1

    if debug_gating:
        logger.info(f"Gating通过统计: raw={passes['raw']}, spread={passes['spread']}, vol={passes['vol']}, tilt={passes['tilt']}, gap={passes['gap']}, confirm={passes['confirm']}, final={passes['final']}")

    return out


# ----------------------------------
# Map time-bucket signals onto event stream with latency & storm protection
# ----------------------------------
def filter_rth(df: pd.DataFrame, tz: str = "America/New_York") -> pd.DataFrame:
    """Keep RTH (09:30–16:00 local). Assumes UTC timestamps."""
    if df.empty: return df
    local = df["ts"].dt.tz_convert(ZoneInfo(tz))
    mask = (local.dt.time >= pd.Timestamp("09:30").time()) & (local.dt.time < pd.Timestamp("16:00").time())
    return df.loc[mask].copy()

def map_signals_to_events(
    events_df: pd.DataFrame,
    sig_df: pd.DataFrame,
    latency_ms: int = 100,
    storm_skip: bool = True,
    storm_threshold_ms: float = 5.0,
    storm_lookback: int = 20,
    window_ms: int = 100,
) -> pd.DataFrame:
    """
    For each signal bucket (whose timestamp is the BUCKET START), schedule activation at
    ts + window_ms (bucket END) + latency_ms, and place the signal on the first event with
    ts >= activation_ts. Optionally skip if recent event spacing is too dense (storm protection).
    This removes look-ahead/leakage across the bucket.
    """
    out = events_df.copy()
    out["signal"] = 0
    out["signal_src_ts"] = pd.NaT
    out["p_up"] = np.nan
    for i in range(1,8):
        out[f"contrib_g{i}"] = np.nan
        out[f"p_g{i}"] = np.nan
    out["p_up_live"] = np.nan
    # Ensure dt_ms exists for storm protection; fallback compute if missing
    if "dt_ms" not in out.columns:
        try:
            out["dt_ms"] = (out["ts"].astype("int64").diff()/1e6).fillna(np.inf)
        except Exception:
            out["dt_ms"] = np.inf

    # Use int64 ns timestamps for robust searchsorted on tz-aware datetimes
    ev_ts_ns = out["ts"].astype("int64").to_numpy()
    sig_rows = sig_df[sig_df["signal"] != 0]

    for _, srow in sig_rows.iterrows():
        s_ts = srow["ts"]
        act_ts = s_ts + pd.Timedelta(milliseconds=window_ms + latency_ms)
        # find first event index with ts >= act_ts using int64 nanoseconds
        idx = ev_ts_ns.searchsorted(act_ts.value)
        if idx >= len(out):
            continue

        # storm protection
        if storm_skip and idx > storm_lookback:
            recent_dt = out["dt_ms"].iloc[max(0, idx-storm_lookback):idx].median()
            if recent_dt < storm_threshold_ms:
                # skip this signal under micro-burst
                continue

        if idx < len(out) and out.iloc[idx]["signal"] == 0:
            out.iloc[idx, out.columns.get_loc("signal")] = int(srow["signal"])
            out.iloc[idx, out.columns.get_loc("signal_src_ts")] = s_ts
            out.iloc[idx, out.columns.get_loc("p_up")] = float(srow["p_up"])
            for j in range(1,8):
                contrib_col = f"contrib_g{j}"
                p_col = f"p_g{j}"
                if contrib_col in out.columns:
                    out.iloc[idx, out.columns.get_loc(contrib_col)] = float(srow.get(contrib_col, np.nan))
                if p_col in out.columns:
                    out.iloc[idx, out.columns.get_loc(p_col)] = float(srow.get(p_col, np.nan))

    # Map continuous probability stream (latency-aligned) and forward-fill to all events
    try:
        all_sig = sig_df[["ts","p_up"]].copy()
        all_sig = all_sig.dropna(subset=["ts","p_up"])
        for _, srow in all_sig.iterrows():
            s_ts_any = srow["ts"]
            act_ts_any = s_ts_any + pd.Timedelta(milliseconds=window_ms + latency_ms)
            idx_live = ev_ts_ns.searchsorted(act_ts_any.value)
            if idx_live < len(out):
                out.iloc[idx_live, out.columns.get_loc("p_up_live")] = float(srow["p_up"])
        out["p_up_live"] = out["p_up_live"].ffill()
    except Exception:
        pass

    return out


# ----------------------------------
# Execution with queue advance (event-driven)
# ----------------------------------
def backtest_with_queue(
    df: pd.DataFrame,
    hold_sec: float = 5.0,
    queue_pos: float = 10.0,
    reset_on_price_move: bool = True,
    cancel_on_adverse: bool = True,
    adverse_max_events: int = 2,
    vi_exit_thr: float = 0.3,
    enable_vi_exit: bool = True,
    consecutive_vi_count: int = 2,
    vi_reversal_threshold: float = 0.5,
    trade_confirmation_sec: float = 0.5,
    min_trade_confirmation: int = 1,
    # price-based exits
    stop_ticks: float = 0.0,
    tp_ticks: float = 0.0,
    trail_ticks: float = 0.0,
    # probability reversal removed (no p_up stream in event-only mode)
    enable_prob_rev: bool = False,
    prob_rev_margin: float = 0.56,
    prob_rev_consec: int = 2,
    # minimum hold time for non-STOP exits (ms)
    min_hold_ms: int = 0,
    # taker execution & time stop
    entry_mode: str = "maker",          # {"maker","taker","pullback","hybrid"}
    taker_slip_ticks: int = 1,
    time_stop_ms: int = 2000,
    # pullback maker parameters
    pullback_ticks: int = 1,
    dwell_ms: int = 200,
    fill_timeout_ms: int = 600,
    min_pullback_l1: float = 1.0,
    # hybrid gating (taker only for very strong edge)
    hybrid_taker_prob: float = 0.66,    # kept for backward compat; unused
    hybrid_taker_edge: float = 3.0,
) -> Dict[str,object]:
    """
    Fill = top-hit (trade_through_side) + same-price cancels (side) while price unchanged.
    Triggers are event-level signals placed by derive_event_triggers (dir ±1).
    """
    logger.info(
        f"开始回测(事件驱动): hold={hold_sec}s, queue_pos={queue_pos}, "
        f"entry_mode={entry_mode}, taker_slip_ticks={taker_slip_ticks}, time_stop_ms={time_stop_ms}, "
        f"hybrid_taker_edge={hybrid_taker_edge}"
    )

    trades = []

    position = 0
    entry_price = None
    entry_ts = None
    side = None

    waiting = False
    q_remaining = None
    adverse_streak = 0

    vi_reversal_count = 0
    last_vi_reversal_ts = None
    max_bid_since_entry = None
    min_ask_since_entry = None
    prob_rev_count = 0

    # pullback state
    pullback_armed = False
    pullback_base_price = None
    pullback_dwell_start = None
    pullback_candidate_price = None
    queue_start_ts = None

    for idx, row in df.iterrows():
        ts = row["ts"]
        bb = float(row["best_bid"])
        ba = float(row["best_ask"])
        TICK_SIZE = float(row.get("tick_size", 0.25))

        # -------------- SIGNAL HANDLING (event-level) --------------
        signal = int(row.get("signal", 0))
        trig_type = str(row.get("trigger", "")) if row.get("trigger", "") is not None else ""
        edge_est = float(row.get("edge_est_ticks", np.nan))

        if position == 0 and not waiting and signal != 0:
            side = signal
            # carry meta at trigger time
            signal_meta = {
                "trigger": trig_type,
                "edge_est_ticks": edge_est,
            }
            adverse_streak = 0

            if entry_mode == "taker" or (entry_mode == "hybrid" and (edge_est == edge_est) and edge_est >= float(hybrid_taker_edge)):
                # taker: immediate execute with slip
                position = side
                entry_ts = ts
                entry_price = (ba + taker_slip_ticks * TICK_SIZE) if side == +1 else (bb - taker_slip_ticks * TICK_SIZE)
                max_bid_since_entry = bb
                min_ask_since_entry = ba
                prob_rev_count = 0
                waiting = False
            elif entry_mode in ("pullback", "hybrid"):
                # arm pullback; wait for adverse move & dwell before starting maker queue
                pullback_armed = True
                pullback_base_price = ba if side == +1 else bb
                pullback_dwell_start = None
                pullback_candidate_price = None
                waiting = False
            else:
                # maker: queue at best immediately
                entry_price = ba if side == +1 else bb
                l1_size = float(row.get("ask_vol_top" if side == +1 else "bid_vol_top", 0.0))
                q_remaining = max(0.0, l1_size + queue_pos)
                queue_start_ts = ts
                waiting = True

        # -------------- PULLBACK ARMING (only when armed and not yet queued) --------------
        if pullback_armed and not waiting and side is not None:
            if side == +1:
                threshold_px = pullback_base_price - pullback_ticks * TICK_SIZE
                if ba <= threshold_px:
                    if pullback_dwell_start is None:
                        pullback_dwell_start = ts
                        pullback_candidate_price = ba
                    else:
                        dwell = (ts - pullback_dwell_start).total_seconds() * 1000.0
                        ask_l1 = float(row.get("ask_vol_top", 0.0))
                        if dwell >= float(dwell_ms) and ask_l1 >= float(min_pullback_l1):
                            entry_price = pullback_candidate_price
                            l1_size = ask_l1
                            q_remaining = max(0.0, l1_size + queue_pos)
                            queue_start_ts = ts
                            waiting = True
                            pullback_armed = False
                else:
                    pullback_dwell_start = None
                    pullback_candidate_price = None
            else:
                threshold_px = pullback_base_price + pullback_ticks * TICK_SIZE
                if bb >= threshold_px:
                    if pullback_dwell_start is None:
                        pullback_dwell_start = ts
                        pullback_candidate_price = bb
                    else:
                        dwell = (ts - pullback_dwell_start).total_seconds() * 1000.0
                        bid_l1 = float(row.get("bid_vol_top", 0.0))
                        if dwell >= float(dwell_ms) and bid_l1 >= float(min_pullback_l1):
                            entry_price = pullback_candidate_price
                            l1_size = bid_l1
                            q_remaining = max(0.0, l1_size + queue_pos)
                            queue_start_ts = ts
                            waiting = True
                            pullback_armed = False
                else:
                    pullback_dwell_start = None
                    pullback_candidate_price = None

        # -------------- QUEUE FILL / ENTRY (maker & pullback queued) --------------
        if waiting and side is not None:
            # maker: advance queue while price unchanged; use side-specific through & cancels
            same_price = ((side == -1 and bb == entry_price) or (side == +1 and ba == entry_price))
            tth = float(row.get("trade_through_bid" if side == -1 else "trade_through_ask", 0.0))
            canc_same = float(row.get("bid_cxl_at_best" if side == -1 else "ask_cxl_at_best", 0.0))
            if same_price:
                q_remaining -= max(0.0, tth + canc_same)
            else:
                if reset_on_price_move:
                    l1_size_now = float(row.get("bid_vol_top" if side == -1 else "ask_vol_top", 0.0))
                    q_remaining = max(0.0, l1_size_now + queue_pos)

            # optional: fill timeout for maker/pullback queues
            if queue_start_ts is not None:
                waited_ms = (ts - queue_start_ts).total_seconds() * 1000.0
                if waited_ms >= float(fill_timeout_ms) and q_remaining > 0.0:
                    # cancel this attempt
                    waiting = False
                    entry_price = None
                    q_remaining = 0.0

            if q_remaining <= 0.0:
                # now in position
                position = side
                entry_ts = ts
                max_bid_since_entry = bb
                min_ask_since_entry = ba
                prob_rev_count = 0
                waiting = False
                queue_start_ts = None

        # -------------- EXIT LOGIC --------------
        if position != 0 and not waiting:
            if position == +1:
                if max_bid_since_entry is None or bb > max_bid_since_entry:
                    max_bid_since_entry = bb
            else:
                if min_ask_since_entry is None or ba < min_ask_since_entry:
                    min_ask_since_entry = ba

            exit_reason = None

            # Hard STOP (allowed before min_hold_ms)
            if stop_ticks and stop_ticks > 0 and entry_price is not None:
                if position == +1 and bb <= (entry_price - stop_ticks * TICK_SIZE):
                    exit_reason = "STOP"
                elif position == -1 and ba >= (entry_price + stop_ticks * TICK_SIZE):
                    exit_reason = "STOP"

            min_hold_ready = (entry_ts is not None) and (((ts - entry_ts).total_seconds() * 1000.0) >= float(min_hold_ms))

            # Trailing using L1 extremes
            if exit_reason is None and min_hold_ready and trail_ticks and trail_ticks > 0 and entry_price is not None:
                if position == +1 and (max_bid_since_entry is not None) and (max_bid_since_entry > entry_price) and (max_bid_since_entry - bb) >= (trail_ticks * TICK_SIZE):
                    exit_reason = "TRAIL"
                elif position == -1 and (min_ask_since_entry is not None) and (min_ask_since_entry < entry_price) and (ba - min_ask_since_entry) >= (trail_ticks * TICK_SIZE):
                    exit_reason = "TRAIL"

            # Take profit using L1
            if exit_reason is None and min_hold_ready and tp_ticks and tp_ticks > 0 and entry_price is not None:
                if position == +1 and bb >= (entry_price + tp_ticks * TICK_SIZE):
                    exit_reason = "TP"
                elif position == -1 and ba <= (entry_price - tp_ticks * TICK_SIZE):
                    exit_reason = "TP"

            # VI reversal exit (optional)
            if exit_reason is None and min_hold_ready and enable_vi_exit and vi_exit_thr > 0:
                vi = float(row.get("vi", 0.0))
                vi_reversal_detected = (position == +1 and vi < -vi_exit_thr) or (position == -1 and vi > vi_exit_thr)
                if vi_reversal_detected:
                    if last_vi_reversal_ts is None or (ts - last_vi_reversal_ts).total_seconds() <= vi_reversal_threshold:
                        vi_reversal_count += 1
                    else:
                        vi_reversal_count = 1
                    last_vi_reversal_ts = ts
                if vi_reversal_detected and vi_reversal_count >= consecutive_vi_count:
                    exit_reason = "VI_REV"

            # Time stop (no ≥1 tick favorable move within time_stop_ms)
            if exit_reason is None and time_stop_ms and time_stop_ms > 0 and entry_price is not None:
                held_ms = (ts - entry_ts).total_seconds() * 1000.0
                if held_ms >= float(time_stop_ms):
                    favorable = False
                    if position == +1:
                        favorable = (max_bid_since_entry is not None) and ((max_bid_since_entry - entry_price) >= TICK_SIZE)
                    else:
                        favorable = (min_ask_since_entry is not None) and ((entry_price - min_ask_since_entry) >= TICK_SIZE)
                    if not favorable:
                        exit_reason = "TIME_STOP"

            # Time exit (fallback)
            if exit_reason is None:
                held = (ts - entry_ts).total_seconds()
                if held >= hold_sec:
                    exit_reason = "TIME"

            if exit_reason is not None and entry_price is not None:
                exit_px = (bb if position == +1 else ba)
                pnl_g = (exit_px - entry_price) * MULTIPLIER * position
                pnl_n = pnl_g - RT_COST
                trades.append(dict(
                    entry_ts=entry_ts, entry_price=entry_price, side=position,
                    exit_ts=ts, exit_price=exit_px, hold_secs=(ts - entry_ts).total_seconds(),
                    gross_usd=pnl_g, net_usd=pnl_n, exit_reason=exit_reason,
                    win=1 if pnl_n > 0 else 0,
                    trigger=signal_meta.get("trigger","") if 'signal_meta' in locals() else "",
                    edge_est_ticks=signal_meta.get("edge_est_ticks", np.nan) if 'signal_meta' in locals() else np.nan,
                ))
                # reset state
                position = 0
                entry_price = None
                entry_ts = None
                side = None
                adverse_streak = 0
                vi_reversal_count = 0
                last_vi_reversal_ts = None
                max_bid_since_entry = None
                min_ask_since_entry = None
                prob_rev_count = 0
                pullback_armed = False
                pullback_base_price = None
                pullback_dwell_start = None
                pullback_candidate_price = None
                waiting = False

    trades_df = pd.DataFrame(trades)
    if not trades_df.empty:
        summary = {
            "trades": int(len(trades_df)),
            "gross_usd": float(trades_df["gross_usd"].sum()),
            "net_usd": float(trades_df["net_usd"].sum()),
            "hit_rate": float(trades_df["win"].mean()),
            "avg_hold_secs": float(trades_df["hold_secs"].mean()),
        }
    else:
        summary = {"trades":0,"gross_usd":0.0,"net_usd":0.0,"hit_rate":0.0,"avg_hold_secs":0.0}

    logger.info(f"回测完成: 交易 {summary['trades']} | 净收益 ${summary['net_usd']:.2f} | 胜率 {summary['hit_rate']:.2%}")
    return {"events": pd.DataFrame(), "trades": trades_df, "summary": summary}


# ----------------------------------
# Daily PnL helper
# ----------------------------------
def save_daily_pnl(trades_df: pd.DataFrame, out_path: Path, tz: str = "America/New_York"):
    if trades_df.empty:
        return
    t = trades_df.copy()
    t["entry_ts"] = pd.to_datetime(t["entry_ts"], utc=True)
    t["entry_day"] = t["entry_ts"].dt.tz_convert(ZoneInfo(tz)).dt.date
    daily = t.groupby("entry_day").agg(
        trades=("net_usd","size"),
        pnl_usd=("net_usd","sum"),
        hit_rate=("win","mean")
    ).reset_index()
    daily.to_csv(out_path, index=False)
    logger.info(f"按日汇总已保存: {out_path}")


# ----------------------------------
# Reporting (charts)
# ----------------------------------

def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def plot_cumulative_pnl(trades_df: pd.DataFrame, out_path: Path):
    if trades_df.empty:
        logger.warning("无交易，跳过累计收益图")
        return
    t = trades_df.sort_values("exit_ts")
    t["cum_pnl"] = t["net_usd"].cumsum()
    plt.figure(figsize=(9,4.5))
    plt.plot(t["exit_ts"], t["cum_pnl"], linewidth=1.5)
    plt.title("Cumulative PnL (net USD)")
    plt.xlabel("Exit Time (UTC)")
    plt.ylabel("PnL (USD)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    logger.info(f"累计收益曲线已保存: {out_path}")


def plot_hit_rate_by_prob(trades_df: pd.DataFrame, out_path: Path, bins: int = 10):
    if trades_df.empty or "p_up" not in trades_df.columns:
        logger.warning("缺少交易或 p_up 字段，跳过命中率分桶图")
        return
    t = trades_df.dropna(subset=["p_up"]).copy()
    if t.empty:
        logger.warning("p_up 为空，跳过命中率分桶图")
        return
    edges = np.linspace(0.0, 1.0, bins+1)
    t["bucket"] = pd.cut(t["p_up"].clip(0,1), bins=edges, include_lowest=True)
    grp = t.groupby("bucket").agg(hit_rate=("win","mean"),
                                   n=("win","size"),
                                   avg_p=("p_up","mean"))
    # plot
    plt.figure(figsize=(9,4.5))
    x = np.arange(len(grp))
    plt.bar(x, grp["hit_rate"].values)
    plt.xticks(x, [f"{a.left:.2f}-{a.right:.2f}\n(n={int(c)})" for a,c in zip(grp.index.categories, grp["n"])], rotation=45, ha="right")
    plt.ylim(0,1)
    plt.title("Hit Rate by Predicted P_up bucket")
    plt.ylabel("Hit Rate")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    logger.info(f"命中率分桶图已保存: {out_path}")


def plot_contrib_importance(trades_df: pd.DataFrame, out_path: Path):
    cols = [f"contrib_g{i}" for i in range(1,8) if f"contrib_g{i}" in trades_df.columns]
    if trades_df.empty or not cols:
        logger.warning("缺少交易或贡献字段，跳过贡献重要性图")
        return
    t = trades_df.copy()
    imp_all = t[cols].abs().mean(axis=0)
    # order by importance
    order = imp_all.sort_values(ascending=False)
    plt.figure(figsize=(9,4.5))
    plt.bar(order.index, order.values)
    plt.title("Mean |Contribution| per Micro-game")
    plt.ylabel("Mean |w * logit(p)|")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    logger.info(f"贡献重要性图已保存: {out_path}")


def generate_report(trades_df: pd.DataFrame, out_dir: Path, tag: str):
    _ensure_dir(out_dir)
    plot_cumulative_pnl(trades_df, out_dir / f"cum_pnl_{tag}.png")
    plot_hit_rate_by_prob(trades_df, out_dir / f"hit_rate_by_prob_{tag}.png", bins=10)
    plot_contrib_importance(trades_df, out_dir / f"contrib_importance_{tag}.png")


# ----------------------------------
# Main
# ----------------------------------
def main():
    ap = argparse.ArgumentParser(description="事件驱动 Scalper（No Windows）")
    ap.add_argument("db_path", help="Topstep DuckDB 文件路径")
    ap.add_argument("--output_dir", default="analysis_gt_fixed", help="输出目录")
    ap.add_argument("--start_time", help="开始时间(UTC，如 2025-08-04 13:30:00)")
    ap.add_argument("--end_time", help="结束时间(UTC)")
    ap.add_argument("--max_depth", type=int, default=10, help="重建的最大深度层数")

    # event-driven trigger params
    ap.add_argument("--min_spread_ticks", type=int, default=1)
    ap.add_argument("--min_volume_top", type=float, default=2.0)
    ap.add_argument("--imb_win", type=int, default=20)
    ap.add_argument("--imb_thr", type=float, default=0.25)
    ap.add_argument("--cancel_z_win", type=int, default=50)
    ap.add_argument("--cancel_z_thr", type=float, default=2.0)
    ap.add_argument("--exhaust_frac", type=float, default=0.6)
    ap.add_argument("--vac_drop_frac", type=float, default=0.5)
    ap.add_argument("--micro_tilt_thr", type=float, default=0.2)
    ap.add_argument("--min_gap_events", type=int, default=30)
    ap.add_argument("--edge_margin_ticks", type=float, default=0.5)
    ap.add_argument("--w_imb", type=float, default=2.0)
    ap.add_argument("--w_cancel", type=float, default=1.0)
    ap.add_argument("--w_tilt", type=float, default=1.0)
    ap.add_argument("--w_breakout", type=float, default=2.5)

    # execution/latency & protections
    ap.add_argument("--latency_ms", type=int, default=100, help="（保留参数，不再映射时间桶）")
    ap.add_argument("--storm_skip", action="store_true", help="事件风暴时跳过信号")
    ap.add_argument("--storm_threshold_ms", type=float, default=5.0, help="事件风暴阈值(ms)")
    ap.add_argument("--storm_lookback", type=int, default=20, help="事件风暴检测回看事件数")

    # position holding & exits
    ap.add_argument("--hold_sec", type=float, default=5.0)
    ap.add_argument("--queue_pos", type=float, default=10.0)
    ap.add_argument("--stop_ticks", type=float, default=0.0)
    ap.add_argument("--tp_ticks", type=float, default=0.0)
    ap.add_argument("--trail_ticks", type=float, default=0.0)
    ap.add_argument("--entry_mode", type=str, default="maker", choices=["maker","taker","pullback","hybrid"])
    ap.add_argument("--taker_slip_ticks", type=int, default=1)
    ap.add_argument("--time_stop_ms", type=int, default=2000)
    ap.add_argument("--pullback_ticks", type=int, default=1)
    ap.add_argument("--dwell_ms", type=int, default=200)
    ap.add_argument("--fill_timeout_ms", type=int, default=600)
    ap.add_argument("--min_pullback_l1", type=float, default=1.0)
    ap.add_argument("--hybrid_taker_edge", type=float, default=3.0)

    # misc
    ap.add_argument("--rth_only", action="store_true", help="仅回测RTH(09:30-16:00 ET)")
    ap.add_argument("--save_daily", action="store_true", help="输出按日汇总CSV")
    ap.add_argument("--report", action="store_true", help="生成图形化报告")
    ap.add_argument("--report_dir", default=None, help="报告输出目录")

    args = ap.parse_args()
    base = Path(args.output_dir); base.mkdir(parents=True, exist_ok=True)

    depth_df, quotes_df = load_topstep_data(args.db_path, args.start_time, args.end_time)
    if depth_df.empty and quotes_df.empty:
        logger.error("无数据"); return

    events = reconstruct_order_book_with_depth(depth_df, quotes_df, max_depth=args.max_depth, quote_default_l1_size=2.0)
    if events.empty:
        logger.error("重建失败/无快照"); return

    ev = filter_rth(events) if args.rth_only else events

    # Build event-driven triggers
    ev_trig = derive_event_triggers(
        ev,
        min_spread_ticks=args.min_spread_ticks,
        min_l1=args.min_volume_top,
        imb_win=args.imb_win,
        imb_thr=args.imb_thr,
        cancel_z_win=args.cancel_z_win,
        cancel_z_thr=args.cancel_z_thr,
        exhaust_frac=args.exhaust_frac,
        vac_drop_frac=args.vac_drop_frac,
        micro_tilt_thr=args.micro_tilt_thr,
        min_gap_events=args.min_gap_events,
        storm_skip=args.storm_skip,
        storm_threshold_ms=args.storm_threshold_ms,
        storm_lookback=args.storm_lookback,
        edge_margin_ticks=args.edge_margin_ticks,
        w_imb=args.w_imb,
        w_cancel=args.w_cancel,
        w_tilt=args.w_tilt,
        w_breakout=args.w_breakout,
    )

    # Backtest with queue model
    res = backtest_with_queue(
        ev_trig,
        hold_sec=args.hold_sec,
        queue_pos=args.queue_pos,
        stop_ticks=args.stop_ticks,
        tp_ticks=args.tp_ticks,
        trail_ticks=args.trail_ticks,
        min_hold_ms=0,
        entry_mode=args.entry_mode,
        taker_slip_ticks=args.taker_slip_ticks,
        time_stop_ms=args.time_stop_ms,
        pullback_ticks=args.pullback_ticks,
        dwell_ms=args.dwell_ms,
        fill_timeout_ms=args.fill_timeout_ms,
        min_pullback_l1=args.min_pullback_l1,
        hybrid_taker_edge=args.hybrid_taker_edge,
    )

    ts_tag = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    events_path = base / f"events_{ts_tag}.parquet"
    trades_path = base / f"trades_{ts_tag}.csv"
    summary_path = base / f"summary_{ts_tag}.json"

    ev_trig.to_parquet(events_path, index=False)
    res["trades"].to_csv(trades_path, index=False)
    with open(summary_path, "w") as f: json.dump(res["summary"], f, indent=2)

    logger.info(f"输出：\n- {events_path}\n- {trades_path}\n- {summary_path}")

    if args.report:
        rep_dir = Path(args.report_dir) if args.report_dir else base
        generate_report(res["trades"], rep_dir, ts_tag)
        logger.info(f"报告输出目录: {rep_dir}")

    print("\n=== 回测结果（事件驱动 · Scalper） ===")
    print(f"交易数: {res['summary']['trades']}")
    print(f"净收益: ${res['summary']['net_usd']:.2f}")
    print(f"胜率: {res['summary']['hit_rate']:.2%}")
    print(f"平均持仓: {res['summary']['avg_hold_secs']:.2f}s")

if __name__ == "__main__":
    main()