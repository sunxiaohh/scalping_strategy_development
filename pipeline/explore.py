"""Exploratory data analysis helpers."""
from __future__ import annotations

from typing import Dict, Any

import pandas as pd


def basic_stats(events: pd.DataFrame) -> Dict[str, Any]:
    """Return quick statistics of reconstructed event data.

    The function is intentionally lightweight so it can run on limited
    hardware.  It focuses on metrics that are often inspected by human
    scalpers such as average spread and distribution of top of book sizes.
    """
    if events.empty:
        return {}
    stats: Dict[str, Any] = {
        "events": len(events),
        "start": events["ts"].iloc[0],
        "end": events["ts"].iloc[-1],
        "avg_spread": events["spread"].mean(),
        "median_l1_bid": events["bid_vol_top"].median(),
        "median_l1_ask": events["ask_vol_top"].median(),
        "trade_through_bid": events["trade_through_bid"].sum(),
        "trade_through_ask": events["trade_through_ask"].sum(),
    }
    return stats

