"""Signal generation using event-driven primitives."""
from __future__ import annotations

from typing import Optional

import pandas as pd

from event_driven_strategy import derive_event_triggers


def build_signals(
    events: pd.DataFrame,
    min_spread_ticks: int = 1,
    min_volume_top: float = 2.0,
    imb_win: int = 20,
    imb_thr: float = 0.25,
    cancel_z_win: int = 50,
    cancel_z_thr: float = 2.0,
    exhaust_frac: float = 0.6,
    vac_drop_frac: float = 0.5,
    micro_tilt_thr: float = 0.2,
    min_gap_events: int = 30,
    storm_skip: bool = False,
    storm_threshold_ms: float = 5.0,
    storm_lookback: int = 20,
    edge_margin_ticks: float = 0.5,
    w_imb: float = 2.0,
    w_cancel: float = 1.0,
    w_tilt: float = 1.0,
    w_breakout: float = 2.5,
) -> pd.DataFrame:
    """Apply event trigger logic and return DataFrame with signals."""
    if events.empty:
        return events
    return derive_event_triggers(
        events,
        min_spread_ticks=min_spread_ticks,
        min_l1=min_volume_top,
        imb_win=imb_win,
        imb_thr=imb_thr,
        cancel_z_win=cancel_z_win,
        cancel_z_thr=cancel_z_thr,
        exhaust_frac=exhaust_frac,
        vac_drop_frac=vac_drop_frac,
        micro_tilt_thr=micro_tilt_thr,
        min_gap_events=min_gap_events,
        storm_skip=storm_skip,
        storm_threshold_ms=storm_threshold_ms,
        storm_lookback=storm_lookback,
        edge_margin_ticks=edge_margin_ticks,
        w_imb=w_imb,
        w_cancel=w_cancel,
        w_tilt=w_tilt,
        w_breakout=w_breakout,
    )

