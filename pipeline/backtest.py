"""Backtesting helpers."""
from __future__ import annotations

from typing import Dict, Any

import pandas as pd

from event_driven_strategy import backtest_with_queue


def run_backtest(
    events_with_signals: pd.DataFrame,
    hold_sec: float = 5.0,
    queue_pos: float = 10.0,
    stop_ticks: float = 0.0,
    tp_ticks: float = 0.0,
    trail_ticks: float = 0.0,
    min_hold_ms: int = 0,
    entry_mode: str = "maker",
    taker_slip_ticks: int = 1,
    time_stop_ms: int = 2000,
    pullback_ticks: int = 1,
    dwell_ms: int = 200,
    fill_timeout_ms: int = 600,
    min_pullback_l1: float = 1.0,
    hybrid_taker_edge: float = 3.0,
) -> Dict[str, Any]:
    """Run queue-based backtest using pre-computed signals."""
    if events_with_signals.empty:
        return {"trades": pd.DataFrame(), "summary": {}}
    result = backtest_with_queue(
        events_with_signals,
        hold_sec=hold_sec,
        queue_pos=queue_pos,
        stop_ticks=stop_ticks,
        tp_ticks=tp_ticks,
        trail_ticks=trail_ticks,
        min_hold_ms=min_hold_ms,
        entry_mode=entry_mode,
        taker_slip_ticks=taker_slip_ticks,
        time_stop_ms=time_stop_ms,
        pullback_ticks=pullback_ticks,
        dwell_ms=dwell_ms,
        fill_timeout_ms=fill_timeout_ms,
        min_pullback_l1=min_pullback_l1,
        hybrid_taker_edge=hybrid_taker_edge,
    )
    return result

