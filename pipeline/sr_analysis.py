"""Support/resistance exploratory analysis using event data."""
from __future__ import annotations

import pandas as pd


def minute_levels(events: pd.DataFrame) -> pd.DataFrame:
    """Return 1-minute support/resistance levels.

    Parameters
    ----------
    events:
        DataFrame with ``ts`` (UTC timestamp) and ``mid_price`` columns.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns ``ts`` (minute timestamp), ``resistance`` (high)
        and ``support`` (low) representing per-minute extremes.
    """
    if events.empty:
        return pd.DataFrame(columns=["ts", "resistance", "support"])
    bars = (
        events.set_index("ts")["mid_price"].resample("1min").agg(["max", "min"])
    )
    bars.rename(columns={"max": "resistance", "min": "support"}, inplace=True)
    return bars.reset_index()


def tag_sr_events(
    events: pd.DataFrame,
    levels: pd.DataFrame,
    tick_size: float = 0.25,
    proximity_ticks: float = 1.0,
) -> pd.DataFrame:
    """Annotate events occurring near support or resistance.

    Parameters
    ----------
    events:
        Event-level DataFrame with ``ts`` and ``mid_price``.
    levels:
        Output from :func:`minute_levels`.
    tick_size:
        Minimum price increment.
    proximity_ticks:
        Distance in ticks to consider "near" a level.

    Returns
    -------
    pandas.DataFrame
        Copy of ``events`` with boolean ``near_res`` and ``near_sup`` columns
        indicating proximity to per-minute resistance/support respectively.
    """
    if events.empty or levels.empty:
        ev = events.copy(); ev["near_res"] = False; ev["near_sup"] = False
        return ev
    ev = events.copy()
    ev["minute"] = ev["ts"].dt.floor("min")
    tagged = ev.merge(levels, left_on="minute", right_on="ts", how="left", suffixes=("", "_bar"))
    tol = tick_size * proximity_ticks
    tagged["near_res"] = tagged["resistance"].notna() & (
        (tagged["mid_price"] - tagged["resistance"]).abs() <= tol
    )
    tagged["near_sup"] = tagged["support"].notna() & (
        (tagged["mid_price"] - tagged["support"]).abs() <= tol
    )
    return tagged.drop(columns=["ts_bar"])


def sr_flow_stats(tagged: pd.DataFrame) -> pd.DataFrame:
    """Compute basic order-flow statistics near support/resistance.

    Parameters
    ----------
    tagged:
        DataFrame produced by :func:`tag_sr_events` containing ``imb``,
        ``bid_cancel_ratio`` and ``ask_cancel_ratio`` columns.

    Returns
    -------
    pandas.DataFrame
        Summary statistics for support and resistance zones.
    """
    rows = []
    for label, mask in [("support", tagged["near_sup"]), ("resistance", tagged["near_res"])]:
        zone = tagged.loc[mask]
        if zone.empty:
            rows.append({"level": label, "events": 0})
        else:
            rows.append(
                {
                    "level": label,
                    "events": len(zone),
                    "avg_imb": zone["imb"].mean(),
                    "avg_bid_cxl": zone["bid_cancel_ratio"].mean(),
                    "avg_ask_cxl": zone["ask_cancel_ratio"].mean(),
                    "avg_tilt": zone["tilt_ticks"].mean(),
                }
            )
    return pd.DataFrame(rows)


__all__ = ["minute_levels", "tag_sr_events", "sr_flow_stats"]
