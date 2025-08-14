"""Data access and preprocessing utilities for scalping pipeline."""
from __future__ import annotations

from pathlib import Path
from typing import Tuple, Optional

import pandas as pd

from event_driven_strategy import load_topstep_data, reconstruct_order_book_with_depth


def load_raw_events(
    db_path: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    max_depth: int = 10,
    quote_default_l1_size: float = 2.0,
) -> pd.DataFrame:
    """Load raw data from DuckDB and reconstruct event level order book.

    Parameters
    ----------
    db_path:
        Path to the DuckDB database.
    start_time, end_time:
        Optional time bounds (UTC ISO format).
    max_depth:
        Depth of order book to reconstruct.
    quote_default_l1_size:
        Default size when quote stream reports zero at L1.

    Returns
    -------
    pandas.DataFrame
        Event-level snapshots with best bid/ask, volume information and
        trade/cancel counts. See :func:`event_driven_strategy.reconstruct_order_book_with_depth`.
    """
    depth_df, quotes_df = load_topstep_data(db_path, start_time, end_time)
    if depth_df.empty and quotes_df.empty:
        return pd.DataFrame()
    events = reconstruct_order_book_with_depth(
        depth_df, quotes_df, max_depth=max_depth, quote_default_l1_size=quote_default_l1_size
    )
    return events

