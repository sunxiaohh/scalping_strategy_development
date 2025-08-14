"""Parameter sweep utilities for scalping backtests."""
from __future__ import annotations

import argparse
from itertools import product
from pathlib import Path
from typing import List

import pandas as pd

from .data import load_raw_events, filter_rth
from .signals import build_signals
from .backtest import run_backtest


def run_sweep(
    db_path: str,
    output_dir: str,
    start: str | None = None,
    end: str | None = None,
    hold_secs: List[float] | None = None,
    queue_pos: List[float] | None = None,
    entry_modes: List[str] | None = None,
    rth_only: bool = False,
    rth_start: int = 10,
    rth_end: int = 14,
    tz: str = "America/New_York",
) -> pd.DataFrame:
    """Run grid search across parameter combinations and save results."""
    events = load_raw_events(db_path, start_time=start, end_time=end)
    if rth_only:
        events = filter_rth(events, start_hour=rth_start, end_hour=rth_end, tz=tz)
    if events.empty:
        raise ValueError("No events loaded for sweep")
    ev_sig = build_signals(events)

    hold_secs = hold_secs or [5.0]
    queue_pos = queue_pos or [10.0]
    entry_modes = entry_modes or ["maker"]

    combos = list(product(hold_secs, queue_pos, entry_modes))
    results = []
    for hold, qpos, mode in combos:
        res = run_backtest(ev_sig, hold_sec=hold, queue_pos=qpos, entry_mode=mode)
        summary = res.get("summary", {}).copy()
        summary.update({"hold_sec": hold, "queue_pos": qpos, "entry_mode": mode})
        results.append(summary)

    df = pd.DataFrame(results)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    df.to_csv(out / "sweep_results.csv", index=False)
    return df


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Run parameter sweep on scalping strategy")
    ap.add_argument("db_path")
    ap.add_argument("output_dir")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--hold-secs", default="5", dest="hold_secs")
    ap.add_argument("--queue-pos", default="10", dest="queue_pos")
    ap.add_argument("--entry-modes", default="maker", dest="entry_modes")
    ap.add_argument("--rth-only", action="store_true", dest="rth_only")
    ap.add_argument("--rth-start", type=int, default=10, dest="rth_start")
    ap.add_argument("--rth-end", type=int, default=14, dest="rth_end")
    ap.add_argument("--tz", default="America/New_York")
    return ap


def parse_csv_floats(text: str) -> List[float]:
    return [float(x) for x in text.split(",") if x]


def parse_csv_str(text: str) -> List[str]:
    return [x for x in text.split(",") if x]



def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    df = run_sweep(
        args.db_path,
        args.output_dir,
        start=args.start,
        end=args.end,
        hold_secs=parse_csv_floats(args.hold_secs),
        queue_pos=parse_csv_floats(args.queue_pos),
        entry_modes=parse_csv_str(args.entry_modes),
        rth_only=args.rth_only,
        rth_start=args.rth_start,
        rth_end=args.rth_end,
        tz=args.tz,
    )
    print(df)


if __name__ == "__main__":
    main()
