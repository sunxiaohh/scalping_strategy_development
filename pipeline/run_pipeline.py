"""High level pipeline orchestration for event-driven scalping research."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .data import load_raw_events
from .explore import basic_stats
from .signals import build_signals
from .backtest import run_backtest


def run(args: argparse.Namespace) -> None:
    events = load_raw_events(
        args.db_path,
        start_time=args.start,
        end_time=args.end,
        max_depth=args.max_depth,
    )
    stats = basic_stats(events)
    if not stats:
        print("No events loaded; aborting.")
        return
    print("Loaded events", json.dumps(stats, default=str, indent=2))

    ev_with_sig = build_signals(
        events,
        min_spread_ticks=args.min_spread_ticks,
        min_volume_top=args.min_volume_top,
    )
    result = run_backtest(
        ev_with_sig,
        hold_sec=args.hold_sec,
        queue_pos=args.queue_pos,
        stop_ticks=args.stop_ticks,
        tp_ticks=args.tp_ticks,
        trail_ticks=args.trail_ticks,
        entry_mode=args.entry_mode,
    )
    out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
    ev_with_sig.to_parquet(out / "events.parquet", index=False)
    result["trades"].to_csv(out / "trades.csv", index=False)
    with open(out / "summary.json", "w") as f:
        json.dump(result.get("summary", {}), f, indent=2)
    print("Pipeline finished; results saved to", out)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Run full scalping research pipeline")
    ap.add_argument("db_path", help="Path to DuckDB file")
    ap.add_argument("output_dir", help="Directory to store outputs")
    ap.add_argument("--start", help="Start time UTC")
    ap.add_argument("--end", help="End time UTC")
    ap.add_argument("--max_depth", type=int, default=10)
    ap.add_argument("--min_spread_ticks", type=int, default=1)
    ap.add_argument("--min_volume_top", type=float, default=2.0)
    ap.add_argument("--hold_sec", type=float, default=5.0)
    ap.add_argument("--queue_pos", type=float, default=10.0)
    ap.add_argument("--stop_ticks", type=float, default=0.0)
    ap.add_argument("--tp_ticks", type=float, default=0.0)
    ap.add_argument("--trail_ticks", type=float, default=0.0)
    ap.add_argument("--entry_mode", choices=["maker", "taker", "pullback", "hybrid"], default="maker")
    return ap


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()

