# Event-Driven Scalping Research Pipeline

This repository provides a minimal yet extendable pipeline for building
and backtesting event-driven scalping strategies.  The design follows a
"think big, do small" philosophy: every step is modular and can be
validated independently before moving to the next stage.

## Steps

1. **Data Loading** – `pipeline.data.load_raw_events`
   - Reads the raw Topstep DuckDB files and reconstructs per-event order
     book snapshots.
   - Output: DataFrame with best bid/ask, volumes, trade-through and
     cancel statistics.

2. **Exploratory Analysis** – `pipeline.explore.basic_stats`
   - Computes lightweight descriptive statistics used by human scalpers.
   - Intended for sanity checks before feature engineering.

3. **Signal Construction** – `pipeline.signals.build_signals`
   - Applies event-based primitives (break-outs, vacuum cancels and
     aggressor imbalance) to produce directional signals and edge
     estimates.
   - Parameters mirror those used by experienced discretionary traders
     and can be tuned for different markets.

4. **Backtesting** – `pipeline.backtest.run_backtest`
   - Simulates queue-based execution using the generated signals.
   - Supports maker, taker and hybrid entries with configurable
     protections.

5. **Orchestration** – `pipeline.run_pipeline`
   - Command line entry point that ties the above steps together and
     writes events, trades and summary statistics to an output folder.
   - Supports filtering to regular trading hours via `--rth-only` and
     custom timezone/hour ranges.

6. **Parameter Sweep** – `pipeline.sweep.run_sweep`
   - Loads events/signals once and evaluates multiple backtest
     configurations in a grid search.
   - Useful for quickly exploring hold times, queue positions and entry
     modes to locate profitable regions.

## Usage

```bash
python -m pipeline.run_pipeline <path-to-db> output_dir \
    --start 2025-08-11T13:00:00Z --end 2025-08-11T14:00:00Z
```

Each module can also be imported independently for iterative research in
interactive environments or notebooks.

To batch test combinations of parameters:

```bash
python -m pipeline.sweep path/to/db sweep_out \
    --start 2025-08-11T13:00:00Z --end 2025-08-11T14:00:00Z \
    --hold-secs 1,3,5 --queue-pos 5,10 --entry-modes maker,taker
```

Results are saved to `sweep_out/sweep_results.csv` for further analysis.

## Next Steps

- Perform deeper statistical validation on generated signals.
- Experiment with alternative event primitives or machine-learning based
  scoring models.
- Integrate brokerage execution or paper trading to move from research
  to deployment.

