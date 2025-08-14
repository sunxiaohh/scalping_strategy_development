# scalping_strategy_development

Research repository for building event-driven scalping strategies using
Topstep futures data.  The code base now includes a modular pipeline that
handles data loading, exploratory analysis, signal generation and
backtesting.

See [docs/PIPELINE.md](docs/PIPELINE.md) for details on the architecture
and how to extend each component.

## Quick Start

```bash
python -m pipeline.run_pipeline path/to/md_CON_F_US_EP_U25_20250811.duckdb outputs
```

The command above loads data, builds signals and runs a queue based
backtest.  Resulting events and trade summaries are written to the
`outputs` directory.

To focus on regular trading hours (e.g. 10:00–14:00 ET) add the
`--rth-only` flag:

```
python -m pipeline.run_pipeline path/to/md_CON_F_US_EP_U25_20250811.duckdb outputs \
    --rth-only --rth-start 10 --rth-end 14 --tz America/New_York
```

Explore multiple parameter combinations in batch with the sweep utility:

```
python -m pipeline.sweep path/to/db sweep_results \
    --hold-secs 1,3,5 --queue-pos 5,10 --entry-modes maker,taker
```

## Disclaimer

This repository is for research and educational purposes.  Use at your
own risk and always validate strategies on your own data before
considering live trading.

