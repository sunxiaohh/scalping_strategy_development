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

## Disclaimer

This repository is for research and educational purposes.  Use at your
own risk and always validate strategies on your own data before
considering live trading.

