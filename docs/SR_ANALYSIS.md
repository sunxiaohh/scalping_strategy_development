# Support/Resistance Event Flow Exploration

This note outlines a lightweight procedure to study how order flow behaves when the
market revisits intraday support and resistance levels. It can be used as a
starting point for scalpers who wish to blend market context with event-level
features.

## Steps

1. **Reconstruct events** with `pipeline.data.load_raw_events` and filter to
   regular trading hours via `pipeline.data.filter_rth`.
2. **Derive per-minute levels** using `pipeline.sr_analysis.minute_levels`, which
   computes the high (resistance) and low (support) of the mid price for each
   minute.
3. **Tag events near the levels** with `pipeline.sr_analysis.tag_sr_events`. An
   event is considered "near" a level when its mid price is within one tick
   (`proximity_ticks=1.0` by default) of the minute high/low.
4. **Evaluate order flow** using `pipeline.sr_analysis.sr_flow_stats`, which
   summarises imbalance, cancellation ratios and microprice tilt for events
   occurring close to support or resistance.

These tools are deliberately simple: they avoid future information, operate on
unaltered event snapshots and can be extended with additional contextual
features (e.g. session VWAP, higher time-frame pivots) to research potential
edges around price levels.
