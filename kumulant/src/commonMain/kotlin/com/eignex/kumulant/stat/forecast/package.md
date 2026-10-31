# Package com.eignex.kumulant.stat.forecast

Predictive recurrences over ordered observations. Exponential smoothing
exposes `forecast(steps)` projections and implements
[com.eignex.kumulant.core.SeriesStat].

## Picking a forecaster

| Stat | Output shape | Reach for it when |
|------|--------------|-------------------|
| [ExponentialSmoothingStat] | Level + trend + seasonal factor vector (length `period`) + damped forecast | The stream has a slow-moving trend on top of noise (Holt, `period = 1`), optionally with a repeating cycle such as daily or weekly (Holt-Winters, additive or multiplicative). |

## Exponential smoothing

[ExponentialSmoothingStat] tracks two coupled cells:

- `level`: the smoothed running level, updated as
  `level = a * value + (1 - a) * (priorLevel + phi * priorTrend)`.
- `trend`: the smoothed level-to-level change, updated as
  `trend = b * (level - priorLevel) + (1 - b) * phi * priorTrend`.

`alpha` and `beta` are decay rates per unit of weight, not smoothing factors:
the steps are `a = 1 - exp(-alpha * weight)` and `b = 1 - exp(-beta * weight)`,
so any finite rate `>= 0` is valid.

The result exposes `forecast(steps)` which projects the level forward
by a damped trend: `forecast(h) = level + phi * (1 - phi^h) / (1 - phi) * trend`.
The damping factor `phi` in `(0, 1]` geometrically discounts the trend's
contribution to long-horizon forecasts; `phi = 1.0` recovers the
classical undamped Holt. Pick `phi` close to 1 for streams with
sustained trends, lower (~0.8) for streams where the trend tends to
revert.

## Seasonality (Holt-Winters)

A `period` above 1 adds a `seasonal[period]` vector, smoothed by `gamma`.
Each update applies one of two compositions:

- **Additive**: `level + seasonal[t mod period]` (use when the
  seasonal amplitude is independent of level).
- **Multiplicative**: `level * seasonal[t mod period]` (use when the
  seasonal amplitude scales with level; typical for ratios, sales,
  anything where seasonal effects multiply rather than add).

`forecast(steps)` then factors in the appropriate seasonal slot for each step.
At `period = 1` the single seasonal factor stays at its identity and the
stat is plain Holt.

## Merge

[ExponentialSmoothingStat] supports approximate merges. An empty accumulator
adopts the remote state; a populated one averages its level/trend and seasonal
factors with the remote estimate and takes the remote phase. These roll-ups enable composition and merge-based windowing, but do
not reproduce processing the combined observations in order.

## Concurrency

[ExponentialSmoothingStat] locks its coupled updates under
[com.eignex.kumulant.core.Concurrency.Strict] and
[com.eignex.kumulant.core.Concurrency.HighWrite]; relaxed cells can drift.
Concurrency does not establish chronological observation order.
