# Package com.eignex.kumulant.stat.forecast

Predictive recurrences over ordered observations. Exponential smoothing
exposes `forecast(steps)` projections; recursive variance tracks volatility.
Each implements [com.eignex.kumulant.core.SeriesStat].

## Picking a forecaster

| Stat | Output shape | Reach for it when |
|------|--------------|-------------------|
| [ExponentialSmoothingStat] | Level + trend + seasonal factor vector (length `period`) + damped forecast | The stream has a slow-moving trend on top of noise (Holt, `period = 1`), optionally with a repeating cycle such as daily or weekly (Holt-Winters, additive or multiplicative). |
| [RecursiveVarianceStat] | GARCH-style volatility tracker `sigma² = omega + alpha * value² + beta * sigma²` | Volatility tracking where a long-run baseline matters; compared with [EwmaVarianceStat][com.eignex.kumulant.stat.decay.EwmaVarianceStat] it adds `omega` and decouples the shock and persistence coefficients. |

## Exponential smoothing

[ExponentialSmoothingStat] tracks two coupled cells:

- `level`: the smoothed running level, updated as
  `level = alpha * value + (1 - alpha) * (priorLevel + priorTrend)`.
- `trend`: the smoothed level-to-level change, updated as
  `trend = beta * (level - priorLevel) + (1 - beta) * priorTrend`.

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

## Recursive variance (GARCH-1,1)

[RecursiveVarianceStat] applies the standard GARCH(1,1) recurrence:
`sigma² = omega + alpha * value² + beta * sigma²`. Three coefficients:

- `omega`: long-run baseline variance.
- `alpha`: sensitivity to the latest shock (squared value).
- `beta`: persistence of the prior variance.

`alpha + beta < 1` is the standard stationarity condition; the long-run
variance is `omega / (1 - alpha - beta)`. Pick this over
[EwmaVarianceStat][com.eignex.kumulant.stat.decay.EwmaVarianceStat]
when the long-run floor matters or you want to decouple shock and
persistence.

## Merge

Both forecasters support approximate merges. An empty accumulator adopts
the remote state; a populated one averages its level/trend and seasonal factors,
or its variance, with the remote estimate. [ExponentialSmoothingStat] uses the
remote phase. These roll-ups enable composition and merge-based windowing, but do
not reproduce processing the combined observations in order.

## Concurrency

[ExponentialSmoothingStat] locks its coupled updates under
[com.eignex.kumulant.core.Concurrency.Strict] and
[com.eignex.kumulant.core.Concurrency.HighWrite]; relaxed cells can drift.
[RecursiveVarianceStat] uses a single-cell CAS recurrence under concurrent
modes. Concurrency does not establish chronological observation order.
