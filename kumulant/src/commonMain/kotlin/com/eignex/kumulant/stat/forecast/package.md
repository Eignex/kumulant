# Package com.eignex.kumulant.stat.forecast

Predictive recurrences with multi-cell state. Their results expose
`forecast(steps)` projections, distinguishing them from the
[decay][com.eignex.kumulant.stat.decay] family's
running-moment shape.

## The three members

| Stat | Output shape | Reach for it when |
|------|--------------|-------------------|
| [HoltStat] | Level + trend + damped forecast | The stream has a slow-moving trend on top of noise and you want a short-horizon forecast. |
| [SeasonalSmoothingStat] | Level + trend + seasonal factor vector (length `period`) | The stream has a repeating cycle (daily, weekly, anything periodic) on top of level and trend. The classical Holt-Winters method, additive or multiplicative. |
| [RecursiveVarianceStat] | GARCH-style volatility tracker `sigma² = omega + alpha * value² + beta * sigma²` | Volatility tracking where a long-run baseline matters; compared with [EwmaVarianceStat][com.eignex.kumulant.stat.decay.EwmaVarianceStat] it adds `omega` and decouples the shock and persistence coefficients. |

## Holt's method

[HoltStat] tracks two coupled cells:

- `level`: the smoothed running level, updated as
  `level = alpha * value + (1 - alpha) * (priorLevel + priorTrend)`.
- `trend`: the smoothed level-to-level change, updated as
  `trend = beta * (level - priorLevel) + (1 - beta) * priorTrend`.

The result exposes `forecast(steps)` which projects the level forward
by a damped trend: `forecast(h) = level + (1 - phi^h) / (1 - phi) * trend`.
The damping factor `phi` in `(0, 1]` geometrically discounts the trend's
contribution to long-horizon forecasts; `phi = 1.0` recovers the
classical undamped Holt. Pick `phi` close to 1 for streams with
sustained trends, lower (~0.8) for streams where the trend tends to
revert.

## Seasonal smoothing (Holt-Winters)

[SeasonalSmoothingStat] adds a `seasonal[period]` vector to Holt's
shape. Each update applies one of two compositions:

- **Additive**: `level + seasonal[t mod period]` (use when the
  seasonal amplitude is independent of level).
- **Multiplicative**: `level * seasonal[t mod period]` (use when the
  seasonal amplitude scales with level; typical for ratios, sales,
  anything where seasonal effects multiply rather than add).

The result exposes the same `forecast(steps)` projection, now factoring
in the appropriate seasonal slot for each step.

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

## Concurrency

All three stats keep multi-cell coupled state (level + trend; level +
trend + seasonal vector; variance + omega/alpha/beta parameters) and
need consistent reads. Locked under [com.eignex.kumulant.core.Concurrency.Strict]
/ [com.eignex.kumulant.core.Concurrency.HighWrite]. Under
[com.eignex.kumulant.core.Concurrency.Relaxed] the cells race; results
may drift briefly under contention but never throw.
