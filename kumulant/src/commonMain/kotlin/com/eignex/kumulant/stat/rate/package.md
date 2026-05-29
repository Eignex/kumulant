# Package com.eignex.kumulant.stat.rate

Throughput estimators. All three implement [com.eignex.kumulant.core.HasRate],
so a downstream consumer can pull `rate` (events per second) or
`per(duration)` through one trait regardless of which underlying
estimator produced the snapshot.

## Picking a rate estimator

| Stat | Reach for it when |
|------|-------------------|
| [RateStat] | End-to-end throughput where every `update` represents one event. Reports observation count divided by the wall-clock span of the window. |
| [CounterRateStat] | The underlying signal is itself a monotonically-increasing counter (packet counter, byte counter, CPU cycle count pulled from another process). Differentiates the counter to recover an event rate; configurable whether a decrease means a reset or a negative rate. |
| [DecayingRateStat] | You want a smooth, responsive metric. Exponentially-decayed events-per-second; tracks recent activity more sharply than [RateStat] (which weights uniformly across the window). |

## Result shapes

All three produce a [RateResult] that exposes:

- `totalWeights`: events folded in.
- `elapsedNanos`: the wall-clock span the rate is normalised over.
- `rate`: events per second (via [com.eignex.kumulant.core.HasRate]).
- `per(duration)`: events per arbitrary `kotlin.time.Duration`.

## Time semantics

Rates are inherently timestamp-driven. All three stats use the
`timestampNanos` argument on `update` as the ordering signal. When
feeding from a queue or replaying a log, pass a monotonic stamp; the
no-stamp overload calls `currentTimeNanos()` which is wall-clock and may
be non-monotonic across system clock changes.

[CounterRateStat] specifically requires its input value to be the
counter reading (not a delta). If the counter decreases between two
updates and `treatDecreaseAsReset = true`, the stat treats the decrease
as a counter reset (e.g. process restart, counter rollover) and the
rate floor is 0. With `treatDecreaseAsReset = false`, decreases produce
negative rates.

## Compose patterns

- `Rate.windowed(duration)` for windowed throughput: the windowed
  wrapper produces a sliding rate over the configured duration.
- `DecayingRate.transform(IfExpr(predicate, 1.0, 0.0))` for an
  exponentially-decayed predicate-match rate.

## Merge

All three merge exactly. [RateStat] and [CounterRateStat] sum their event /
delta totals cell-wise and take the earlier window start; [DecayingRateStat]
delegates to [com.eignex.kumulant.stat.decay.DecayingSumStat]'s exact merge and
re-projects the rate. Workers can track slices of the same stream and the
coordinator merges snapshots without bias.

## Concurrency

[RateStat] and [DecayingRateStat] are additive over uncoupled cells;
update count and timestamp range are independent. Exact under every
[com.eignex.kumulant.core.Concurrency] level. [CounterRateStat] is
Welford-coupled (previous counter reading + current rate) and locks
under [com.eignex.kumulant.core.Concurrency.Strict] /
[com.eignex.kumulant.core.Concurrency.HighWrite].
