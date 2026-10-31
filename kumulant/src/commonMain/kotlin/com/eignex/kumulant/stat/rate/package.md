# Package com.eignex.kumulant.stat.rate

Throughput estimators. Every rate result implements [com.eignex.kumulant.core.HasRate],
so a downstream consumer can pull `rate` (events per second) or
`per(duration)` through one trait regardless of which underlying
estimator produced the snapshot.

## Picking a rate estimator

| Stat | Reach for it when |
|------|-------------------|
| [RateStat] | End-to-end throughput where every `update` represents one event. Reports the accumulated value divided by the wall-clock span since the first update. |
| [CounterRateStat] | The underlying signal is itself a monotonically-increasing counter (packet counter, byte counter, CPU cycle count pulled from another process). Differentiates the counter to recover an event rate; configurable whether a decrease means a reset or a negative rate. |
| [DecayingSumStat][com.eignex.kumulant.stat.decay.DecayingSumStat] | You want a smooth, responsive metric. Its result's `rate` is the exponentially-decayed events-per-second; it tracks recent activity more sharply than [RateStat] (which weights uniformly since the start). |

## Result shapes

[RateStat] and [CounterRateStat] produce a [RateResult] holding the window start,
the accumulated value, and the snapshot timestamp; `rate` divides the value by the
elapsed span. [com.eignex.kumulant.stat.decay.DecayingSumResult] derives `rate` from
its decayed sum and half-life.

## Time semantics

Rates are inherently timestamp-driven. Each stat uses the
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
- `DecayingSum.transform(ifThenElse(predicate, 1.0, 0.0))` for an
  exponentially-decayed predicate-match rate.

## Merge

All three merge exactly. [RateStat] and [CounterRateStat] sum their event /
delta totals cell-wise and take the earlier window start;
[com.eignex.kumulant.stat.decay.DecayingSumStat] aligns the incoming sum to its
landmark before adding it. Workers can track slices of the same stream and the
coordinator merges snapshots without bias.

## Concurrency

[RateStat] is additive over uncoupled cells; update count and timestamp range
are independent. Exact under every [com.eignex.kumulant.core.Concurrency] level.
[CounterRateStat] is Welford-coupled (previous counter reading + current rate)
and locks under [com.eignex.kumulant.core.Concurrency.Strict] /
[com.eignex.kumulant.core.Concurrency.HighWrite].
[com.eignex.kumulant.stat.decay.DecayingSumStat] locks its body under every
concurrent level.
