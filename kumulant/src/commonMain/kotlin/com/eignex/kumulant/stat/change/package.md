# Package com.eignex.kumulant.stat.change

Drift detectors. Each member tracks a running statistic, applies a
configurable threshold, and exposes an alarm flag on its result. The
three differ in what they assume about the in-control state and how
they decide a shift has occurred.

## Picking a detector

| Stat | Result | Reach for it when |
|------|--------|-------------------|
| [CusumStat] | [CusumResult] | The in-control mean is known up front. Configure target, allowance, threshold; the two-sided cumulative sum alarm fires once it crosses threshold. |
| [PageHinkleyStat] | [PageHinkleyResult] | The in-control mean must be learned online. Tracks cumulative deviation from the running mean against a tolerance and a threshold. |
| [AdwinStat] | [AdwinResult] | Neither a target nor a fixed window is known. Maintains an exponential-histogram window of recent observations and drops the older portion whenever a statistically-significant mean shift is detected against the Hoeffding bound. |

## How they compare

[CusumStat] is the strictest and the cheapest if you already know the
in-control target. It tracks two cumulative deviations from the target
and alarms when either crosses a configured threshold; the allowance
absorbs in-control noise. Suitable for monitoring against a known SLO
or a calibrated baseline.

[PageHinkleyStat] is the next step up: same one-sided cumulative-deviation
idea but the reference is the running mean rather than a fixed target.
Add a tolerance to absorb in-control fluctuation. Suitable for streams
whose baseline drifts over time but where you still want to alarm on
sudden shifts.

[AdwinStat] is the heaviest and the most flexible. Maintains a window
of recent observations as an exponential histogram of buckets; on every
update it checks whether the window can be split into a recent portion
and an older portion that differ by more than the Hoeffding bound at
confidence `delta`. When such a split exists, the older portion is
dropped and the running statistic reflects only the post-shift regime.
Suitable when the stream may have multiple regimes and you want the
detector to track the current one without explicit reset.

## Output

All three produce a result with the running running statistic plus an
`alarm: Boolean` field. The alarm doesn't auto-reset; caller decides
what to do (page, reset the stat, write to an event store). Repeated
alarm reads return the same value until the underlying statistic falls
back below threshold.

For drift detectors on classification accuracy specifically, feed the
binary correctness signal `1[predicted == truth]` through one of these
stats. The same pattern works for any binary signal.

## Merge

All three drift detectors merge only approximately; the state is an
order-dependent recurrence with no exact parallel combine. [CusumStat] and
[PageHinkleyStat] average their cumulative-deviation cells (and [PageHinkleyStat]
weight-averages the running mean); [AdwinStat] carries over the change counter
without reconstructing the windowed histogram. Treat merge as a roll-up
convenience; for distributed drift detection, run a detector per stream rather
than merging partials.

## Concurrency

All three keep coupled state (running statistic + previous reference)
that has to stay consistent across reads. Locked under
[com.eignex.kumulant.core.Concurrency.Strict] /
[com.eignex.kumulant.core.Concurrency.HighWrite]. Under
[com.eignex.kumulant.core.Concurrency.Relaxed] the cells race and
the alarm signal may flicker briefly under contention.
