# Package com.eignex.kumulant.stat.event

Stats whose output is discrete-temporal in shape: state transitions,
dwell times, last-seen timestamps, level crossings, peak excursions.
They share the streaming-stats discipline but their results carry counts
of state changes and timestamps rather than numeric aggregates.

## Picking an event stat

| Stat | Result | Question |
|------|--------|----------|
| [ExcursionStat] | [ExcursionResult] | What's the largest peak-to-subsequent-trough excursion seen? |
| [RunLengthStat] | [RunLengthResult] | What's the current and longest consecutive truthy-run length? |
| [CrossingStat] | [CrossingResult] | How often does the input cross a configured level? |
| [RecencyStat] | [RecencyResult] | How long since the most recent observation? |
| [SojournStat] | [SojournResult] | How long has each categorical state been occupied? |

## When to reach for what

- **ExcursionStat** for drawdown / recovery monitoring, or any
  "how far has the signal fallen from its running high" question.
  Tracks the running peak, the lowest value seen since the peak was last
  set, and the largest peak-to-subsequent-trough excursion observed.

- **RunLengthStat** for the classic "longest streak" diagnostic. Truthy
  means a nonzero, non-NaN value, so feeding it the output of a
  predicate produces consecutive-match-count metrics. Tracks both the
  current run and the longest observed run.

- **CrossingStat** when the useful signal is "how active is this around
  a threshold"; zero-crossings of a centred series, threshold-touch
  counts on an SLO, transitions through any level. Reports up-crossings
  and down-crossings separately.

- **RecencyStat** for staleness checks and last-error-seen monitors.
  Compose with [com.eignex.kumulant.schema.ops.filter] on the spec side for
  "time since the last matching event" diagnostics. The result reads
  elapsed time at snapshot time, not at update time, so a stale stream
  still reports a growing elapsed time even without new updates.

- **SojournStat** for uptime / availability breakdowns and any
  dwell-time accounting where the state alphabet is known up front.
  The result carries per-state total nanos, per-state transition counts,
  the current state, and the current dwell. Configure the alphabet at
  construction; out-of-alphabet values are dropped.

## Compose patterns

- `Mean.transform(IfExpr(X gt threshold, 1.0, 0.0)).windowed(window)`
  for windowed fraction-meeting-threshold (SLO compliance).
- `RunLength.filter(predicate)` for "longest consecutive run satisfying
  a predicate".
- `Recency.filter(predicate)` for "time since last matching event".
- Compose [CrossingStat] with [ExcursionStat] for both "how often" and
  "how far" diagnostics over the same signal.

## Merge

[CrossingStat], [RecencyStat], and [SojournStat] merge exactly: crossing counts
sum cell-wise, recency takes the later last-seen timestamp, sojourn sums
per-state nanos and transition counts. [ExcursionStat] and [RunLengthStat] merge
approximately; peak/trough and run-length state is order-dependent, so the merge
combines the extremes (max peak, min trough, longest run) without reconstructing
the exact sequence.

## Concurrency

[CrossingStat] and [RecencyStat] keep a single coupled "previous value
+ counter" pair, locked under [com.eignex.kumulant.core.Concurrency.Strict]
and [com.eignex.kumulant.core.Concurrency.HighWrite]. [ExcursionStat]
and [RunLengthStat] track coupled peak/trough or run/longest cells;
also locked. [SojournStat] keeps per-state weight cells plus a current-
state cell; the state cell is the coupled piece.
