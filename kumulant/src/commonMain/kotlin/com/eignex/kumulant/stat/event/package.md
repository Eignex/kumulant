# Package com.eignex.kumulant.stat.event

Stats whose output is discrete-temporal in shape: last-seen timestamps,
level crossings, and run lengths.
They share the streaming-stats discipline but their results carry counts
of state changes and timestamps rather than numeric aggregates.

## Picking an event stat

| Stat | Result | Question |
|------|--------|----------|
| [RunLengthStat] | [RunLengthResult] | What's the current and longest consecutive truthy-run length? |
| [CrossingStat] | [CrossingResult] | How often does the input cross a configured level? |
| [RecencyStat] | [RecencyResult] | How long since the most recent observation? |

## When to reach for what

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

## Compose patterns

- `Mean.transform(ifThenElse(X gt threshold, 1.0, 0.0)).windowed(window)`
  for windowed fraction-meeting-threshold (SLO compliance).
- `RunLength.filter(predicate)` for "longest consecutive run satisfying
  a predicate".
- `Recency.filter(predicate)` for "time since last matching event".

## Merge

[CrossingStat] and [RecencyStat] merge exactly: crossing counts sum cell-wise
and recency takes the later last-seen timestamp. [RunLengthStat] merges approximately;
run-length state is order-dependent, so the merge combines the extremes (longest
run) without reconstructing the exact sequence.

## Concurrency

[CrossingStat] and [RecencyStat] keep a single coupled "previous value
+ counter" pair, locked under [com.eignex.kumulant.core.Concurrency.Strict]
and [com.eignex.kumulant.core.Concurrency.HighWrite]. [RunLengthStat]
tracks coupled run/longest cells; also locked.
