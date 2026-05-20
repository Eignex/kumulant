# Concurrency

Every Stat picks a concurrency contract at construction. The contract is
user-facing: you say what you need, and the stat picks the cell encoding
and lock strategy that honours it for its mathematical structure.

```kotlin
val hits = SumStat(concurrency = Concurrency.HighWrite)
val ols = UnivariateRegressionStat(concurrency = Concurrency.Strict)
```

A schema can also propagate one mode to every registered stat:

```kotlin
object Telemetry : StatSchema(concurrency = Concurrency.Strict) {
    val latencyMean by series(Mean)
    val errorRate by series(Rate)
}
```

## The four levels

### None

Single-threaded. No atomics, no locks. This is the default and the
cheapest path; it is correct when one thread owns the accumulator.

A None stat hit by two threads at once falls out of unsynchronised
reads and writes. Exceptions and state corruption are both possible, so
do not share a None stat across threads.

### Relaxed

Lock-free atomics on every cell. Many writers, no blocking.

For stats whose state is one independent cell (SumStat, MinStat,
MaxStat, the Bernoulli sum, the counter), Relaxed is exact under
concurrency because increments and CAS-min/max commute.

For stats whose state is coupled across cells (Welford-style mean,
variance, and moments stats), Relaxed lets the cells race. Independent
racers reading a stale numerator against an updated denominator can
produce results that drift by ULPs up to roughly 1e-5 relative under
heavy contention. The drift does not compound and the stat never
throws. Pick Relaxed on hot paths where a percentile metric that is
0.001 percent off is preferable to writers blocking on a lock.

### Strict

Multi-threaded with whatever synchronisation each stat needs to stay
exact across coupled state. For Welford-style stats this is a lock
around the body of update. For sketches that already self-serialise
(DDSketch, t-digest, HDR, and so on) Strict is the natural mode and
adds no extra overhead.

Every writer goes through the same lock for coupled stats. Shard the
work and merge when the write rate swamps a single lock.

### HighWrite

JVM-only striped adders for additive stats. On a heavily-loaded SumStat
or CountStat this scales linearly with the writer count.

On non-JVM platforms HighWrite falls back to Strict. For stats where
striping makes no sense (anything coupled, anything sketch-based)
HighWrite also behaves like Strict.

## Picking a mode

A single-threaded producer should pick None. Many writers on an
additive stat (sum, count, Bernoulli sum) should pick HighWrite on the
JVM and Relaxed elsewhere. Many writers that can tolerate ULP-level
drift on coupled state in exchange for not blocking should pick
Relaxed. Many writers that need exact arithmetic should pick Strict.
Many writers on a sketch should also pick Strict, because sketches
self-serialise and the other modes degrade to it anyway.

## Per-stat concurrency clauses

Each stat's KDoc has a concurrency section that names the mechanism and
the per-level behaviour. The recurring patterns are: a single atomic
add per update (exact under every level, swapped for a striped adder
under HighWrite); a single-cell CAS min or max loop (exact under every
level, with the retry serialising racing writers naturally);
independent striped cells with deterministic bucket assignment (exact
under every level, used by BernoulliSumStat and friends);
Welford-coupled cells without a lock (exact under None, Strict, and
HighWrite, with Relaxed drifting a small amount without throwing); and
a body locked under any concurrent level (exact under every level with
throughput bounded by lock contention).

Bandits inherit the concurrency of their per-arm stat; see
[Bandits](06-bandits.md).

## Snapshot consistency

A read call is best-effort across cells. A reader that races with a
writer on a Relaxed Welford stat may see the count incremented before
the mean is. The drift is the same as the writer-on-writer case: small
in magnitude, no exceptions, no corruption.

For a strictly atomic snapshot, use Strict; the per-stat lock will
serialise readers behind the writer.
