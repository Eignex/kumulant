package com.eignex.kumulant.core

/**
 * User-facing concurrency contract for stats. Each stat translates the chosen level
 * into a cell-encoding and lock strategy that honours it for that stat's
 * mathematical structure.
 *
 * Bare-stat construction defaults to [None]. To give a coherent bag of stats one
 * contract, declare them in a [com.eignex.kumulant.schema.runtime.StatSchema] and pass
 * the level to the group that materializes it, which applies it to every stat the schema
 * describes. The schema itself carries no level, so the same one can back a single-threaded
 * shard and a contended coordinator.
 *
 * ## Picking a mode
 *
 * - A single-threaded producer should pick [None].
 * - Many writers on an additive stat (sum, count, Bernoulli sum) should pick
 *   [HighWrite] on the JVM and [Relaxed] elsewhere.
 * - Many writers that can tolerate ULP-level drift on coupled state in exchange
 *   for not blocking should pick [Relaxed].
 * - Many writers that need exact arithmetic should pick [Strict].
 * - Many writers on a sketch should also pick [Strict]: sketches self-serialise
 *   and the other modes degrade to it anyway.
 *
 * ## Per-stat concurrency clauses
 *
 * Each stat's KDoc has a `**Concurrency:**` section naming the mechanism and
 * per-level behaviour. Recurring patterns:
 *
 * - A single atomic add per update: exact under every level; swapped for a
 *   striped adder under [HighWrite].
 * - A single-cell CAS min or max loop: exact under every level; the retry
 *   serialises racing writers naturally.
 * - Independent striped cells with deterministic bucket assignment: exact under
 *   every level (used by additive histograms, `BernoulliSumStat`, and friends).
 * - Welford-coupled cells without a lock: exact under [None], [Strict], and
 *   [HighWrite]; [Relaxed] drifts a small amount without throwing.
 * - A body locked under any concurrent level: exact under every level with
 *   throughput bounded by lock contention.
 *
 * Bandits inherit the concurrency of their per-arm stat.
 *
 * ## Snapshot consistency
 *
 * A `read()` call is best-effort across cells. A reader that races with a writer
 * on a [Relaxed] Welford stat may see the count incremented before the mean is.
 * The drift is the same as the writer-on-writer case: small in magnitude, no
 * exceptions, no corruption. For a strictly atomic snapshot use [Strict]; the
 * per-stat lock will serialise readers behind the writer.
 *
 * @sample com.eignex.kumulant.samples.perStatConcurrency
 */
enum class Concurrency {

    /**
     * Single-threaded. No atomics, no locks. The default and cheapest path; it is
     * correct when one thread owns the accumulator.
     *
     * A [None] stat hit by two threads at once falls out of unsynchronised reads
     * and writes. Exceptions and state corruption are both possible, so do not
     * share a [None] stat across threads.
     */
    None,

    /**
     * Lock-free atomic cells. Many writers, no blocking.
     *
     * For stats whose state is one independent cell ([com.eignex.kumulant.stat.summary.SumStat],
     * [com.eignex.kumulant.stat.summary.MinStat], [com.eignex.kumulant.stat.summary.MaxStat],
     * the Bernoulli sum, the counter) [Relaxed] is exact because increments and
     * CAS-min/max commute.
     *
     * For Welford-coupled stats ([com.eignex.kumulant.stat.summary.MeanStat],
     * [com.eignex.kumulant.stat.summary.VarianceStat],
     * [com.eignex.kumulant.stat.summary.MomentsStat]) the cells race; independent
     * racers reading a stale numerator against an updated denominator can produce
     * results that drift by ULPs up to roughly 1e-5 relative under heavy
     * contention. The drift does not compound and the stat never throws. Pick
     * [Relaxed] on hot paths where a metric that is 0.001 percent off is
     * preferable to writers blocking on a lock.
     */
    Relaxed,

    /**
     * Multi-threaded with whatever synchronisation each stat needs to stay exact
     * across coupled state. For Welford-style stats this is a lock around the body
     * of `update`. For sketches that already self-serialise (DDSketch, t-digest,
     * HDR, and so on) [Strict] is the natural mode and adds no extra overhead.
     *
     * Every writer goes through the same lock for coupled stats. Shard the work
     * and merge when the write rate swamps a single lock.
     */
    Strict,

    /**
     * JVM-only striped adders for additive stats. On a heavily-loaded
     * [com.eignex.kumulant.stat.summary.SumStat] or
     * [com.eignex.kumulant.stat.summary.CountStat] this scales linearly with the
     * writer count.
     *
     * On non-JVM platforms [HighWrite] falls back to [Strict]. For stats where
     * striping makes no sense (anything coupled, anything sketch-based)
     * [HighWrite] also behaves like [Strict].
     */
    HighWrite,
}
