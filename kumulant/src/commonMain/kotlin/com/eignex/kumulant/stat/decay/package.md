# Package com.eignex.kumulant.stat.decay

Exponentially weighted sums and moments over a stream. Each member implements
[com.eignex.kumulant.core.SeriesStat]. Timestamp-based stats discount observations
by elapsed time; EWMA stats discount history by cumulative observation weight.

## Picking an estimator

| Stat | Clock | Reach for it when |
|------|-------|-------------------|
| [DecayingSumStat] | Event timestamps | Accumulate a total whose contributions fade with age, or read a time-decayed rate. |
| [DecayingMeanStat] | Event timestamps | Estimate a recent weighted mean when observation intervals vary. |
| [DecayingVarianceStat] | Event timestamps | Track recent dispersion with a coupled mean and variance. |
| [EwmaMeanStat] | Observation weight | Smooth a signal according to how much evidence arrives, independent of elapsed time. |
| [EwmaVarianceStat] | Observation weight | Track dispersion using the same weight-based smoothing clock. |

Both families estimate the same exponentially weighted moments and differ only in the clock. With
unit weights, a bias-corrected EWMA equals a ratio of decayed sums on an observation-count clock.
They stay separate classes because the implementations do not share a cost: the timestamp stats
carry landmark rotation and underflow recovery, while EWMA is a two-cell update cheap enough for
per-event hot paths.

## Timestamp-based decay

[DecayWeighting.HalfLife] takes a `Duration` and sets
`alpha = ln(2) / halfLifeNanos`. A contribution stamped at `t` has weight
`exp(-alpha * (readTime - t))` at a later read. The stats also accept the
half-life directly through their `halfLife: Duration` constructors.

[DecayingSumStat] rotates its numerical landmark after long elapsed intervals.
[DecayingMeanStat] composes two such sums for the weighted numerator and
denominator. [DecayingVarianceStat] decays its weight and second central moment
together before applying a weighted Welford update.

## Weight-based EWMA

[DecayWeighting.Alpha] specifies a rate per unit of observation weight. An
observation of weight `w` uses the smoothing fraction
`a(w) = 1 - exp(-alpha * w)`. With unit weights, history is discounted once per
observation; larger weights advance the smoothing clock further. Reads correct
for the initial zero seed using cumulative weight.

EWMA does not use timestamp spacing. Choose the timestamp-based stats when
elapsed time should determine how quickly evidence fades.

## Result shapes

| Result | Produced by | Fields |
|--------|-------------|--------|
| [DecayingSumResult] | [DecayingSumStat] | Sum, snapshot timestamp, and half-life; derives the decayed `rate`. |
| [DecayingMeanResult] | [DecayingMeanStat] | Mean, decayed total weight, and snapshot timestamp. |
| [DecayingVarianceResult] | [DecayingVarianceStat] | Mean, variance, decayed total weight, and snapshot timestamp; derives `stdDev`. |
| [com.eignex.kumulant.stat.summary.WeightedMeanResult] | [EwmaMeanStat] | Bias-corrected mean and cumulative observation weight. |
| [com.eignex.kumulant.stat.summary.WeightedVarianceResult] | [EwmaVarianceStat] | Bias-corrected mean and variance, plus cumulative observation weight. |

Timestamp-based total weights fade with elapsed time. EWMA total weights count
accepted observation weight and supply the bias correction. The dedicated
timestamp-based results do not implement the summary family's center/scale
traits.

## Merge

All members support merging. With matching half-lives, timestamp-based sums and
moments can align evidence to a common timestamp before combining it. Mean and
variance snapshots retain their effective weights for this combine.

EWMA merges blend already-smoothed summaries using their bias-correction
weights. The result approximates a pooled summary; it does not reproduce a
single chronological replay, and grouping repeated merges can change the result.

Decay schedules are configuration and are absent from these result types.
Matching half-lives or alpha rates is the caller's responsibility.

## Concurrency

[DecayingSumStat] serializes updates, reads, merges, resets, and landmark changes
under every concurrent mode. [DecayingMeanStat] inherits that protection for each
of its two sums, but the pair is updated and read separately: a concurrent reader
can observe a partial numerator/denominator pair. [DecayingVarianceStat] locks its
whole coupled state under every concurrent mode.

[EwmaMeanStat] and [EwmaVarianceStat] lock their coupled state under
[com.eignex.kumulant.core.Concurrency.Strict] and
[com.eignex.kumulant.core.Concurrency.HighWrite].
[com.eignex.kumulant.core.Concurrency.Relaxed] uses independent atomic cells
without a body lock, so concurrent results can drift. EWMA also depends on update
order. [com.eignex.kumulant.core.Concurrency.None] provides no synchronization.
