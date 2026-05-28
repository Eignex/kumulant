# Package com.eignex.kumulant.stat.summary

Exact running aggregates over a stream of scalars. Memory is constant in
the stream length; updates are O(1); every entry merges cleanly across
parallel workers via Chan-style parallel formulas or commuting cell
arithmetic.

This is the largest stat family and the one most other packages compose
against; change detectors run over a [MeanStat] / [VarianceStat] internally,
calibration runs over outcome means via [MeanStat], regression carries
covariance through [WeightedVarianceResult]-shaped cells, and so on.

## The trivial entries

[SumStat], [MinStat], [MaxStat], and [RangeStat] are single-cell stats:
one Double of state, O(1) update, exact under every [com.eignex.kumulant.core.Concurrency]
level via independent commuting cell arithmetic ([SumStat]) or a CAS
loop on a single cell ([MinStat] / [MaxStat]). [RangeStat] is a
[MinStat] and [MaxStat] tracked together.

[CountStat], [TotalWeightsStat], and [BernoulliSumStat] look similar but
answer different questions. [CountStat] counts updates, ignoring weight.
[TotalWeightsStat] sums weight, ignoring the value. [BernoulliSumStat]
sums weight only when the value is nonzero, which is the natural counter
for binary outcomes (click-or-not, pass-or-fail). All three are
naively additive and gain a striped-adder JVM path under
[com.eignex.kumulant.core.Concurrency.HighWrite].

## The Welford family

[MeanStat], [VarianceStat], and [MomentsStat] use Welford's recurrence
to keep mean / variance / higher moments numerically stable for streams
that span many orders of magnitude. A naive sum-divided-by-count loses
precision as the running sum grows; Welford updates the running mean in
place and accumulates only mean-centred deviations.

The cells are *coupled*: an updated count needs its matching updated
mean to stay consistent. Under [com.eignex.kumulant.core.Concurrency.Strict]
or [com.eignex.kumulant.core.Concurrency.HighWrite] the body is locked.
Under [com.eignex.kumulant.core.Concurrency.Relaxed] the cells race
without a lock; readers and writers may drift by ULPs of the workload
under contention but the stat never throws.

| Stat | Result | Tracks |
|------|--------|--------|
| [MeanStat] | [WeightedMeanResult] | running mean, total weight |
| [VarianceStat] | [WeightedVarianceResult] | mean + variance |
| [MomentsStat] | [MomentsResult] | mean + variance + skewness + kurtosis (third and fourth central moments) |
| [SummaryStat] | [SummaryResult] | mean + variance + min + max in one accumulator; useful as a primary for mixed-scaler feedback projections |

[WeightedVarianceResult] implements both
[com.eignex.kumulant.core.HasSampleVariance] and
[com.eignex.kumulant.core.HasCenterScale]; [SummaryResult] implements
both of those plus [com.eignex.kumulant.core.HasMinMax], which is what
makes it valuable as a feedback primary; one accumulator covers both
the standardize and min-max projections downstream.

## Robust dispersion

[MadStat] tracks the running median and median absolute deviation via
two t-digests: one over raw values and one over absolute deviations from
the running median. Reach for it as the robust analog of standard
deviation for heavy-tailed inputs where the mean and variance overstate
central tendency and spread. The result implements
[com.eignex.kumulant.core.HasCenterScale] (with `center = median`,
`scale = mad`), so the standardize / band projections work on it
directly.

## Paired

[PairedSumStat] is the analogue of [SumStat] for paired streams: tracks
per-axis sums of `(x, y)` updates. Not a regression; covariance and
correlation live in [com.eignex.kumulant.stat.regression.CovarianceStat].

## Composing patterns

A few recurring patterns built from this family rather than as dedicated
stats:

- **Fraction meeting a threshold**: for SLO compliance and error
  budgets. Compose
  [Mean][com.eignex.kumulant.schema.Mean]`.transform(IfExpr(X gt threshold, 1.0, 0.0)).windowed(window)`.
  Mean over the Bernoulli predicate is exactly the matched fraction.
- **Lag-k autocorrelation**;
  [Covariance][com.eignex.kumulant.schema.Covariance]`.withSelfLag(k)`
  self-pairs each input with the value seen `k` updates ago; the Pearson
  correlation falls out of the running covariance.
- **Standardised input**: feed any series stat through the
  [StandardScalerSeries][com.eignex.kumulant.schema.StandardScalerSeries]
  spec or its modality siblings; the scaler reads `center` and `scale`
  off a [VarianceStat] / [MomentsStat] / [MadStat] / [SummaryStat]
  primary on every update.

## Memory and concurrency at a glance

| Stat | Memory | Update | Concurrency |
|------|--------|--------|-------------|
| [SumStat], [CountStat], [TotalWeightsStat], [BernoulliSumStat] | O(1) | O(1) | striped-additive under HighWrite, atomic otherwise |
| [MinStat], [MaxStat], [RangeStat] | O(1) | O(1) | CAS loop on a single cell |
| [MeanStat] | O(1) | O(1) | Welford-coupled; locked under Strict / HighWrite, racing under Relaxed |
| [VarianceStat], [SummaryStat] | O(1) | O(1) | Welford-coupled |
| [MomentsStat] | O(1) | O(1) | Welford-coupled (four cells) |
| [MadStat] | O(t-digest compression) | O(log compression) | t-digest self-serialises |
| [PairedSumStat] | O(1) | O(1) | two independent additive cells |
