# Package com.eignex.kumulant.stat.summary

Running aggregates over scalar streams, from exact sums and moments to
approximate robust dispersion estimates. Memory is bounded in the stream
length; update and read costs depend on the stat. Each entry supports
merging snapshots from parallel workers.

This is the largest stat family and the one most other packages compose
against; change detectors run over a [MeanStat] / [VarianceStat] internally,
calibration runs over outcome means via [MeanStat], regression carries
covariance through [WeightedVarianceResult]-shaped cells, and so on.

## The trivial entries

[SumStat], [MinStat], and [MaxStat] each keep one Double of state with
O(1) updates. Concurrent modes use additive cells for [SumStat] and CAS
loops for [MinStat] / [MaxStat]. [RangeStat] tracks min and max in two
independent cells, so concurrent reads need not observe both from the
same update. Floating-point sums depend on update order.

[ArgMinStat] and [ArgMaxStat] extend [MinStat] / [MaxStat] with the
timestamp at which the extreme was observed. An internal lock keeps the
coupled (value, timestamp) pair consistent under concurrency. Merge keeps the pair from
the smaller (larger) value.

[CountStat] and [BernoulliSumStat] look similar but answer different
questions. [CountStat] counts updates, ignoring weight. [BernoulliSumStat]
tracks weighted successes (`sum(weight * value)`) and trials (`sum(weight)`),
supporting both binary outcomes and soft probabilities. Summed weight alone is
`Sum.withValue(1.0)`, and every result with an observation count reports it as
`totalWeights`. Both are
additive and gain a striped-adder JVM path under
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
without a shared lock; coupled state can drift under contention with
no numerical error bound. Floating-point results depend on update order.

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

[MadStat] retains a single raw-value t-digest. At read time it computes the
weighted median of centroid locations and their absolute deviations from that
same center. Both estimates approximate the original observations because
compression loses within-centroid spread. [medianAbsoluteDeviation] also
projects an existing [com.eignex.kumulant.stat.quantile.TDigestResult] directly,
without requiring precomputed quantiles.

[FrugalMadStat] keeps two adaptive estimates and an initialization flag. Its
constant step size trades responsiveness against jitter; positive weights each
trigger one step. It estimates recent center and spread, with no historical
accuracy guarantee. Merge adopts a remote pair while empty and otherwise
averages the two estimates equally.

Both return [MadResult], which implements
[com.eignex.kumulant.core.HasCenterScale] with `center = median` and
`scale = mad`. MAD is unscaled: it is not a Gaussian standard deviation.
[MadResult.digest] retains merge evidence for digest-backed results; frugal and
manually constructed summary-only results carry no digest.

## Paired

[PairedSumStat] is the analogue of [SumStat] for paired streams: tracks
per-axis sums of `(x, y)` updates. Not a regression; covariance and
correlation live in [com.eignex.kumulant.stat.regression.CovarianceStat].

## Compose patterns

A few recurring patterns built from this family rather than as dedicated
stats:

- **Fraction meeting a threshold**: for SLO compliance and error
  budgets. Compose
  [Mean][com.eignex.kumulant.schema.spec.Mean]`.transform(ifThenElse(X gt threshold, 1.0, 0.0)).windowed(window)`.
  Mean over the Bernoulli predicate is exactly the matched fraction.
- **Lag-k autocorrelation**;
  [Covariance][com.eignex.kumulant.schema.spec.Covariance]`.withSelfLag(k)`
  self-pairs each input with the value seen `k` updates ago; the Pearson
  correlation falls out of the running covariance.
- **Standardised input**: `.standardScaler()` z-scores each input against a
  hidden [VarianceStat]. For another center/scale, use `.withFeedback` with a
  [MomentsStat] / [MadStat] / [SummaryStat] primary; the projection reads
  `center` and `scale` off its snapshot on every update.

## Batch mean estimates

[estimateMean] accepts values, relative importance weights, and optional known observation
variances. [MeanEstimate] separates response dispersion from sampling variance of a weighted
mean, using Kish effective sample size rather than treating importance weights as counts.
[pairedMeanDifference] preserves pairing when estimating a mean difference. A standard-error
multiple is descriptive; repeated comparisons do not turn it into an anytime-valid confidence
bound. One unknown-noise observation cannot estimate a standard error.

[estimateCorrelatedMean] estimates a common location from a supplied positive-definite covariance.
It uses generalized least squares, including correlations and observation noise. Its reported mean
variance is conditional on that covariance; it does not include covariance-estimation uncertainty.

## Merge

The additive stats ([SumStat],
[CountStat], [BernoulliSumStat]) sum cell-wise; [MinStat],
[MaxStat], and [RangeStat] take cell-wise min/max; [ArgMinStat] and
[ArgMaxStat] keep the (value, timestamp) pair from the smaller (larger)
value; the Welford family
([MeanStat], [VarianceStat], [MomentsStat], [SummaryStat]) uses the Chan-style
parallel recurrence; [PairedSumStat] sums each axis. All of these are exact.
[MadStat] merges the complete digest in [MadResult.digest] and recomputes the
median and MAD; the result inherits t-digest approximation. A summary-only
[MadResult] contributes unit weight split between `median - mad` and
`median + mad`, approximating the missing evidence. [FrugalMadStat] adopts
a remote pair while empty and otherwise averages both estimates equally.

## Concurrency

| Stat | Memory | Update | Concurrency |
|------|--------|--------|-------------|
| [SumStat], [CountStat], [BernoulliSumStat] | O(1) | O(1) | JVM striped adders under HighWrite; atomic under other concurrent modes |
| [MinStat], [MaxStat], [RangeStat] | O(1) | O(1) | per-cell CAS; Range reads min and max independently |
| [ArgMinStat], [ArgMaxStat] | O(1) | O(1) | internal lock keeps the (value, timestamp) pair consistent |
| [MeanStat] | O(1) | O(1) | Welford-coupled; locked under Strict / HighWrite, racing under Relaxed |
| [VarianceStat], [SummaryStat] | O(1) | O(1) | Welford-coupled |
| [MomentsStat] | O(1) | O(1) | Welford-coupled (five cells) |
| [MadStat] | O(t-digest compression) | Buffered t-digest update; O(k) read for k centroids | inherits t-digest snapshot consistency |
| [FrugalMadStat] | O(1) | O(1) | coupled estimates locked under every concurrent level |
| [PairedSumStat] | O(1) | O(1) | two independent additive cells |
