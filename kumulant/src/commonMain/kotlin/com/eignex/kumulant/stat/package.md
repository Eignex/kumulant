# Package com.eignex.kumulant.stat

Concrete accumulators grouped by family. Each family lives in its own
subpackage; this page is the navigation index.

## The four modalities

Every stat extends [com.eignex.kumulant.core.Stat] and shares
`read` / `merge` / `reset` / `create` / `concurrency`. They differ only
in the signature of `update`:

| Interface | update signature | Typical use |
|-----------|------------------|-------------|
| [com.eignex.kumulant.core.SeriesStat] | `update(value: Double, weight: Double = 1.0)` | One scalar per observation |
| [com.eignex.kumulant.core.DiscreteStat] | `update(value: Long, weight: Double = 1.0)` | Opaque keys, integer counts |
| [com.eignex.kumulant.core.PairedStat] | `update(x: Double, y: Double, weight: Double = 1.0)` | Scalar `(x, y)` pairs |
| [com.eignex.kumulant.core.VectorStat] | `update(vector: VectorView, weight: Double = 1.0)` | Multi-channel observations |
| [com.eignex.kumulant.core.RegressionStat] | `update(x: VectorView, y: Double, weight: Double = 1.0)` | Vector covariate, scalar response |

Every `update` overload has a sibling that takes an explicit
`timestampNanos`; the no-timestamp form calls `currentTimeNanos`. Stats
that ignore time silently drop the stamp; stats that care about it
(rates, windowed wrappers, decaying accumulators) treat it as the
ordering signal — pass a monotonic stamp when replaying a log.

`VectorStat` and `RegressionStat` both accept a `VectorView` so sparse
callers can feed sparse vectors without materialising them. Each
interface also exposes a `DoubleArray` convenience overload that wraps
the array in a `DenseVector`.

## Cross-cutting result traits

A [com.eignex.kumulant.core.Result] is an immutable snapshot. Every
concrete result is a `@Serializable` data class, so the same value that
comes out of `read()` goes into `merge()` over the wire. Traits in
`core.StatTraits` surface on multiple families:

- [com.eignex.kumulant.core.HasRate] — normalised throughput; `rate`
  (events per second) plus `per(duration)`.
- [com.eignex.kumulant.core.HasSampleVariance] — `totalWeights`,
  `variance`, `stdDev`, `sampleVariance`, `sampleStdDev`.
- [com.eignex.kumulant.core.HasShapeMoments] — extends
  `HasSampleVariance` with `m3`, `m4`, `skewness`, `kurtosis`, and the
  size-adjusted unbiased variants.
- [com.eignex.kumulant.core.HasLinearModel] — `weights`, `bias`, and
  `predict(VectorView)` over a fitted hyperplane.
- [com.eignex.kumulant.core.HasSlope] — univariate special case with
  `slope`, `intercept`, scalar `predict(Double)`; implements
  `HasLinearModel` for free.
- [com.eignex.kumulant.core.HasRegression] — `sse`, `ssr`, `mse`,
  `rmse`, `rSquared` on top of `HasSampleVariance`.
- [com.eignex.kumulant.core.HasCenterScale] / [com.eignex.kumulant.core.HasMinMax]
  — projection-friendly access to running centre/scale and running
  min/max, consumed by the standardize / min-max feedback operators.

A consumer written against a trait works for every concrete result that
implements it; that is how one downstream pipeline handles both a
univariate fit and a multivariate one.

## Family map

| Subpackage | Family |
|-----------|--------|
| [com.eignex.kumulant.stat.summary] | Mean / variance / moments / running sum / count / min / max / range / MAD |
| [com.eignex.kumulant.stat.event] | Excursion / run length / level crossings / recency / sojourn |
| [com.eignex.kumulant.stat.rate] | Rate / counter rate / decaying rate |
| [com.eignex.kumulant.stat.change] | CUSUM / Page-Hinkley / ADWIN drift detectors |
| [com.eignex.kumulant.stat.quantile] | DDSketch / t-digest / HDR / reservoir / frugal / linear histograms |
| [com.eignex.kumulant.stat.cardinality] | HyperLogLog / linear counting |
| [com.eignex.kumulant.stat.sketch] | Bloom / count-min / space-saving / MinHash |
| [com.eignex.kumulant.stat.decay] | Decaying / EWMA mean / variance / sum |
| [com.eignex.kumulant.stat.forecast] | Holt / Holt-Winters / recursive variance |
| [com.eignex.kumulant.stat.regression] | Covariance, Softmax, Naive Bayes; see [glm][com.eignex.kumulant.stat.regression.glm] and [tree][com.eignex.kumulant.stat.regression.tree] for the model families |
| [com.eignex.kumulant.stat.regression.glm] | GLM-family linear models with [Link][com.eignex.kumulant.stat.regression.glm.Link] and [Penalty][com.eignex.kumulant.stat.regression.glm.Penalty] (univariate, stochastic, diagonal, Bayesian, hierarchical) |
| [com.eignex.kumulant.stat.regression.tree] | Online VFDT regression and classification trees + random forests |
| [com.eignex.kumulant.stat.score] | Evaluation metrics: MSE / MAE / log loss / Brier / pinball / AUC / confusion matrix / accuracy |
| [com.eignex.kumulant.stat.calibration] | Probability calibration: reliability diagnostic, Platt scaling, isotonic regression |
| [com.eignex.kumulant.stat.anomaly] | Gaussian z-score, quantile-filter, half-space trees |

Each individual stat carries its own KDoc covering exact memory, update
cost, and concurrency story. The canonical shape is
[com.eignex.kumulant.stat.summary.MeanStat]; replicate it when adding
new stats.

## Building a stat

The cheapest construction is the no-arg form. Pass `concurrency` to opt
into a thread-safety mode; required configuration is positional or named.

```kotlin
val hits = SumStat(concurrency = Concurrency.HighWrite)
val ols = UnivariateRegressionStat(concurrency = Concurrency.Strict)
```

When you want a bag of stats sharing one concurrency contract and one
wire format, declare a schema instead — see
[com.eignex.kumulant.schema].
