# Stats

Stat is the root interface for everything that accumulates. The shape of
the input determines which sub-interface a stat implements; the shape of
the output is the Result it produces.

## The four modalities

All four extend Stat and share read, merge, reset, create, and
concurrency. They differ only in the signature of update:

| Interface       | update signature                                              | Typical use                            |
| --------------- | ------------------------------------------------------------- | -------------------------------------- |
| SeriesStat      | update(value: Double, weight: Double = 1.0)                   | One scalar per observation             |
| DiscreteStat    | update(value: Long, weight: Double = 1.0)                     | Opaque keys, integer counts            |
| PairedStat      | update(x: Double, y: Double, weight: Double = 1.0)            | Scalar (x, y) pairs                    |
| VectorStat      | update(vector: VectorView, weight: Double = 1.0)              | Multi-channel observations             |
| RegressionStat  | update(x: VectorView, y: Double, weight: Double = 1.0)        | Vector covariate, scalar response      |

Every update overload has a sibling that takes an explicit
timestampNanos; the no-timestamp form calls currentTimeNanos. Stats that
do not care about time ignore it; stats that do (rates, windowed
wrappers, decaying accumulators) use it as the ordering signal, so pass
a monotonic stamp when feeding from a queue or replaying a log.

VectorStat and RegressionStat both accept a VectorView so sparse callers
can feed sparse vectors without materialising them. Each interface also
exposes a DoubleArray convenience overload (timestamped and untimestamped)
as a default method that wraps the array in a DenseVector.

## Results

A Result is an immutable snapshot. Every concrete result is a serializable
data class, so the same value that comes out of read goes into merge over
the wire.

Cross-cutting traits live in core.StatTraits and surface on multiple
families. HasRate carries a normalised throughput and exposes rate
(events per second) plus per(duration). HasSampleVariance exposes
totalWeights, variance, stdDev, sampleVariance, and sampleStdDev.
HasShapeMoments extends HasSampleVariance with m3, m4, skewness,
kurtosis, and the size-adjusted unbiased variants. HasLinearModel
exposes weights, bias, and a predict(VectorView) method over a fitted
hyperplane. HasSlope is the univariate special case with slope,
intercept, and a scalar predict(Double); it implements HasLinearModel
for free. HasRegression exposes sse, ssr, mse, rmse, and rSquared on
top of HasSampleVariance.

A consumer written against a trait works for every concrete result that
implements it; that is how one downstream pipeline handles both a
univariate fit and a multivariate one.

## Family catalog

This is a tour, not a reference. Each individual stat carries its own
KDoc covering the exact memory, update, and concurrency stories.

### Summary

The summary family covers exact running aggregates whose memory is
constant in the stream length. SumStat, MinStat, MaxStat, and RangeStat
(min plus max) are the trivial ones: each holds a single double and
updates it in O(1).

MeanStat is the simplest non-trivial entry. A naive sum-divided-by-count
loses precision as the running sum grows; MeanStat uses Welford's
recurrence to keep the running mean stable for streams that span many
orders of magnitude.

VarianceStat and MomentsStat extend the Welford idea. VarianceStat
tracks mean and the sum of squared deviations, producing variance,
stdDev, sampleVariance, and sampleStdDev. MomentsStat additionally
tracks the third and fourth central moments and exposes skewness and
kurtosis (with size-adjusted unbiased variants). Use MomentsStat when
the shape of the distribution matters, for example to detect heavy
tails or asymmetry in latency or reward.

CountStat, TotalWeightsStat, and BernoulliSumStat look similar at first
glance but answer different questions. CountStat counts updates,
ignoring weight. TotalWeightsStat sums weight, ignoring the value.
BernoulliSumStat sums weight only when the value is nonzero, which is
the natural counter for binary outcomes such as click-or-not or
pass-or-fail.

### Quantile

The quantile family answers "what value sits at the p-th percentile?"
in bounded memory. Each entry trades a different precision-versus-cost
knob against the others.

DDSketchStat is the default choice for percentile metrics whose values
span orders of magnitude (latencies, payload sizes, error magnitudes).
Bins are log-spaced, the precision knob is a single relative-error
guarantee, and merging across replicas is exact.

TDigestStat uses adaptive centroid clusters concentrated near the tails.
It typically gives tighter tail-quantile error than DDSketch at the same
memory budget, which makes it the better pick when you specifically care
about the 99th or 99.9th percentile and not the body of the
distribution.

HdrHistogramStat is a fixed-precision histogram over a bounded range.
Pick it when you know the range up front (for example an SLO with
latencies between one microsecond and an hour) and want the strictest
precision guarantees in that range.

LinearHistogramStat uses caller-supplied uniform bins. Reach for it
when you already know the meaningful breakpoints and want bins that
match them directly, with no rebucketing on read.

ReservoirHistogramStat keeps a bounded reservoir sample of raw values
rather than a digest. Use it when you need the raw observations back,
either to feed another stat downstream or to compute quantities the
sketches do not expose.

FrugalQuantileStat is a constant-memory single-quantile tracker (two
state variables). Use it when you can fit only a few bytes per stat and
you only care about one percentile.

### Cardinality

The cardinality family answers "how many distinct keys have I seen?".
Memory is bounded; accuracy depends on the cardinality regime.

HyperLogLogStat is the default. Its memory is two raised to the
precision parameter (precision 14 is roughly 16 KB and one percent
relative error). Merge is exact via cell-wise max, which makes HLL the
natural choice when many workers each track a slice of the same
stream.

LinearCountingStat is bitmap-backed. It is tight when the true
cardinality is much smaller than the bitmap, and degrades sharply once
the bitmap saturates. Pick it when the cardinality is bounded and known
to stay well below the bitmap size; otherwise pick HLL.

### Sketch

The sketch family covers structural queries on a stream that are not
exactly cardinality or quantile shaped. Each entry answers a different
question.

BloomFilterStat answers "have I seen this key before?". False positives
are one-sided (a "yes" may be wrong, a "no" never is) and the rate is
controlled by the precision knob. Membership only; there are no per-key
counts.

CountMinSketchStat answers "how many times has this key appeared, at
least?". The estimate is biased upward by collisions and exact for
heavy items. Use it as a per-key counter when an exact map of counts
would blow memory.

SpaceSavingStat tracks the top K heavy hitters by approximate count.
Heavy hitters are the keys that appear far more often than the rest of
the stream, the long-tail-flipped-around problem: out of millions of
distinct items, a handful might account for most of the volume (a few
viral URLs, a few hot database rows, a few users responsible for most
requests). SpaceSaving keeps only K counters and guarantees that any
key whose true frequency exceeds the K-th largest will be in that set,
which is much cheaper than maintaining an exact map. Use it when you
do not need every key's count, only the busiest few.

MinHashStat is a Jaccard similarity sketch. It estimates the overlap
between two sets without comparing them directly, which is the standard
tool for deduplication and near-duplicate detection over streams.

### Rate

The rate family produces a HasRate result, so consumers can pull events
per second (and per any other duration) through one trait.

RateStat divides observation count by the wall-clock span of the
window. Use it for end-to-end throughput where every update represents
one event.

CounterRateStat is the right pick when the underlying signal is itself
a monotonically-increasing counter (for example a packet counter, a
byte counter, or a CPU cycle count pulled from another process). It
differentiates the counter to recover an event rate and can be told
whether a decrease means a reset or an actual negative rate.

DecayingRateStat is an exponentially-decayed events-per-second.
Compared with RateStat it tracks recent activity more responsively;
compared with CounterRateStat it weights events by recency rather than
treating them uniformly across the window. Use it when you want a
smooth, responsive rate metric.

### Decay

The decay family covers time-weighted moments. Each observation enters
the accumulator with a weight that shrinks toward zero with age, so
older observations contribute less.

DecayingSumStat, DecayingMeanStat, and DecayingVarianceStat are the
exact time-weighted counterparts of SumStat, MeanStat, and VarianceStat.
The decay schedule lives in DecayWeighting (half-life, time constant,
or custom). These handle irregular update intervals correctly because
weight is a function of timestamp, not step count.

EwmaMeanStat and EwmaVarianceStat are the classical
exponentially-weighted moving variants with a step-based decay
(`alpha * new + (1 - alpha) * old`). They are cheaper and more familiar
but assume roughly fixed intervals between observations; mixed-interval
streams should reach for the timestamp-based decay variants instead.

### Regression

The regression family fits a linear model `y = bias + weights · x`.
Every entry produces a HasLinearModel or HasSlope result so downstream
code can be written against the trait.

UnivariateRegressionStat is the scalar-on-scalar version (slope and
intercept). Penalty selects OLS, ridge, or lasso. Use it for the
canonical "fit a line to a stream of (x, y) points" case.

CovarianceStat carries the paired covariance and Pearson correlation,
which are the building blocks for both UnivariateRegression and the
multivariate stats. Use it directly when correlation is the metric you
want.

BayesianRegressionStat is full Bayesian linear regression with a
covariance matrix over the weights. It supports posterior sampling
(used by Linear Thompson Sampling) and the LinUcb confidence bound.
Reach for it when you need uncertainty in addition to point
predictions, in particular when the regressor feeds a contextual
bandit.

DiagonalRegressionStat is the feature-independent Bayesian variant. It
keeps a per-feature precision without the full covariance matrix, which
makes it the natural choice for high-dimensional features where the
quadratic memory of full Bayesian regression is prohibitive.

All three (Bayesian, Diagonal, and Stochastic below) take a canonical
GLM Link. Link.Identity is plain linear regression; Link.Logit is
logistic regression on binary outcomes; Link.Log is Poisson-style
regression on counts. Under Identity the Bayesian variant is the
strict closed-form conjugate Gaussian posterior; under Logit and Log
it is an online Laplace approximation around the running mean.

StochasticRegressionStat is online SGD with a configurable Link
function and a configurable LearningRates schedule. Use it for large
feature spaces, sparse vectors, or settings where exact closed-form
updates are too expensive.

HierarchicalBayesianRegression fits a population prior across many
per-group regressors. Use it when you have many parallel regressions
that share structure, such as one regressor per arm in a bandit that
should benefit from cross-arm pooling.

### Tree

The tree family is the non-linear counterpart of the regression family.

DecisionTreeRegressionStat is a single online regression tree. Splits
are evaluated against a SplitMetric over a candidate pool defined by
TreeConfig. Use it when one tree captures the structure you need or as
a debugging aid for the forest.

RandomForestRegressionStat is the bagged ensemble. Each tree updates
independently from a Poisson-bagged copy of the stream, which gives the
forest its variance estimate. Reach for it when you need uncertainty
across trees (the natural backbone for a tree-based contextual bandit
via ForestPosteriors) or when single-tree predictions vary too much.

### Score

The score family is online evaluation metrics. They consume `(observed,
predicted)` pairs (or richer shapes for distributional metrics) and
report a calibration or accuracy summary.

MseLoss and MaeLoss are the standard regression errors. Use MseLoss
when large errors matter quadratically, MaeLoss when you want the
median-error-style robust alternative.

LogLoss is the proper scoring rule for predicted probabilities on
binary outcomes. BrierScore is the squared-error counterpart that
weights confident wrong predictions less harshly than LogLoss. Use
LogLoss for log-likelihood-shaped objectives and BrierScore when you
want bounded, reliability-decomposable error.

PinballLoss scores quantile predictions and is the right pick when the
model emits a quantile rather than a mean.

AucStat reports ROC AUC, which measures discrimination (whether
positives score higher than negatives) and ignores calibration.

ReliabilityStat builds the calibration diagram (binned predicted
probability versus observed frequency) and is the tool for diagnosing
whether predicted probabilities are trustworthy at face value.

PitHistogram and PitTests cover the probability integral transform.
Feed a forecast CDF and an observed value and the histogram tracks PIT
uniformity; PitTests runs the standard uniformity tests on the
histogram. Use them to diagnose distributional forecasts (does the
model under-cover, over-cover, miss the tails?).

## Building a stat

The cheapest construction is the no-arg form:

```kotlin
val mean = MeanStat()
val sum = SumStat()
val variance = VarianceStat()
```

Pass concurrency to opt into a thread-safety mode. For stats with
required configuration, the constructor takes them positionally or by
name:

```kotlin
val sketch = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5, 0.99))
val hll = HyperLogLogStat(precision = 14)
val bayes = BayesianRegressionStat(featureSize = 8)
```

When you want a bag of stats sharing one concurrency contract and one
wire format, declare a StatSchema instead; see
[Schemas and the wire](05-schemas.md).
