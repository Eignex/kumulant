<p align="center">
  <a href="https://eignex.com/">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/Eignex/.github/refs/heads/main/profile/banner-white.svg">
      <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/Eignex/.github/refs/heads/main/profile/banner.svg">
      <img alt="Eignex" src="https://raw.githubusercontent.com/Eignex/.github/refs/heads/main/profile/banner.svg" style="max-width: 100%; width: 22em;">
    </picture>
  </a>
</p>

# Kumulant

[![Maven Central](https://img.shields.io/maven-central/v/com.eignex/kumulant.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/com.eignex/kumulant)
[![Build](https://github.com/eignex/kumulant/actions/workflows/build.yml/badge.svg)](https://github.com/eignex/kumulant/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/eignex/kumulant/branch/main/graph/badge.svg)](https://codecov.io/gh/eignex/kumulant)
[![License](https://img.shields.io/github/license/eignex/kumulant)](https://github.com/eignex/kumulant/blob/main/LICENSE)

Kumulant is a Kotlin multiplatform library for streaming machine
learning. Every model runs online: feed observations as they arrive,
ask for a result any time, and the memory footprint stays bounded no
matter how long the stream runs. Two instances of the same accumulator
combine into one, so partial results from independent shards merge
back without losing fidelity.

It covers online regression and classification (OLS, logistic, softmax,
naive Bayes, decision trees, random forests), multi-armed and
contextual bandits, anomaly scoring, post-hoc calibration, and the
streaming-statistics surface underneath: running summaries, quantile
sketches, cardinality estimators, heavy-hitter sketches, time-decayed
averages, change detection, and a scoring-metric family for evaluating
predictions as they come in. Everything runs on the JVM, in the
browser, in WebAssembly, and on native Linux, macOS, Windows, and iOS.

The [API site](https://eignex.com/docs/kumulant) is the canonical
reference.

## Installation

```kotlin
dependencies {
    implementation("com.eignex:kumulant:0.3.0")
}
```

## A quick taste

```kotlin
val mean = MeanStat()
for (x in stream) mean.update(x)
println(mean.read().mean)

val sketch = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5, 0.99))
for (x in stream) sketch.update(x)
val r = sketch.read() // r.probabilities and r.quantiles are parallel arrays

val cov = CovarianceStat()
for ((x, y) in pairs) cov.update(x, y)
val r = cov.read() // r.covariance, r.correlation

val classifier = BayesianRegressionStat(featureSize = 8, link = Link.Logit)
for ((x, label) in labeled) classifier.update(x, label)
val pHat = classifier.read().predict(features) // sigmoid(eta) under Link.Logit

val platt = PlattCalibratorStat()
for ((x, label) in labeled) platt.update(classifier.read().predict(x), label)
val calibrated = platt.read().calibrate(pHat)

val anomaly = HalfSpaceTreesStat(featureSize = 4, featureRanges = ranges)
for (x in vectorStream) anomaly.update(x)
val anomalyScore = anomaly.read().score(point)
```

Stats group into families under the `stat.*` subpackages. Each family
page on the Dokka site lists the entries and the notes on when to pick
which.

| Family       | Stats                                                                          |
|--------------|--------------------------------------------------------------------------------|
| Summary      | Sum, Mean, Min, Max, ArgMin, ArgMax, Range, Variance, Moments, Summary, BernoulliSum, Count, Mad |
| Event        | Excursion, RunLength, Crossing, Recency, Sojourn                                |
| Rate         | Rate, CounterRate, DecayingRate                                                 |
| Change       | Cusum, PageHinkley, Adwin                                                       |
| Quantile     | DDSketch, TDigest, HdrHistogram, LinearHistogram, ReservoirHistogram, FrugalQuantile, ThresholdBucket |
| Cardinality  | HyperLogLog, LinearCounting                                                    |
| Sketch       | BloomFilter, CountMinSketch, MinHash, SpaceSaving                              |
| Regression   | UnivariateRegression (OLS / L1 / L2), Covariance, Softmax, GaussianNaiveBayes; GLMs (Stochastic, Diagonal, Bayesian, Hierarchical); trees (DecisionTree, RandomForest, classifier variants) |
| Decay        | DecayingSum, DecayingMean, DecayingVariance, EwmaMean, EwmaVariance             |
| Forecast     | Holt, SeasonalSmoothing, RecursiveVariance                                      |
| Score        | MseLoss, MaeLoss, LogLoss, PinballLoss, BrierScore, Auc, Accuracy, ConfusionMatrix, PitHistogram |
| Calibration  | Reliability, PlattCalibrator, IsotonicCalibrator                                |
| Anomaly      | GaussianScorer, QuantileFilter, HalfSpaceTrees                                  |

## Composing stats

You can wrap a stat to change how it sees its input. Time-windowing,
weighting, filtering, and pre-update transforms all stack on top of
any stat. See the `operation` package overview on the Dokka site for
the full adapter surface.

```kotlin
val recentMean = MeanStat().windowed(1.minutes, slices = 10)
val positiveMean = Mean.filter(X gt 0.0).materialize()
```

## Sending stats over the wire

You can also describe a whole collection of stats as data, ship that
description to another process, and start sending partial results
across. The receiver rebuilds the same shape of accumulator and merges
the snapshots in as they arrive. See the
`schema` package overview on the Dokka site for the wire-portable spec
family.

```kotlin
object Telemetry : StatSchema(concurrency = Concurrency.Strict) {
    val latencyMean by series(Mean)
    val latencyP99 by series(DDSketch(probabilities = listOf(0.99)))
    val errorRate by series(Rate)
    val uniqueUsers by discrete(HyperLogLog(precision = 14))
}

val group = StatGroup(Telemetry)
group.update(value = 12.7)
val p99 = group.read()[Telemetry.latencyP99]
```

## Bandits

Bandits build on per-arm stats: each arm owns a kumulant accumulator
and the bandit picks arms by scoring their snapshots. Per-arm state
inherits the same concurrency modes, wire-portable snapshots, and merge
semantics as any other stat. The `bandit` package overview on the
Dokka site walks through the hierarchy, the univariate and contextual
families, policies, and arms.

```kotlin
val bandit = MultiArmedBandit(nbrArms = 4, policy = BetaBernoulliTS())
val arm = bandit.choose()
bandit.update(arm, value = 1.0)
```

| Family       | Bandits                                                                                  |
|--------------|-------------------------------------------------------------------------------------------|
| Univariate   | MultiArmedBandit, RouletteWheelBandit, BoltzmannBandit, Exp3Bandit, TopTwoThompsonBandit  |
| Contextual   | RegressionContextualBandit, KnnContextualBandit, Exp4Bandit                                |
| Policies     | UCB1, UCB1-Normal, UCB1-Tuned, KL-UCB, MOSS, UCB-V, Thompson sampling, Greedy, EpsilonGreedy, EpsilonDecreasing, UniformSelection |

For context-aware decisions, the contextual bandit wraps one regression
stat per arm and scores each arm under the round's feature vector.

```kotlin
val cb = RegressionContextualBandit(
    nbrArms = 4,
    template = BayesianRegressionStat(featureSize = 8),
    posterior = MultivariateGaussian,
)
val a = cb.choose(features)
cb.update(a, features, reward = 12.7)
```

The bandit hierarchy splits action and state into orthogonal interfaces.
UnivariateBandit and ContextualBandit carry the choose / update surface;
PerArmBandit and Snapshotable carry the snapshot/merge/replicate
surface; Scorable and ContextualScorable are opt-in for bandits whose
choose is an argmax over independent per-arm scores. Bandits that
select arms via joint sampling (Top-Two Thompson, Boltzmann) or that
don't fit a per-arm state shape (Exp4) slot in cleanly without bending
the contract.

```kotlin
// Whole-bandit configurations round-trip on the wire alongside their policies.
val spec: UnivariateBanditSpec = MultiArmedSpec(
    nbrArms = 4,
    policy = Ucb1Spec(alpha = 1.5),
)
val live: Bandit = spec.materialize(Random(0))
```

Composite arms model multi-component rewards like zero-inflated lognormal
revenue without writing a class per shape; routing and score combination
travel as the same expression ASTs the rest of the library uses, so the
whole composite round-trips over the wire. Continuous pooling on contextual
bandits and a hierarchical Bayesian manager cover the cold-start story
when arms join an in-progress run.

## Concurrency

Each stat picks a concurrency mode at construction. The default is
single-threaded and the cheapest. Relaxed mode lets many threads
update the same accumulator without any locks, using atomic operations
on every cell. Coupled-state stats may drift by a tiny amount under
heavy contention but never throw or corrupt their state, which makes
it a good fit for hot paths where a strict lock would dominate the
cost. Strict mode adds the locking needed to keep coupled state exact,
and HighWrite swaps in striped adders on the JVM for additive stats
under write-heavy load. The `Concurrency` enum's KDoc on the Dokka
site covers the per-stat semantics in more depth.

```kotlin
val hits = SumStat(concurrency = Concurrency.HighWrite)
val ols = UnivariateRegressionStat(concurrency = Concurrency.Strict)
```

Every stat is exercised under every concurrency mode in the
[kumulant-bench](kumulant-bench) module, which runs accuracy, drift,
and throughput benches across the full catalogue so the lock-free
claims travel with the code.
