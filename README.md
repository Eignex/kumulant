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

Kumulant is a Kotlin multiplatform library for computing statistics over
data that arrives one observation at a time. You feed values in as you
see them, ask for a result whenever you want one, and the memory it
uses stays the same no matter how long the stream runs. Two instances
of the same statistic can be combined into one, so you can compute in
parallel and stitch the partial results back together.

It covers the usual summaries like mean and variance, quantile sketches
for percentiles, cardinality estimators for counting distinct items,
sketches for set membership and heavy hitters, time-decayed averages,
online regression, and a handful of scoring metrics for evaluating
predictions as they come in. Everything runs on the JVM, in the browser,
in WebAssembly, and on native Linux, macOS, Windows, and iOS.

## Installation

```kotlin
dependencies {
    implementation("com.eignex:kumulant:0.1.0")
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

val ols = UnivariateRegressionStat()
for ((x, y) in pairs) ols.update(x, y)
val fit = ols.read()
val yHat = fit.slope * 7.0 + fit.intercept
```

| Family       | Stats                                                                          |
|--------------|--------------------------------------------------------------------------------|
| Summary      | Sum, Mean, Min, Max, Range, Variance, Moments, BernoulliSum, Count             |
| Quantile     | DDSketch, TDigest, HdrHistogram, LinearHistogram, ReservoirHistogram, FrugalQuantile |
| Cardinality  | HyperLogLog, LinearCounting                                                    |
| Sketch       | BloomFilter, CountMinSketch, MinHash, SpaceSaving                              |
| Rate         | Rate, CounterRate, DecayingRate                                                |
| Regression   | UnivariateRegression (OLS / L1 / L2), Covariance, SGD, Diagonal, Bayesian      |
| Decay        | DecayingSum, DecayingMean, DecayingVariance, EwmaMean, EwmaVariance            |
| Score        | MseLoss, MaeLoss, LogLoss, PinballLoss, BrierScore, Auc, Reliability, PitHistogram |

## Composing stats

You can wrap a stat to change how it sees its input. Time-windowing,
weighting, filtering, and pre-update transforms all stack on top of
any stat.

```kotlin
val recentMean = MeanStat().windowed(1.minutes, slices = 10)
val positiveMean = Mean.filter(X gt 0.0).materialize()
```

## Sending stats over the wire

You can also describe a whole collection of stats as data, ship that
description to another process, and start sending partial results
across. The receiver rebuilds the same shape of accumulator and merges
the snapshots in as they arrive.

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
semantics as any other stat.

```kotlin
val bandit = MultiArmedBandit(nbrArms = 4, policy = BetaBernoulliTS())
val arm = bandit.choose()
bandit.update(arm, value = 1.0)
```

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
under write-heavy load.

```kotlin
val hits = SumStat(concurrency = Concurrency.HighWrite)
val ols = UnivariateRegressionStat(concurrency = Concurrency.Strict)
```
