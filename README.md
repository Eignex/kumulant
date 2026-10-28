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
matter how long the stream runs. Compatible snapshots from independent
shards can be merged into one accumulator. Each stat documents whether
its merge is exact or approximate and which state its snapshots retain.

It covers online regression and classification (OLS, logistic, softmax,
naive Bayes, decision trees, random forests), multi-armed and
contextual bandits, anomaly scoring, post-hoc calibration, and the
streaming-statistics surface underneath: running summaries, quantile
sketches, cardinality estimators, heavy-hitter sketches, time-decayed
averages, change detection, and a scoring-metric family for evaluating
predictions as they come in. Everything runs on the JVM and on native
Linux (x64 and Arm64) and macOS Arm64.

The [API site](https://eignex.com/docs/kumulant) is the canonical
reference.

## Installation

```kotlin
dependencies {
    implementation("com.eignex:kumulant:0.3.3")
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

val variance = VarianceStat()
for (x in stream) variance.update(x)
val anomalyScore = variance.read().absoluteZScore(value)
val tailScore = sketch.read().exceedanceScore(value, probability = 0.99)
```

Stats group into families under the `stat.*` subpackages. Each family
page on the Dokka site lists the entries and the notes on when to pick
which. They include summaries such as mean and variance, quantile
sketches such as DDSketch and t-digest, cardinality estimators such as
HyperLogLog, heavy-hitter sketches, time-decayed averages, change
detection, forecasting, calibration, anomaly scoring, and streaming
prediction metrics.

## Composing stats

You can wrap a stat to change how it sees its input. Time-windowing,
weighting, filtering, and pre-update transforms all stack on top of
compatible stats. Composition happens on the *spec*, then you materialize the
result into a live accumulator. See the `schema.ops` package overview
on the Dokka site for the full adapter surface.

```kotlin
val recentMean = Mean.windowed(durationMillis = 60_000, slices = 10).materialize()
val positiveMean = Mean.filter(X gt 0.0).materialize()
```

Composing on the spec rather than on a live stat is what lets the same
composition travel over the wire, since the spec is data and the
adapters that implement it are an internal detail.

## Sending stats over the wire

You can also describe a whole collection of stats as data, ship that
description to another process, and start sending partial results
across. The receiver rebuilds the same shape of accumulator and merges
the snapshots in as they arrive. See the
`schema` package overview on the Dokka site for the wire-portable spec
family.

```kotlin
object Telemetry : StatSchema() {
    val latencyMean by series(Mean)
    val latencyP99 by series(DDSketch(probabilities = listOf(0.99)))
    val errorRate by series(Rate)
    val uniqueUsers by discrete(HyperLogLog(precision = 14))
}

val latencies = StatGroup(Telemetry, concurrency = Concurrency.Strict)
latencies.update(value = 12.7)
val p99 = latencies.read()[Telemetry.latencyP99]

// A schema can mix modalities, and each one is driven by its own group,
// because a scalar observation and a discrete key are not the same input.
val users = DiscreteStatGroup(Telemetry, concurrency = Concurrency.Strict)
users.update(value = userIdHash)
val distinctUsers = users.read()[Telemetry.uniqueUsers]
```

`StatGroup` covers the `series` entries, `DiscreteStatGroup` the
`discrete` ones, and `PairedStatGroup`, `VectorStatGroup`, and
`RegressionStatGroup` the remaining modalities. Reading a key from a
group that does not own it throws, so pick the group matching the entry.

## Bandits

Bandits build on per-arm stats and select arms from their snapshots.
The `bandit` package overview on the Dokka site covers the available
univariate and contextual bandits, policies, and arms.

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

## Concurrency

Each stat picks a concurrency mode at construction. The default is
single-threaded and the cheapest. Relaxed mode uses atomic cells for
additive and monotonic stats and permits drift in coupled recurrences;
structures that require serial mutation still lock. No numerical error
bound is guaranteed for coupled state under contention. Strict mode
locks coupled recurrences, while independently updated cells can still
produce a snapshot spanning several updates. HighWrite uses striped
adders for additive JVM stats and follows Strict semantics for coupled
state and on other platforms. Floating-point results depend on update
order. See the `Concurrency` enum and each stat's KDoc on the Dokka site
for snapshot consistency and locking details.

```kotlin
val hits = SumStat(concurrency = Concurrency.HighWrite)
val ols = UnivariateRegressionStat(concurrency = Concurrency.Strict)
```

The [kumulant-bench](kumulant-bench) module provides registry-based
quality and throughput runs. Its concurrency checks cover sum, mean,
and variance updates, merges, and snapshots under Strict, Relaxed,
and HighWrite.
