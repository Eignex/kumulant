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

Kumulant is a pure Kotlin multiplatform library of streaming statistical
accumulators: single-pass, mergeable, and constant-memory in the number of
observations.

## Features

* Single-pass, mergeable, constant-memory accumulators for sums, means, variances, higher moments, quantiles, cardinality, heavy hitters, rates, regression, and scoring losses.
* Composable operations: time-windowed aggregation, weighted updates, pre-update transforms, predicate filtering, and adapters between scalar, paired, vector, and discrete input streams.
* Serializable stat schemas via kotlinx.serialization that round-trip through JSON or protobuf and rehydrate into live stat groups on the other side.
* Wire-expressible transforms and filters via a serializable expression AST — operations travel without lambdas.
* Concurrency selectable per stat: single-threaded, lock-free relaxed, fully serialized, or striped adders for write-heavy paths on the JVM.
* Pure Kotlin Multiplatform: JVM, JS (IR), wasmJs, wasmWasi, Linux x64/Arm64, macOS x64/Arm64, mingwX64, iOS x64/Arm64/SimulatorArm64.

## Overview

Every stat takes one observation at a time, holds bounded state, and produces a
result that can be merged with the result of another instance running on a
different machine. That makes the same primitives usable for in-process
counters, parallel workers folding into a single estimate, and distributed
aggregation over a queue.

There are three layers stacked on each other; pick the lowest one that fits.
The bottom layer is the live stats — concrete classes like MeanStat, OLSStat,
HyperLogLogStat. Construct, feed observations, read the result. Use these
directly when the choice of stat is fixed at compile time.

The middle layer is composable operations — extension functions on the live
stat interfaces such as withWeight, atX, windowed, transformValue, filter,
foldVector. Each preserves the inner stat's result type and merge semantics,
so wrapped stats remain ordinary stats.

The top layer is the wire schema — pure-data StatSpec variants (Mean, OLS,
DDSketch, …) under kotlinx.serialization polymorphism. A StatSchema is a
declared bag of specs that round-trips through JSON or protobuf and rehydrates
into a live StatGroup on the other side. Reach for it when the choice of
stats has to travel — for instance, a UI declaring what to track over a
stream of events processed elsewhere.

## Installation

```kotlin
dependencies {
    implementation("com.eignex:kumulant:0.1.0")
}
```

The wire schema layer additionally requires kotlinx-serialization-core and a
format such as kotlinx-serialization-json.

---

## Live stats

Every stat implements one of four modality interfaces — SeriesStat (scalar
input), PairedStat (x/y input), VectorStat (fixed-dim vector input), or
DiscreteStat (Long input) — and exposes the same update, read, merge, reset,
create surface.

```kotlin
val mean = MeanStat()
for (x in stream) mean.update(x)
println(mean.read().mean)

val sketch = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5, 0.99))
for (x in stream) sketch.update(x)
val r = sketch.read() // r.probabilities and r.quantiles are parallel arrays

val ols = OLSStat()
for ((x, y) in pairs) ols.update(x, y)
val fit = ols.read()
val yHat = fit.slope * 7.0 + fit.intercept
```

Results are immutable data classes annotated @Serializable. Two stats of the
same type merge by feeding one's result into the other's merge:

```kotlin
val a = MeanStat().apply { repeat(100) { update(it.toDouble()) } }
val b = MeanStat().apply { repeat(100) { update((it + 100).toDouble()) } }
a.merge(b.read()) // a is now the mean of 0..199
```

| Family       | Stats                                                                                  |
|--------------|----------------------------------------------------------------------------------------|
| Summary      | `Sum`, `Mean`, `Min`, `Max`, `Range`, `Variance`, `Moments`, `BernoulliSum`, `Count`   |
| Quantile     | `DDSketch`, `TDigest`, `HdrHistogram`, `LinearHistogram`, `ReservoirHistogram`, `FrugalQuantile` |
| Cardinality  | `HyperLogLog`, `LinearCounting`                                                        |
| Sketch       | `BloomFilter`, `CountMinSketch`, `MinHash`, `SpaceSaving`                              |
| Rate         | `Rate`, `CounterRate`, `DecayingRate`                                                  |
| Regression   | `OLS`, `Covariance`, `Ridge`, `Lasso`                                                  |
| Decay        | `DecayingSum`, `DecayingMean`, `DecayingVariance`, `EwmaMean`, `EwmaVariance`          |
| Score        | `MseLoss`, `MaeLoss`, `LogLoss`, `PinballLoss`, `BrierScore`, `Auc`, `Reliability`, `PitHistogram` |

---

## Composable operations

Operations are extension functions on the live stat interfaces that produce
another stat of the same modality (or a different modality, for adapters like
atX or foldVector).

```kotlin
// Time-windowed mean over 1 minute, with 10 slices.
val recentMean = MeanStat().windowed(1.minutes, slices = 10)

// Drive a SumStat from the y-coordinate of (x, y) inputs.
val sumY = SumStat().atY()

// Mean of x*y, computed by folding the pair before update.
val meanXY = MeanStat().foldPaired { x, y -> x * y }

// Drop non-positive samples before aggregating.
val positiveMean = MeanStat().filter { it > 0.0 }
```

| Operation                                            | Effect                                                              |
|------------------------------------------------------|---------------------------------------------------------------------|
| `withWeight(w)`                                      | Multiplies the per-update weight (all four modalities).             |
| `withValue(v)`                                       | Replaces the incoming value with a constant.                        |
| `transformValue`, `transformPair`, `transformX/Y`    | Pre-update lambda transform on series / paired / discrete inputs.   |
| `filter { … }`                                       | Drops updates the predicate rejects.                                |
| `windowed(duration, slices)`                         | Sliding ring of sub-windows over a time duration.                   |
| `atX`, `atY`, `atIndex`, `atIndices`                 | Drive a series/paired stat from a paired/vector stream.             |
| `withFixedX/Y`, `withTimeAsX/Y`                      | Pin one axis of a paired stat to a constant or the timestamp.       |
| `foldPaired`, `foldVector`                           | Lift a series stat to consume paired or vector input.               |
| `vectorized(dimensions)`                             | Replicate a series stat per coordinate of a vector input.           |
| `asSeries`, `asDiscrete`                             | Cast a discrete stat to series surface or vice versa.               |

---

## Wire schema

StatSpec is the pure-data counterpart of every live stat — a @Serializable
sealed hierarchy whose subclasses are the parameter records (Mean, Sum,
DDSketch, OLS, HyperLogLog, DecayingMean, …). StatSchema is a typed bag of
specs that you declare once, then either materialize directly or send over the
wire and rehydrate on the other side.

```kotlin
object Telemetry : StatSchema(concurrency = Concurrency.Strict) {
    val latencyMean by series(Mean)
    val latencyP99 by series(DDSketch(probabilities = listOf(0.99)))
    val errorRate by series(Rate)
    val uniqueUsers by discrete(HyperLogLog(precision = 14))
}

val group = StatGroup(Telemetry)
group.update(value = 12.7)

val snapshot = group.read()
val p99 = snapshot[Telemetry.latencyP99]
```

The schema is its own wire payload:

```kotlin
val def: StatSchemaDef = Telemetry.statSchemaDef()
val payload = Json.encodeToString(def)

// On a different machine / process:
val decoded = Json.decodeFromString<StatSchemaDef>(payload)
val rehydrated = StatGroup(stats = decoded.materializeSeries(Concurrency.Strict))
```

Composable operations are mirrored on the spec layer as wire-friendly
counterparts. `Mean.windowed(60_000, slices = 10).withWeight(0.5)` serializes
verbatim. Lambdas are not on the wire — filter, transformValue, transformPair,
and the fold operations all use ScalarExpr, BoolExpr, and VectorExpr
expression ASTs instead:

```kotlin
// Wire-expressible: ignore non-positive samples, square the rest.
val spec = Mean
    .filter(X gt 0.0)
    .transform(X * X)
```

---

## Concurrency

Every stat constructor takes a Concurrency argument that controls the
cell-encoding and locking strategy chosen for that stat:

| Level         | Behavior                                                                                   |
|---------------|--------------------------------------------------------------------------------------------|
| `None`        | Single-threaded; no synchronisation. Default. Cheapest path.                               |
| `Relaxed`     | Multi-threaded, lock-free. Coupled-state stats (Welford-style) may drift, but never throw. |
| `Strict`      | Multi-threaded, serialised where coupling demands it. Full correctness.                    |
| `HighWrite`   | Multi-threaded write-heavy. On JVM, striped adders for naively additive stats.             |

For bag-of-stats deployments, set Concurrency once on the StatSchema and it
propagates to every materialized stat. For ad-hoc construction, pass it to the
stat constructor directly:

```kotlin
val hits = SumStat(concurrency = Concurrency.HighWrite)
val ols = OLSStat(concurrency = Concurrency.Strict)
```

---

## Platforms

Kumulant compiles for every standard Kotlin Multiplatform target: JVM, JS (IR,
browser + node), wasmJs, wasmWasi, Linux x64/Arm64, macOS x64/Arm64, Windows
(mingwX64), iOS x64/Arm64/SimulatorArm64. The JVM target additionally backs
Concurrency.HighWrite with java.util.concurrent.atomic.LongAdder / DoubleAdder
for naively additive stats.
