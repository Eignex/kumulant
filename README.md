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
* Transforms and filters expressed via a serializable expression AST, so every operation travels on the wire (no lambda escape hatch).
* Concurrency selectable per stat: single-threaded, lock-free relaxed, fully serialized, or striped adders for write-heavy paths on the JVM.
* Pure Kotlin Multiplatform: JVM, JS (IR), wasmJs, wasmWasi, Linux x64/Arm64, macOS x64/Arm64, mingwX64, iOS x64/Arm64/SimulatorArm64.

## Overview

Every stat takes one observation at a time, holds bounded state, and produces a
result that can be merged with the result of another instance running on a
different machine. That makes the same primitives usable for in-process
counters, parallel workers folding into a single estimate, and distributed
aggregation over a queue.

Three layers stack on each other; pick the lowest one that fits.

1. **Live stats** — concrete classes like `MeanStat`, `OLSStat`, `HyperLogLogStat`.
   Construct, feed observations, read the result. Reach for these when the
   choice of stat is fixed at compile time.
2. **Composable operations** — parameter-only extensions on live stats
   (`withWeight`, `windowed`, `atX/Y`, `vectorized`, …) plus AST-driven
   operations on specs (`filter`, `transform`, `foldPaired`, …). Lambda-bound
   operations exist only on the spec layer where the lambda is an
   expression-AST; live stats have no public lambda filter/transform/fold.
3. **Wire schema** — pure-data `StatSpec` variants (`Mean`, `OLS`, `DDSketch`,
   …) under kotlinx.serialization polymorphism. A `StatSchema` is a declared
   bag of specs that round-trips through JSON or protobuf and rehydrates into
   a live `StatGroup` on the other side. Reach for it when the choice of
   stats has to travel.

## Installation

```kotlin
dependencies {
    implementation("com.eignex:kumulant:0.1.0")
}
```

The wire schema layer additionally requires kotlinx-serialization-core and a
format such as kotlinx-serialization-json.

## Live stats

Every stat implements one of four modality interfaces: SeriesStat (scalar
input), PairedStat (x/y input), VectorStat (fixed-dim vector input), or
DiscreteStat (Long input). All four expose the same update, read, merge,
reset, and create surface.

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

| Family       | Stats                                                                          |
|--------------|--------------------------------------------------------------------------------|
| Summary      | Sum, Mean, Min, Max, Range, Variance, Moments, BernoulliSum, Count             |
| Quantile     | DDSketch, TDigest, HdrHistogram, LinearHistogram, ReservoirHistogram, FrugalQuantile |
| Cardinality  | HyperLogLog, LinearCounting                                                    |
| Sketch       | BloomFilter, CountMinSketch, MinHash, SpaceSaving                              |
| Rate         | Rate, CounterRate, DecayingRate                                                |
| Regression   | OLS, Covariance, Ridge, Lasso                                                  |
| Decay        | DecayingSum, DecayingMean, DecayingVariance, EwmaMean, EwmaVariance            |
| Score        | MseLoss, MaeLoss, LogLoss, PinballLoss, BrierScore, Auc, Reliability, PitHistogram |

## Composable operations

Parameter-only operations are extension functions on the live stat interfaces.
Anything that would take a lambda is exposed only on the spec layer, where it
takes a serializable expression AST instead, so an operation never has the
choice of being non-portable.

```kotlin
// Time-windowed mean over 1 minute, with 10 slices.
val recentMean = MeanStat().windowed(1.minutes, slices = 10)

// Drive a SumStat from the y-coordinate of (x, y) inputs.
val sumY = SumStat().atY()

// Drop non-positive samples before aggregating.
val positiveMean = Mean.filter(X gt 0.0).materialize()

// Mean of x*y, computed by folding the pair before update.
val meanXY = Mean.foldPaired(X * Y).materialize()
```

| Operation                                            | Surface | Argument                  | Effect                                                            |
|------------------------------------------------------|---------|---------------------------|-------------------------------------------------------------------|
| withWeight(w)                                        | Live    | Double                    | Multiplies the per-update weight (all four modalities).           |
| withValue(v)                                         | Live    | Double or Long            | Replaces the incoming value with a constant.                      |
| windowed(duration, slices)                           | Live    | Duration, Int             | Sliding ring of sub-windows over a time duration.                 |
| atX, atY, atIndex, atIndices                         | Live    | none / Int                | Drive a series/paired stat from a paired/vector stream.           |
| withFixedX/Y, withTimeAsX/Y                          | Live    | Double / none             | Pin one axis of a paired stat to a constant or the timestamp.     |
| vectorized(dimensions)                               | Live    | Int                       | Replicate a series stat per coordinate of a vector input.         |
| asSeries, asDiscrete                                 | Live    | none                      | Cast a discrete stat to series surface or vice versa.             |
| filter                                               | Spec    | BoolExpr                  | Drops updates the predicate rejects.                              |
| transform                                            | Spec    | ScalarExpr                | Pre-update transform on series or discrete inputs.                |
| transformPair, transformX, transformY                | Spec    | ScalarExpr                | Per-axis pre-update transform on paired inputs.                   |
| transformElement, transformVector                    | Spec    | ScalarExpr / VectorExpr   | Element-wise or whole-vector transform.                           |
| foldPaired, foldVector                               | Spec    | ScalarExpr                | Lift a series stat to consume paired or vector input.             |

## Wire schema

Two distinct things travel on the wire, and keeping them apart matters:

1. A **schema** (`StatSchema` / `StatSchemaDef`) is *definition only*: which
   stats exist, their parameters, and how they compose. Specs are pure data
   classes — no observations, no accumulator state, no behavior. Construction
   of the live stat lives separately in `StatFactory.kt`.
2. A **result** (the `Result` returned by `stat.read()`) is *data only*: the
   current snapshot. Each stat has its own `@Serializable` Result type
   (`SumResult`, `WeightedMeanResult`, `SketchResult`, …). The receiver feeds
   it into a stat of the matching spec via `merge()` to reconstitute state.

Declare a schema, build a live group from it, feed observations, read
snapshots, send those snapshots to whoever aggregates them:

```kotlin
object Telemetry : StatSchema(concurrency = Concurrency.Strict) {
    val latencyMean by series(Mean)
    val latencyP99 by series(DDSketch(probabilities = listOf(0.99)))
    val errorRate by series(Rate)
    val uniqueUsers by discrete(HyperLogLog(precision = 14))
}

val group = StatGroup(Telemetry)
group.update(value = 12.7)

val snapshot: GroupResult = group.read()          // data: the current accumulator state
val p99 = snapshot[Telemetry.latencyP99]
```

The schema itself is its own wire payload, separate from any snapshot:

```kotlin
// Producer side: ship the recipe.
val def: StatSchemaDef = Telemetry.statSchemaDef()
val schemaPayload = Json.encodeToString(def)

// Consumer side: decode and build an empty live group.
val decoded = Json.decodeFromString<StatSchemaDef>(schemaPayload)
val rehydrated = StatGroup(stats = decoded.materializeSeries(Concurrency.Strict))
```

The rehydrated group starts empty; it has no observations. To populate it with
state from elsewhere, encode a snapshot and merge it in:

```kotlin
// Producer side: read a snapshot and ship it.
val resultPayload = Json.encodeToString(group.read())

// Consumer side: merge into the live group built from the same schema.
val incoming = Json.decodeFromString<GroupResult>(resultPayload)
rehydrated.merge(incoming)
```

Schemas and snapshots are decoupled on purpose: the schema travels once at
configuration time, and snapshots flow continuously as workers fold their
partial state up to an aggregator.

All operations are spec-friendly. `Mean.windowed(60_000, slices = 10).withWeight(0.5)`
serializes verbatim. Operations that need a predicate or transform take a
ScalarExpr, BoolExpr, or VectorExpr from the AST in schema/Expr.kt:

```kotlin
// Ignore non-positive samples, square the rest.
val spec = Mean
    .filter(X gt 0.0)
    .transform(X * X)
```

## Concurrency

Every stat constructor takes a Concurrency argument that controls the
cell-encoding and locking strategy chosen for that stat:

| Level         | Behavior                                                                                   |
|---------------|--------------------------------------------------------------------------------------------|
| `None`        | Single-threaded; no synchronisation. Default. Cheapest path.                               |
| `Relaxed`     | Multi-threaded, lock-free. Coupled-state stats (Welford-style) may drift slightly, but never throw. |
| `Strict`      | Multi-threaded, serialised where coupling demands it. Full correctness.                    |
| `HighWrite`   | Multi-threaded write-heavy. On JVM, striped adders for naively additive stats.             |

For bag-of-stats deployments, set Concurrency once on the StatSchema and it
propagates to every stat in the group. For ad-hoc construction, pass it to
the stat constructor directly:

```kotlin
val hits = SumStat(concurrency = Concurrency.HighWrite)
val ols = OLSStat(concurrency = Concurrency.Strict)
```
