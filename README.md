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

Kumulant is a Kotlin multiplatform library of streaming statistical
accumulators. Every stat is single-pass, mergeable, and constant-memory
in the number of observations. Targets: JVM, JS (IR), wasmJs, wasmWasi,
Linux x64/Arm64, macOS x64/Arm64, mingwX64, iOS x64/Arm64/SimulatorArm64.

The API stacks in three layers and you pick the lowest one that fits:
live stats (`MeanStat`, `OLSStat`, …) for compile-time-fixed code,
composable operations (`windowed`, `filter`, `withWeight`, …) for
declarative pipelines, and a serializable wire schema (`StatSchema` /
`StatSpec`) when the choice of stats has to travel across processes.

## Installation

```kotlin
dependencies {
    implementation("com.eignex:kumulant:0.1.0")
}
```

The wire schema layer additionally requires `kotlinx-serialization-core`
and a format such as `kotlinx-serialization-json`.

## Live stats

Every stat implements one of four modality interfaces — `SeriesStat` (scalar
input), `PairedStat` (x/y input), `VectorStat` (fixed-dim vector input), or
`DiscreteStat` (Long input) — all with the same `update`, `read`, `merge`,
`reset`, and `create` surface.

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

Results are `@Serializable` data classes. Merge two stats of the same type by
feeding one's result into the other:

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
Anything that would take a lambda lives on the spec layer and takes a
serializable expression AST, so every operation can travel on the wire.

```kotlin
val recentMean = MeanStat().windowed(1.minutes, slices = 10)
val sumY = SumStat().atY()
val positiveMean = Mean.filter(X gt 0.0).materialize()
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

A **schema** (`StatSchema` / `StatSchemaDef`) is definition only: which stats
exist, their parameters, how they compose. Specs are pure data classes;
construction of the live stat lives in `StatFactory.kt`. A **result** (the
type returned by `stat.read()`) is data only: the current snapshot. Each stat
has its own `@Serializable` Result type (`SumResult`, `WeightedMeanResult`,
`SketchResult`, …); the receiver feeds it into a stat of the matching spec
via `merge()`.

Declare a schema, build a group, feed observations, read snapshots:

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

The schema and snapshots ship separately:

```kotlin
// Producer: schema once, then snapshots continuously.
val schemaPayload = Json.encodeToString(Telemetry.statSchemaDef())
val resultPayload = Json.encodeToString(group.read())

// Consumer: rebuild a live group from the schema, then merge in snapshots.
val def = Json.decodeFromString<StatSchemaDef>(schemaPayload)
val rehydrated = StatGroup(stats = def.materializeSeries(Concurrency.Strict))
rehydrated.merge(Json.decodeFromString<GroupResult>(resultPayload))
```

Operations carry over to specs without ceremony — `Mean.windowed(60_000, slices = 10).withWeight(0.5)`
serializes verbatim. Predicates and transforms take a `ScalarExpr`, `BoolExpr`,
or `VectorExpr` from `schema/Expr.kt`:

```kotlin
val spec = Mean.filter(X gt 0.0).transform(X * X)
```

## Concurrency

Every stat constructor takes a `Concurrency` argument controlling its
cell-encoding and locking strategy:

| Level         | Behavior                                                                                   |
|---------------|--------------------------------------------------------------------------------------------|
| `None`        | Single-threaded; no synchronisation. Default. Cheapest path.                               |
| `Relaxed`     | Multi-threaded, lock-free. Coupled-state stats (Welford-style) may drift slightly, but never throw. |
| `Strict`      | Multi-threaded, serialised where coupling demands it. Full correctness.                    |
| `HighWrite`   | Multi-threaded write-heavy. On JVM, striped adders for naively additive stats.             |

Set it once on the `StatSchema` and it propagates to every stat in the group,
or pass it directly to a stat constructor for ad-hoc use:

```kotlin
val hits = SumStat(concurrency = Concurrency.HighWrite)
val ols = OLSStat(concurrency = Concurrency.Strict)
```
