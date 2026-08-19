# Package com.eignex.kumulant.core

Foundation types every other package builds on. Everything else in the
library imports from here: the modality interfaces, the result hierarchy,
the cross-cutting result traits, and the concurrency contract.

## The Stat contract

[Stat] is the root interface for all accumulators. A stat is built once,
fed observations through `update`, snapshotted with `read`, and combined
with peers via `merge`. The exact signature of `update` depends on the
modality.

### The five modalities

| Interface | update | Typical input |
|-----------|--------|---------------|
| [SeriesStat] | `update(value: Double, weight: Double = 1.0)` | One scalar per observation |
| [DiscreteStat] | `update(value: Long, weight: Double = 1.0)` | Opaque keys, integer counts |
| [PairedStat] | `update(x: Double, y: Double, weight: Double = 1.0)` | Scalar `(x, y)` pairs |
| [VectorStat] | `update(vector: VectorView, weight: Double = 1.0)` | Multi-channel observations |
| [RegressionStat] | `update(x: VectorView, y: Double, weight: Double = 1.0)` | Vector covariate, scalar response |

Every `update` overload has a sibling that takes an explicit
`timestampNanos`; the no-timestamp form calls
`com.eignex.kumulant.stream.currentTimeNanos`. Stats that ignore time
silently drop the stamp. Stats that care about it (rates, windowed
wrappers, decaying accumulators) treat it as the ordering signal; pass a
monotonic stamp when replaying a log.

[VectorStat] and [RegressionStat] both accept a `VectorView`
([com.eignex.koblas.VectorView]) so sparse callers can feed sparse
vectors without materialising them. Each also exposes a `DoubleArray`
convenience overload that wraps the array in a `DenseVector` before
forwarding.

### Snapshots

A [Result] is an immutable snapshot of a stat's state at a moment in
time. Concrete results are `@Serializable` data classes so the same
value that comes out of `read` goes into `merge` over the wire. The
serializer round-trip is the merge boundary; workers can ship snapshots
across a process boundary without sharing live stats.

[ResultList] wraps an ordered list of results with per-entry names; it is
the result type of fan-out wrappers ([com.eignex.kumulant.schema.spec.Vectorized],
the various `ListStats` materializations) so consumers can look up
per-entry snapshots by name or position.

[IndexedResult] wraps an inner result with the coordinate index currently
being evaluated. Per-coordinate feedback wrappers in the schema layer
pass this to the projection AST so it can branch on `VIndex` and still
address primary-snapshot fields (`Center`, `Scale`, `Low`, `High`).

## Cross-cutting result traits

Traits in `StatTraits.kt` surface on multiple stat families. A consumer
written against a trait works for every concrete result that implements
it; that is how one downstream pipeline handles both a univariate fit
and a multivariate one, or both a `MeanStat` and a `DecayingMeanStat`.

| Trait | Exposes |
|-------|---------|
| [HasRate] | `rate` (events per second), `per(duration)` |
| [HasSampleVariance] | `totalWeights`, `variance`, `stdDev`, `sampleVariance`, `sampleStdDev` |
| [HasShapeMoments] | extends `HasSampleVariance` with `m3`, `m4`, `skewness`, `kurtosis`, and the size-adjusted unbiased variants |
| [HasLinearModel] | `weights: VectorView`, `bias: Double`, `predict(VectorView)` over a fitted hyperplane |
| [HasSlope] | scalar special case: `slope`, `intercept`, `predict(Double)`; implements `HasLinearModel` |
| [HasRegression] | `sse`, `ssr`, `mse`, `rmse`, `rSquared` on top of `HasSampleVariance` |
| [HasCenterScale] | `center: Double`, `scale: Double`; consumed by standardize projections and the band wrapper |
| [HasMinMax] | `min: Double`, `max: Double`; consumed by min-max projections |

## Concurrency contract

[Concurrency] is the deployment knob; `None` / `Relaxed` / `Strict` /
`HighWrite`. Each stat translates the chosen level into a cell encoding
and lock strategy that honours it for the stat's mathematical structure.
The enum's own KDoc covers the four modes in detail; the short version:

- `None`: single-threaded, no synchronisation, default.
- `Relaxed`: lock-free atomic cells; coupled-state stats may drift
  by ULPs under contention but never throw.
- `Strict`: coarse lock around coupled state; exact arithmetic.
- `HighWrite`: JVM-only striped adders for naively additive stats
  under heavy concurrent writes; falls back to `Strict` elsewhere.

To give a coherent bag of stats one contract, declare them in a
[com.eignex.kumulant.schema.runtime.StatSchema] and pass the level to the
group that materializes it, which applies it to every stat the schema
describes, nested groups included. The schema carries no level of its own,
so the same one can back a single-threaded shard and a contended
coordinator.

## Reading an empty accumulator

`read()` on a stat that has seen no observations is always legal and never
throws. What it reports depends on whether the statistic has a meaningful
identity element:

| Family | Empty read reports |
|--------|--------------------|
| Sum, Count, BernoulliSum | `0.0`, the additive identity |
| Min, Max | `+Infinity` / `-Infinity`, the comparison identities |
| HdrHistogram, LinearHistogram, Reservoir | empty arrays |
| DDSketch, TDigest, Mad | `NaN` per quantile |
| Auc | `NaN` |
| Mean, Variance, Moments | `0.0` for every field |

Where an identity exists, it is reported: a sum of nothing is genuinely zero,
and the infinities are the correct starting points for a min/max fold and are
already distinguishable from real data. Where none exists, the result is `NaN`
rather than a plausible-looking number. An empty quantile sketch has no p99,
and reporting `0.0` claimed one; on a latency dashboard that read as excellent
rather than as absent.

Mean, Variance and Moments still report `0.0` on an empty stream. That has the
same ambiguity as the quantile case did and is a candidate for the same
treatment, but changing it is a wider break than the sketches were.

Rather than knowing which field carries the count, gate on
[com.eignex.kumulant.core.HasObservationCount.isEmpty]:

```kotlin
val r = sketch.read()
if (!r.isEmpty) report(r.quantiles.last())
```

The count is spelled `totalWeights` on most results, `totalSeen` on the
sketches and `totalWeight` on a few others; `HasObservationCount` normalises
that, so `isEmpty` is the same check everywhere it is implemented.

## Lifecycle

@sample com.eignex.kumulant.samples.basicMeanLifecycle
