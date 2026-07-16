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

To configure a coherent bag of stats with one contract, declare them
inside a [com.eignex.kumulant.schema.runtime.StatSchema] with the desired
`concurrency`; the schema propagates the choice to every registered stat
at delegate registration.

## Lifecycle

@sample com.eignex.kumulant.samples.basicMeanLifecycle
