# Operations

Operations wrap one Stat and change how it sees its input or how it
reports its output. Every operation preserves the result type, so chains
compose cleanly.

There are two surfaces for the same operations. The live-stat extensions
in com.eignex.kumulant.operation work on a constructed Stat, and they
are the only place lambda-bound operations (with arbitrary Kotlin
closures) live. The spec extensions in com.eignex.kumulant.schema.Operations
work on a StatSpec, and the lambda-bound ones have AST counterparts
(ScalarExpr, BoolExpr, VectorExpr) so the whole composition serialises.
Use specs when you need wire-portability, and live extensions when you
need an arbitrary Kotlin lambda.

## Per-update weighting

`withWeight(constant)` replaces every incoming observation's weight with
a constant, discarding the caller-supplied weight. It is available on
each modality, both live and as a spec.

```kotlin
val pinned = MeanStat().withWeight(0.5)
val pinnedSpec = Mean.withWeight(0.5)
```

`weightBy` multiplies the caller-supplied weight by a per-update value.
The live form takes a Kotlin lambda; the spec form takes a `ScalarExpr`
from the AST DSL. Series sees the value, paired sees `(x, y)`, vector
sees the full vector, discrete sees `value.toDouble()`.

```kotlin
val squared = SumStat().weightBy { v -> v * v }
val squaredSpec = Sum.weightBy(X * X)
```

Use `withWeight` to pin the weight regardless of input. Use `weightBy`
to scale per-update on top of whatever weight the caller supplied.

## Filtering

`filter` drops observations that fail a predicate. The live form takes a
Kotlin lambda; the spec form takes a BoolExpr from the AST DSL (X gt 0.0,
X lt 100.0 and X gt 0.0, and so on).

```kotlin
val positiveMean = Mean.filter(X gt 0.0).materialize()
```

The wire-friendly form is the standard path. The lambda-only live form
exists for the rare case where the predicate cannot be expressed in the
AST.

## Pre-update transforms

A transform applies a function to the value before it lands in the inner
stat. The spec form uses ScalarExpr and serialises; the live form takes
a Kotlin lambda.

The series and discrete spec surfaces expose `transform(expr)`. Paired
specs expose `transformX(expr)`, `transformY(expr)`, and
`transformPair(xExpr, yExpr)`. Vector specs expose
`transformElement(expr)` for element-wise transforms and
`transformVector(expr)` (with a VectorExpr) for whole-vector transforms.

Two one-off helpers do not need a lambda. `withValue(value)` replaces
every incoming value with a constant, which is useful when the event
matters but the value does not. `asSeries` and `asDiscrete` cast the
modality without changing the observation.

## Folds across modalities

A fold lifts a stat to consume a richer modality. `foldPaired(expr)` on a
series spec makes it consume only x or y from a paired feed.
`foldVector(expr)` on a series spec makes it consume one coordinate from
a vector feed. `foldVector(xExpr, yExpr)` on a paired spec makes it
consume two coordinates from a vector feed. These are how you point a
univariate accumulator at one channel of a multi-channel stream.

## Axis bindings between paired and series

`atX` and `atY` turn a series stat into a paired stat by binding the
other axis to "ignore". `atIndex(i)` turns a series stat into a vector
stat reading coordinate i. `atIndices(ix, iy)` turns a paired stat into
a vector stat. `withFixedX(value)` and `withFixedY(value)` collapse a
paired stat into a series stat by pinning one axis. `withTimeAsX` and
`withTimeAsY` feed the per-update timestamp into one axis of a paired
stat, which is useful for fitting a value-versus-time trend.

## Throttling and sampling

`throttle(every = N)` forwards only every Nth update to the inner stat
and drops the rest. Useful for cheap downsampling, capping audit-leaf
sub-stat work, and any "every Nth observation" diagnostic. The counter
is an atomic so multi-thread updates still pick exactly one in N.

```kotlin
val every10th = MeanStat().throttle(every = 10)
val every10thSpec = Mean.throttle(every = 10)
```

`sample(rate, ...)` keeps each update with Bernoulli probability `rate`.
The live form takes a `kotlin.random.Random` so the caller can plug in
any PRNG; the spec form takes a `Long` seed and the materialiser
constructs a fresh `Random(seed)` so replays are deterministic. The
`Random` instance is not thread-safe by default. Either wrap it
externally or run under `Concurrency.None` if you need exact replay
under contention.

```kotlin
val downsampled = MeanStat().sample(rate = 0.1, random = Random(42))
val downsampledSpec = Mean.sample(rate = 0.1, seed = 42L)
```

Both compose with every other op. `throttle` is deterministic by tick
count; `sample` is deterministic per seed.

## Fanning out: ListStats and StatGroup

To send each update to N inner stats and read back a `ResultList`, use
`ListStats` (and its modality siblings `PairedListStats`,
`VectorListStats`, `DiscreteListStats`). Each takes named entries; on
read the per-entry snapshots come back keyed by both name (for
`.toMap()`) and position (for merge alignment).

```kotlin
val tee = ListStats(
    "count" to CountStat(),
    "mean" to MeanStat(),
    "variance" to VarianceStat(),
)
tee.update(3.0)
val snap: ResultList<Result> = tee.read()
snap.toMap()["mean"]
```

For schema-driven cases where you want `StatKey` / `BoundStat`
plumbing on top, reach for `StatGroup` instead. `ListStats` is the
lighter form for "I just want N stats fanned out."

## Windowing

`windowed(duration, slices, concurrency)` wraps any stat in a
tumbling-slice ring buffer. Observations are bucketed across slice
slots; on read the in-window slots merge into a fresh accumulator and
the merged result returns. More slices smooths the trailing boundary at
the cost of memory and per-read merge work.

```kotlin
val recent = MeanStat().windowed(1.minutes, slices = 10)
val recentSpec = Mean.windowed(1.minutes, slices = 10)
```

Windowing is available on all four modalities, live and as a spec.

## Vectorisation

`vectorized(dimensions)` lifts a per-channel series accumulator into a
vector accumulator over the given number of parallel channels. The
result is a ResultList carrying the per-channel snapshots.

```kotlin
val perChannelMean = Mean.vectorized(dimensions = 8)
```

For sparse inputs, pass `skipZeros = true` so the fan-out walks the
vector's stored entries instead of every coordinate. The cost drops
from O(dimensions) to O(nnz) per update, and an absent index counts as
"no observation" rather than "observed 0.0". Use it for additive
channels like Sum, Count, or Rate where the two are equivalent; leave
the default for Mean and Variance, which need the distinction.

```kotlin
val sparseCounts = Count.vectorized(dimensions = 10_000, skipZeros = true)
```

## RegressionStat decorators

RegressionStat has the same op surface as the other modalities. The bindings
match the ScalarExpr DSL convention: `Y` is the target y, `V` is the feature
vector x, `X` is unused. BoolExpr predicates see the same.

```kotlin
val regressor = StochasticRegression(featureSize = 2)
    .filter(Y gt 0.0)
    .transformY(Y - 1.0)
    .weightBy(Y * Y)
    .throttle(every = 4)
```

Live extensions take Kotlin lambdas with the signature `(VectorView, Double)
-> ...`. Spec/wire forms take ScalarExpr / BoolExpr / VectorExpr just like the
other modalities, and the materializer in `StatFactory.kt` builds the closure
from the AST at construction.

Five leaf RegressionStatSpecs are wire-portable: `BayesianRegression`,
`StochasticRegression`, `DiagonalRegression`, `DecisionTreeRegression`,
`RandomForestRegression`. The tree specs carry a `TreeConfig` and a
`List<Split>`, both serializable. Live regressors with non-serializable
hooks (custom `leafArmFactory`) stay constructable directly but not via
spec.

### Lifting a SeriesStat into the regression modality

`SeriesStat<R>.foldRegression(featureSize, project)` lifts a series stat
into a `RegressionStat<R>` by projecting `(x, y)` to a scalar. The most
common use is the marginal-y view: pass `Y` (spec) or `{ _, y -> y }`
(live).

```kotlin
val marginalY = VarianceStat().foldRegression(featureSize = 1) { _, y -> y }
val marginalYSpec = Mean.foldRegression(featureSize = 1, project = Y)
```

### Tee for RegressionStat

`RegressionListStats` fans one `(x, y)` update to N inner regressors and
produces a `ResultList`. Combine it with `foldRegression` to attach a
marginal-y observation alongside a regressor in a single composite:

```kotlin
val composite = RegressionListStats(
    "tree" to DecisionTreeRegressionStat(featureSize, splits),
    "marginalY" to VarianceStat().foldRegression(featureSize) { _, y -> y },
)
composite.update(x, y)
val snap = composite.read(0L).toMap()
snap["marginalY"]  // marginal mean / variance of y, independent of x
```

This is the standard composition. No bespoke wrapper needed.

## Series shifts

`lag(k)`, `diff(k)`, and `derivative()` are series-only adapters that
re-shape the value before it reaches the inner stat. `lag(k)` forwards
the observation from `k` updates ago, `diff(k)` forwards
`value - value_{t-k}` (the k-step finite difference), and `derivative()`
forwards `(value - prev) * 1e9 / (timestamp - prev)` so the inner stat
sees an instantaneous rate. The first `k` (or one, for `derivative()`)
updates warm silently; nothing is forwarded until enough history exists.

```kotlin
val laggedMean = MeanStat().lag(k = 5)
val firstDifference = SumStat().diff(k = 1)
val slope = MeanStat().derivative()
```

All three are available live and as specs.

## Hysteresis

`hysteresis(low, high)` debounces a noisy numeric stream into a 0.0 /
1.0 signal using two thresholds. The state flips to 1.0 when an input
rises above `high`, and back to 0.0 when an input falls below `low`;
values inside the deadband `[low, high]` hold the current state. Each
update forwards the current debounced state to the inner stat (not just
transitions), so downstream sums and rates observe per-update progress.

```kotlin
val onOffMean = MeanStat().hysteresis(low = 0.2, high = 0.8)
```

Use it to derive a stable on/off interpretation from a flapping signal
before feeding it into a counter, mean, or rate.

## Wall-clock resampling

`resampleByTime(bucket, aggregator)` aligns the input stream onto fixed
wall-clock buckets and forwards one observation per closed bucket to the
inner stat. The per-bucket value is chosen by the aggregator (Mean, Sum,
Last, Min, Max). The in-progress bucket is held until an update arrives
in a later bucket. Compared with `.windowed()`, this exposes the
per-bucket boundary explicitly: downstream sees one update per closed
slot rather than a sliding view of raw inputs.

```kotlin
val perMinuteMean = MeanStat().resampleByTime(1.minutes, ResampleAggregator.Mean)
val perMinuteMeanSpec = Mean.resampleByTime(bucketMillis = 60_000L, ResampleAggregator.Mean)
```

Use it to downsample noisy high-rate streams before feeding them into
sketches, regressors, or any downstream consumer that prefers a
regularly-spaced input.

## Bands around a center

`band(k)` derives `center ± k * scale` from any series stat whose result
implements the `HasCenterScale` trait (currently `VarianceStat`,
`MomentsStat`, `MadStat`, and any future stat exposing the trait). The
returned series stat produces a `BandResult` with center, scale, k,
lower, and upper. Merging through the band wrapper is unsupported —
merge the inner stat directly, then read the band.

```kotlin
val variance = VarianceStat().band(k = 2.0)
val mad = MadStat().band(k = 1.5)
```

Pair it with `.windowed(...)` for sliding bands, or feed the inner stat
through any of the other operators (filter, transform, weightBy) before
deriving the band.

## Operation locality

Most operations are zero-state: filter, transform, withWeight, withValue,
weightBy, atX/atY/atIndex, withFixedX/Y, withTimeAsX/Y, asSeries,
asDiscrete, fold. They delegate every cell, lock, and snapshot to the
inner stat and just intercept update to massage its input. A chain like
`Mean.filter(X gt 0.0).withWeight(0.5).windowed(1.minutes)` is still one
MeanStat doing the math, with a thin chain of adapters in front.

The stateful ops carry small extras: `throttle` keeps a single atomic
tick counter, `sample` keeps a `Random` instance, `windowed` keeps a
ring buffer of slice sub-stats, and `vectorized` keeps `dimensions`
sub-stats. `ListStats` / `StatGroup` keep one inner per entry.
