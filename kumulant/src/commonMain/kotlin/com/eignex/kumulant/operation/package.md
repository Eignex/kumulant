# Package com.eignex.kumulant.operation

Composable wrappers that change how a stat sees its input or how it
reports its output. Every operation preserves the result type, so chains
compose cleanly.

## Two surfaces, one set of operations

There are two surfaces for the same operations:

- **Live extensions** (this package) operate on a constructed
  [Stat][com.eignex.kumulant.core.Stat] and accept arbitrary Kotlin
  lambdas. Reach for them when you need a closure that captures
  externally and the predicate / projection can't be expressed as an
  AST node.
- **Spec extensions** in `com.eignex.kumulant.schema` operate on a
  [StatSpec][com.eignex.kumulant.schema.StatSpec] and take
  [ScalarExpr][com.eignex.kumulant.schema.ScalarExpr] /
  [BoolExpr][com.eignex.kumulant.schema.BoolExpr] /
  [VectorExpr][com.eignex.kumulant.schema.VectorExpr] from the AST DSL.
  Use them when the whole composition needs to serialise.

Both surfaces produce stats that satisfy the same modality interface,
so the rest of the library can't tell the difference at the consumption
end.

## Operation catalog

### Per-update weighting

- `withWeight(constant)` — replace the incoming weight with a constant.
- `weightBy { v -> ... }` (live) / `weightBy(scalarExpr)` (spec) —
  multiply the caller-supplied weight by a per-update value.

### Filtering

- `filter { v -> v > 0 }` (live) / `filter(BoolExpr)` (spec) — drop
  observations failing a predicate.

### Pre-update transforms

- `transform(...)` (series, discrete) / `transformX` / `transformY` /
  `transformPair` (paired) / `transformElement` / `transformVector`
  (vector) — apply a projection before the inner stat sees the value.
- `withValue(v)` — pin every observation's value to a constant.
- `asSeries` / `asDiscrete` — cast the modality without changing the
  observation.

### Folds across modalities

- `foldPaired(expr)` — point a series stat at one axis of a paired
  feed.
- `foldVector(expr)` — point a series stat at one coordinate of a
  vector feed.
- `foldVector(xExpr, yExpr)` — point a paired stat at two coordinates
  of a vector feed.
- `SeriesStat.foldRegression(featureSize, project)` — lift a series
  stat into the regression modality by projecting `(x, y)` to a scalar.

### Axis bindings

- `atX` / `atY` — turn a series stat into a paired stat by binding the
  other axis to "ignore".
- `atIndex(i)` — turn a series stat into a vector stat reading
  coordinate `i`.
- `atIndices(ix, iy)` — turn a paired stat into a vector stat.
- `withFixedX(v)` / `withFixedY(v)` — collapse a paired stat into a
  series stat by pinning one axis.
- `withTimeAsX` / `withTimeAsY` — feed the per-update timestamp into
  one axis of a paired stat.

### Throttling and sampling

- `throttle(every = N)` — forward only every Nth update; deterministic
  by tick count.
- `sample(rate, Random)` (live) / `sample(rate, seed)` (spec) — keep
  each update with Bernoulli probability `rate`; deterministic per
  seed.

### Tee / fan-out

- `ListStats(...)` (and its modality siblings `PairedListStats`,
  `VectorListStats`, `DiscreteListStats`, `RegressionListStats`) — fan
  each update out to N inner stats and read back a
  [ResultList][com.eignex.kumulant.core.ResultList].
- `StatGroup` — schema-driven fan-out with typed `StatKey` /
  `BoundStat` plumbing; see [com.eignex.kumulant.schema].

### Windowing and resampling

- `windowed(duration, slices)` — tumbling-slice ring buffer; on read
  the in-window slots merge into a fresh accumulator.
- `resampleByTime(bucket, aggregator)` — align inputs onto fixed
  wall-clock buckets and forward one observation per closed bucket.

### Vectorisation

- `vectorized(dimensions)` — lift a series accumulator into a vector
  accumulator over `dimensions` parallel channels. Pass
  `skipZeros = true` for sparse inputs (drops the cost from
  O(dimensions) to O(nnz) per update).

### Series shifts

- `lag(k)` — forward the observation from `k` updates ago.
- `diff(k)` — forward `value - value_{t - k}` (k-step finite
  difference).
- `derivative()` — forward `(value - prev) * 1e9 / (timestampNanos - prevTimestamp)`.

### Hysteresis and bands

- `hysteresis(low, high)` — debounce a noisy numeric stream into a
  0.0 / 1.0 signal using two thresholds.
- `band(k)` — derive `center ± k * scale` from any series stat whose
  result implements [HasCenterScale][com.eignex.kumulant.core.HasCenterScale].

### Per-feature scaling

- `standardScaler()` / `minMaxScaler(low, high)` — series and paired
  forms.
- `standardScaleFeatures(dimensions)` / `minMaxScaleFeatures(...)` —
  vector and regression forms.
- `withFeedback(primary, project)` — generic feedback operator: pair
  an inner stat with a primary whose snapshot drives a projection
  expression applied to the observation before the inner stat sees it.

## Operation locality

Most operations are zero-state: `filter`, `transform`, `withWeight`,
`weightBy`, `atX` / `atY` / `atIndex`, `withFixedX` / `Y`,
`withTimeAsX` / `Y`, `asSeries`, `asDiscrete`, the folds. They delegate
every cell, lock, and snapshot to the inner stat and just intercept
`update` to massage its input. A chain like
`Mean.filter(X gt 0.0).withWeight(0.5).windowed(1.minutes)` is still one
[MeanStat][com.eignex.kumulant.stat.summary.MeanStat] doing the math,
with a thin chain of adapters in front.

The stateful ops carry small extras: `throttle` keeps a single atomic
tick counter, `sample` keeps a `Random` instance, `windowed` keeps a
ring buffer of slice sub-stats, and `vectorized` keeps `dimensions`
sub-stats. `ListStats` / `StatGroup` keep one inner per entry.

## Spec round-trip

When the composition is built on the spec surface, the entire chain
serialises:

```kotlin
val spec: StatSpec = Mean
val json = SchemaJson.encodeToString(spec)
val decoded = SchemaJson.decodeFromString<StatSpec>(json)
```

See the spec-side documentation at [com.eignex.kumulant.schema] for
end-to-end examples.
