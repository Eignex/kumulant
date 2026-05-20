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

`withWeight(constant)` multiplies every incoming observation's weight by
a constant. It is available on each modality, both live and as a spec.

```kotlin
val downweighted = MeanStat().withWeight(0.5)
val specDownweighted = Mean.withWeight(0.5)
```

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

## Operation locality

All operations are zero-state except windowed and vectorized. The others
delegate every cell, lock, and snapshot to the inner stat and just
intercept update to massage its input. A chain like
`Mean.filter(X gt 0.0).withWeight(0.5).windowed(1.minutes)` is still one
MeanStat doing the math, with a thin chain of adapters in front.
