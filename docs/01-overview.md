# Overview

Kumulant is a streaming-statistics library. You feed observations into an
accumulator one at a time, ask for a result whenever you want one, and the
accumulator's memory stays bounded no matter how long the stream runs. Two
accumulators of the same shape can be merged into one, so partial results
from parallel workers stitch back together without re-reading inputs.

## Mental model

Every kumulant accumulator implements Stat. The contract is three verbs
and one constant.

The `update` call folds a single observation into the running state. The
exact signature depends on the modality: a series stat takes one Double, a
paired stat takes two Doubles, a vector stat takes a DoubleArray, a
discrete stat takes a Long, and a regression stat takes a VectorView plus
a Double response. The `read` call materialises the current state as an
immutable Result. Reads never mutate, so you can call them as often as you
like. The `merge` call folds another accumulator's snapshot into this
one. Snapshots are the merge unit, not live accumulators, which is what
lets merge cross a process boundary. The `concurrency` field on the stat
records the thread-safety contract picked at construction; see
[Concurrency](03-concurrency.md).

```kotlin
val mean = MeanStat()
for (x in stream) mean.update(x)
val snapshot = mean.read()
println(snapshot.mean)

val other = MeanStat()
for (x in otherStream) other.update(x)
mean.merge(other.read())
```

`reset` returns to the prior-seeded baseline. `create` spawns a fresh
accumulator with the same configuration, optionally overriding the
concurrency mode.

## What's in the library

Everything is built around Stat. The packages divide responsibility along
expected lines.

The core package holds the Stat and Result interfaces, the Concurrency
enum, and the cross-cutting result traits (HasRate, HasSampleVariance,
HasShapeMoments, HasLinearModel, HasSlope, HasRegression). The stat
package holds concrete accumulators grouped by family: summary, quantile,
cardinality, sketch, rate, decay, regression, tree, score. See
[Stats](02-stats.md). The operation package holds composition adapters
that wrap one stat and change how it sees its input: windowed, weighted,
filtered, transformed, vectorised. See [Operations](04-operations.md).
The schema package holds StatSchema, StatGroup, and the StatSpec family
that lets a whole bag of stats round-trip on the wire. See
[Schemas and the wire](05-schemas.md). The bandit package holds the
bandit hierarchy and the univariate and contextual families. See
[Bandits](06-bandits.md). The math and stream packages are mostly
internal: math carries the vector and matrix primitives plus sampling
extensions on Random, and stream is where the four concurrency modes
are implemented (atomic cells, ring buffers, hashing). The only
user-facing piece is VectorView, which is the input type for
VectorStat and RegressionStat and lets sparse callers feed sparse
vectors without materialising them.

## Lifecycle of an observation

For a bare stat the path is direct: update mutates the cells the stat
owns, read reads them.

For a composed stat such as `MeanStat().windowed(...).withWeight(0.5)`,
the adapter chain wraps the base accumulator. Each adapter does a single
small job (multiply the weight, drop the observation when a predicate
fails, route the observation to the right slice in a windowed buffer)
then forwards to its inner stat. The result type flows through unchanged.

For a StatGroup or schema-driven group, one update call fans out across
every registered stat, and the group reads them back into a GroupResult
keyed by name.

## Multiplatform

Kumulant builds for the JVM, JS, Wasm, and Kotlin/Native (Linux, macOS,
Windows, iOS). Everything in commonMain is platform-agnostic. Two paths
are platform-specific: the HighWrite concurrency mode uses JVM LongAdder
and DoubleAdder striping for additive stats and falls back to Strict
elsewhere, and time stamps come from each platform's native monotonic
clock via currentTimeNanos.

The wire formats (StatSpec, StatSchemaDef, bandit specs, snapshots)
serialise via kotlinx.serialization and travel between any two platforms
that share the format.

## When to reach for kumulant

Kumulant fits when you have a stream of observations and want
bounded-memory summaries or sketches with merge semantics, when you want
quantile, cardinality, and heavy-hitter sketches alongside the usual
moments and rates under one interface, when you are building an online
regressor or a bandit and want the per-arm accumulator, the policy, the
wire format, and the merge story to share a contract, or when you want
the same code to run on the JVM, in the browser, on native, and on Wasm.
