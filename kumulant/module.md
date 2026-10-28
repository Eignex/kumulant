# Module kumulant

Kumulant is a streaming machine-learning library for Kotlin Multiplatform.
Every model runs online: feed observations into an accumulator one at a time,
ask for a result whenever you want one, and memory stays bounded no matter how
long the stream runs. Two accumulators of the same shape can be merged into
one, so partial results from parallel workers stitch back together without
re-reading inputs.

## Mental model

Every accumulator implements [com.eignex.kumulant.core.Stat]. The main operations
and configuration are:

- `update(...)` folds a single observation into the running state. The exact
  signature depends on the modality (see the [stat][com.eignex.kumulant.stat]
  package for the five modality interfaces).
- `read()` materialises the current state as an immutable
  [Result][com.eignex.kumulant.core.Result] snapshot without adding an observation.
  Snapshot consistency follows the stat's concurrency contract.
- `merge(snapshot)` folds another accumulator's snapshot into this one.
  Snapshots are the merge unit, not live accumulators; that is what lets
  merge cross a process boundary.
- `concurrency: Concurrency` records the thread-safety contract the stat was
  built for. See [com.eignex.kumulant.core.Concurrency].

`reset()` returns the stat to its construction baseline. `create()` spawns a
fresh accumulator with the same configuration, optionally overriding the
concurrency mode.

```kotlin
val mean = MeanStat()
for (x in doubleArrayOf(1.0, 2.0, 3.0)) mean.update(x)
val snapshot = mean.read()
println(snapshot.mean) // 2.0

val peer = MeanStat()
for (x in doubleArrayOf(4.0, 5.0)) peer.update(x)
mean.merge(peer.read())
println(mean.read().mean) // 3.0
```

The compile-checked version of this example lives at
[com.eignex.kumulant.samples.basicMeanLifecycle], pulled into the
rendered API docs via `@sample` directives on individual classes.

## What's in the library

- [com.eignex.kumulant.core]: the [Stat][com.eignex.kumulant.core.Stat] and
  [Result][com.eignex.kumulant.core.Result] interfaces, the
  [Concurrency][com.eignex.kumulant.core.Concurrency] enum, and the
  cross-cutting result traits.
- [com.eignex.kumulant.stat]: concrete accumulators grouped by family:
  summary, quantile, cardinality, sketch, rate, decay, regression (with glm
  and tree subfamilies), score, calibration, anomaly, event, change, forecast.
- [com.eignex.kumulant.schema.ops]: composable spec operators that change how a
  stat sees its input (filtering, weighting, windowing, sampling, lagging)
  or how it reports its output (folding, transforming, projecting). Materialize
  the composed spec to build its live adapters.
- [com.eignex.kumulant.schema]: typed, named, wire-portable schemas. Declare
  a bag of stats once, materialise it into a live [StatGroup][com.eignex.kumulant.schema.runtime.StatGroup],
  encode the schema to wire and rehydrate on the other side.
- [com.eignex.kumulant.bandit]: multi-armed and contextual bandits built on
  the same Stat/Result foundation.

## Conventions

- Each built-in stat has a corresponding spec in
  [com.eignex.kumulant.schema.spec], carrying configuration only. Specs are
  serializable through the spec roots. JSON uses compact node names and
  operator keys; other formats, including CBOR and Protobuf, retain the
  generated class tags and field layouts.
- Concrete stat KDoc includes a summary, Use cases, Memory, Update, and
  Concurrency sections. See
  [com.eignex.kumulant.stat.summary.MeanStat] for the canonical shape.
- Results are snapshots that can be serialized and passed to `merge()`.
  Each stat documents its merge approximations and any state omitted from
  the snapshot.

