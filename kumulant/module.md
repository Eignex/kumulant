# Module kumulant

Kumulant is a streaming-statistics library for Kotlin Multiplatform. You feed
observations into an accumulator one at a time, ask for a result whenever you
want one, and the accumulator's memory stays bounded no matter how long the
stream runs. Two accumulators of the same shape can be merged into one, so
partial results from parallel workers stitch back together without re-reading
inputs.

## Mental model

Every accumulator implements [com.eignex.kumulant.core.Stat]. The contract is
three verbs and one constant:

- `update(...)` folds a single observation into the running state. The exact
  signature depends on the modality (see the [stat][com.eignex.kumulant.stat]
  package for the four modality interfaces).
- `read()` materialises the current state as an immutable
  [Result][com.eignex.kumulant.core.Result]. Reads never mutate; call them as
  often as you like.
- `merge(snapshot)` folds another accumulator's snapshot into this one.
  Snapshots are the merge unit, not live accumulators; that is what lets
  merge cross a process boundary.
- `concurrency: Concurrency` records the thread-safety contract the stat was
  built for; see [com.eignex.kumulant.core.Concurrency].

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
[com.eignex.kumulant.samples.basicMeanLifecycle]; the corresponding
`@sample` directives on individual classes pull it into the rendered
API docs.

## What's in the library

- [com.eignex.kumulant.core]; the [Stat][com.eignex.kumulant.core.Stat] and
  [Result][com.eignex.kumulant.core.Result] interfaces, the
  [Concurrency][com.eignex.kumulant.core.Concurrency] enum, and the
  cross-cutting result traits.
- [com.eignex.kumulant.stat]; concrete accumulators grouped by family:
  `summary`, `quantile`, `cardinality`, `sketch`, `rate`, `decay`,
  `regression` (with `glm/` and `tree/` subfamilies), `score`, `calibration`,
  `anomaly`, `event`, `change`, `forecast`.
- [com.eignex.kumulant.operation]; composable wrappers that change how a
  stat sees its input (filtering, weighting, windowing, sampling, lagging)
  or how it reports its output (folding, transforming, projecting).
- [com.eignex.kumulant.schema]; typed, named, wire-portable schemas. Declare
  a bag of stats once, materialise it into a live [StatGroup][com.eignex.kumulant.schema.StatGroup],
  encode the schema to wire and rehydrate on the other side.
- [com.eignex.kumulant.bandit]; multi-armed and contextual bandits built on
  the same Stat/Result foundation.

## Conventions

- Every concrete stat ships a sibling `StatSpec` data class (or `data object`
  for parameter-less stats) carrying configuration only. Specs are
  `@Serializable` with `@SerialName` discriminators matching the Kotlin
  class names, so polymorphic JSON / CBOR / Protobuf put the same type
  strings on the wire regardless of format.
- Every public type has KDoc with a one-sentence summary, then
  `**Use cases:**`, `**Memory:**`, `**Update:**`, and `**Concurrency:**`
  sections. See [com.eignex.kumulant.stat.summary.MeanStat] for the
  canonical shape.
- Results are immutable, sealed where it makes sense, and structurally
  comparable via `equals`/`hashCode`. The value that comes out of `read()`
  is the same value that goes into `merge()` over the wire.

