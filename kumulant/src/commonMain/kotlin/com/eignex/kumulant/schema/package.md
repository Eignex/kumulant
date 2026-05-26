# Package com.eignex.kumulant.schema

Typed, named, wire-portable schemas for declaring bags of stats. A
[StatSchema] does three things:

1. Lets you read results back by typed [StatKey] instead of by string.
2. Materialises into a [StatGroup] that fans every update out to every
   registered stat.
3. Round-trips on the wire as a `StatSchemaDef` so a remote process can
   stand up the same bag, run it independently, and ship snapshots back
   to merge.

## Declaring a schema

```kotlin
val telemetry = object : StatSchema(concurrency = Concurrency.Strict) {
    val latencyMean by series(Mean)
    val errorRate by series(Rate)
}
val group = StatGroup(telemetry)
group.update(42.0)
val results = group.read()
println(results[telemetry.latencyMean].mean)
```

The `series`, `paired`, `vector`, and `discrete` declarators register a
[StatSpec] of the matching modality and return a [StatKey] carrying the
result type. The delegate gives you a typed property; you read results by
passing the key back to the group's `GroupResult`. The `group` declarator
nests a sub-schema, whose entries materialise as their own [StatGroup]
keyed by the outer name.

## Specs

Every concrete stat has a sibling [StatSpec]: a data class (or
`data object` for parameter-less stats) carrying only configuration.
Specs are `@Serializable` with `@SerialName` discriminators matching the
Kotlin class names, so polymorphic serialization puts the same type
strings on the wire regardless of format (JSON, CBOR, Protobuf).

Construction lives separately from declaration. Calling
`spec.materialize(concurrency)` builds the live stat. The schema layer
calls this for you when you build a [StatGroup] from a schema.

Specs carry no [Concurrency][com.eignex.kumulant.core.Concurrency]. The
concurrency mode is a deployment knob passed at materialize time, so
the same wire payload can run at `Concurrency.None` in a single-threaded
test and `Concurrency.Strict` in a contended hot loop.

## Composing specs

Every operation in [com.eignex.kumulant.operation] has a spec form. The
lambda-bound operations (`filter`, `transform`, `transformPair`,
`foldVector`, `foldPaired`) take an AST on the spec side so the
projection / predicate travels as data:

```kotlin
val positiveMean = Mean.filter(X gt 0.0).withWeight(0.5).windowed(1.minutes)
```

The AST DSL covers comparison (`gt`, `ge`, `lt`, `le`, `eq`), boolean
combinators (`and`, `or`), and arithmetic on `X`, `Y`, `V(index)`, and
`Const(v)`. Sugar nodes such as `Switch`, `In`, `Standardize`, and
`MinMax` make per-feature projection AST trees readable. Anything that
cannot be expressed in the AST stays live-only.

## Running a group

`StatGroup` is itself a [com.eignex.kumulant.core.SeriesStat] over
`GroupResult`. It can be nested inside another stat, windowed, or merged
with another group's `GroupResult`. `PairedStatGroup`,
`VectorStatGroup`, and `DiscreteStatGroup` variants fan updates only to
entries of the matching modality.

## Shipping over the wire

```kotlin
val spec: StatSpec = Mean
val json = SchemaJson.encodeToString(spec)
val decoded = SchemaJson.decodeFromString<StatSpec>(json)
val live = (decoded as SeriesStatSpec<WeightedMeanResult>).materialize(Concurrency.None)
live.update(1.0)
```

The `materializeSeries`, `materializePaired`, `materializeVector`, and
`materializeDiscrete` variants enforce that every entry matches the
expected modality. The unfiltered `materialize` returns a list of bound
stats and leaves the caller to split.

## Merging across processes

Each [Result][com.eignex.kumulant.core.Result] is a serializable data
class. A common pattern is to have many workers run the same
[StatGroup], periodically call `read` on the group, and ship the
`GroupResult` (or its per-entry results) back to a coordinator. The
coordinator runs its own [StatGroup] of the same shape and folds each
worker's snapshot in with `merge`. Because `merge` takes a [Result]
rather than a live [com.eignex.kumulant.core.Stat], the boundary is
serialisation-friendly and the worker is free to terminate after each
report.

## Optimizers

Linear-model stats ([com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat],
[com.eignex.kumulant.stat.regression.SoftmaxRegressionStat]) take an
[OptimizerSpec] that materialises into an
[com.eignex.kumulant.stat.regression.Optimizer]. The wire variants are
[Sgd], [Adagrad], [Rmsprop], and [Adam].
