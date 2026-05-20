# Schemas and the wire

A StatSchema is a typed, named declaration of a bag of stats. It does
three things. It lets you read results back by StatKey instead of by
string. It materialises into a StatGroup that fans every update out to
every registered stat. And it round-trips on the wire as a StatSchemaDef
so a remote process can stand up the same bag, run it independently, and
ship snapshots back to merge.

## Declaring a schema

```kotlin
object Telemetry : StatSchema(concurrency = Concurrency.Strict) {
    val latencyMean by series(Mean)
    val latencyP99  by series(DDSketch(probabilities = listOf(0.99)))
    val errorRate   by series(Rate)
    val uniqueUsers by discrete(HyperLogLog(precision = 14))
}
```

The series, paired, vector, and discrete declarators register a StatSpec
of the matching modality and return a StatKey carrying the result type.
The delegate gives you a typed property; you read results by passing the
key back to the group's GroupResult. The group declarator nests a
sub-schema, whose entries materialise as their own StatGroup keyed by the
outer name.

## Specs

Every concrete stat has a sibling StatSpec: a data class (or data object
for parameter-less stats) carrying only configuration. Specs are
serializable with SerialName discriminators matching their Kotlin names,
so kotlinx.serialization polymorphism puts the same type strings on the
wire regardless of format.

```kotlin
val sumSpec: SeriesStatSpec<SumResult> = Sum
val sketchSpec = DDSketch(relativeError = 0.01, probabilities = listOf(0.5, 0.99))
val regSpec = UnivariateRegression(penalty = Penalty.Ols)
val hllSpec = HyperLogLog(precision = 14)
```

Construction lives separately from declaration. Calling
`spec.materialize(concurrency)` builds the live stat. The schema layer
calls this for you when you build a StatGroup from a schema.

Specs carry no Concurrency. The concurrency mode is a deployment knob
passed at materialize time, so the same wire payload can run at
Concurrency.None in a single-threaded test and Concurrency.Strict in a
contended hot loop.

## Composing specs

Every operation in [Operations](04-operations.md) has both a live and a
spec form. The lambda-bound operations (filter, transform, transformPair,
foldVector, foldPaired) take an AST on the spec side so the closure
travels as data:

```kotlin
import com.eignex.kumulant.schema.X
import com.eignex.kumulant.schema.gt

val positiveMean: SeriesStatSpec<WeightedMeanResult> =
    Mean.filter(X gt 0.0).withWeight(0.5).windowed(1.minutes)
```

The AST DSL covers comparison (gt, ge, lt, le, eq), boolean combinators
(and, or), and arithmetic on X, Y, V(index), and Const(v). Anything that
cannot be expressed in the AST stays live-only.

## Running a group

```kotlin
val group = StatGroup(Telemetry)
group.update(value = 12.7)
val results: GroupResult = group.read()
val p99 = results[Telemetry.latencyP99]
```

StatGroup is itself a SeriesStat over GroupResult. It can be nested
inside another stat, windowed, or merged with another group's
GroupResult.

For paired, vector, and discrete schemas there are PairedStatGroup,
VectorStatGroup, and DiscreteStatGroup variants. Each one fans updates
out only to entries of the matching modality.

## Shipping over the wire

The serialisable view of a schema is StatSchemaDef, a map from name to
StatSpec. The same view comes out of an authored schema via
`Telemetry.statSchemaDef()` and goes back into a live group via the
materialize extensions:

```kotlin
val def: StatSchemaDef = Telemetry.statSchemaDef()
val json = Json.encodeToString(def)
// ship `json`
val rehydrated: StatSchemaDef = Json.decodeFromString(json)
val live = rehydrated.materializeSeries(Concurrency.None)
```

The materializeSeries, materializePaired, materializeVector, and
materializeDiscrete variants enforce that every entry matches the
expected modality. The unfiltered materialize returns a list of bound
stats and leaves the caller to split.

## Merging across processes

Each Result is a serializable data class. A common pattern is to have
many workers run the same StatGroup, periodically call read on the
group, and ship the GroupResult (or its per-entry results) back to a
coordinator. The coordinator runs its own StatGroup of the same shape
and folds each worker's snapshot in with merge. Because merge takes a
Result rather than a live Stat, the boundary is serialisation-friendly
and the worker is free to terminate after each report.
