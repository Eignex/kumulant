# Package com.eignex.kumulant.schema.ops

The composition operators for the wire specs in
[com.eignex.kumulant.schema]. Each operator is an extension on a
modality-specific spec that returns another spec, so compositions stay
pure data and round-trip on the wire exactly like the leaf specs they
wrap. Materialization builds the internal live adapters from the composed
spec; callers author compositions through this package.

## What you can compose

Input-side wrappers change what a stat sees: weighting and fixed-value injection,
windowing, sampling and throttling, lagging and feedback, and the
adapters that move a stat between modalities (a series stat reading the
x of a pair, a vector coordinate, and so on). Output-side wrappers change
what it reports: folding and projecting results, the scalers, and the
band around a center-scale result.

## AST-backed operators

The operators that take a projection or a predicate, such as filtering,
transforming, and weighting by a computed value, accept an expression
tree from [com.eignex.kumulant.schema.expr] rather than a live lambda, so
the whole composition still serializes. Live adapters compile these trees
into internal update closures during materialization.

## Named compositions and result projections

Any modality can give a complete recipe a portable name with [named]:

```kotlin
val recentPositiveMean = Mean.filter(X gt 0.0).windowed(60_000).named("RecentPositiveMean")
```

Its JSON carries both the name and the inner tree under `named`. A receiver can
materialize the recipe without a registry of named factories. Names do not change
input admission, result types, merge behavior, or concurrency. A name must not be blank.

Built-in result projections also travel in the recipe. [medianAbsoluteDeviation]
projects a t-digest into a `MadResult`, [covariance] projects a univariate regression
into a `CovarianceResult`, [plattCalibration] projects a one-feature logit model
into a `PlattCalibratorResult`, and [classificationAccuracy] projects a mean of
binary correctness scores into an `AccuracyResult`. Materialization checks the
inner result shape before admitting observations. Projections reconstruct the
underlying merge evidence; each result documents any evidence it omits.

[asPaired] adapts a regression recipe to scalar pairs through portable feature and
response expressions. For example, `model.asPaired(vectorOf(X))` passes one feature
and the pair's `Y` response to the model.

The `Mad`, `Covariance`, `PlattCalibrator`, and `Accuracy` factories use these
operators and return named recipes. Their JSON representation is the complete
composition rather than a dedicated leaf node. The constructor-shaped runtime
factories return the corresponding `SeriesStat` or `PairedStat` interface.
