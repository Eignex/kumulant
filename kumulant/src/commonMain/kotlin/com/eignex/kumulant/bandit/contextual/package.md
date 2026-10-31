# Package com.eignex.kumulant.bandit.contextual

Contextual bandits: each round comes with a feature vector, and the
reward depends on both the chosen arm and the context.
[RegressionContextualBandit] composes a regressor and posterior scoring rule
for linear, tree, or forest reward models.

## Picking a contextual model

| Bandit | Reward model | Reach for it when |
|--------|--------------|-------------------|
| [RegressionContextualBandit] | One regression stat per arm; scoring rule is a [com.eignex.kumulant.stat.regression.RegressionPosterior] | The relationship between context and reward is structurally linear / tree-shaped; anything modellable by a [com.eignex.kumulant.stat.regression.glm] or [com.eignex.kumulant.stat.regression.tree] regressor. |

## RegressionContextualBandit

Each arm owns a regressor (any
[com.eignex.kumulant.core.RegressionStat]) and a
[com.eignex.kumulant.stat.regression.RegressionPosterior] turns the
regressor's snapshot into a per-arm score at the round's context.

Common combinations:

- **Linear Thompson sampling**: `BayesianRegressionStat` per arm +
  [MultivariateGaussian][com.eignex.kumulant.stat.regression.glm.MultivariateGaussian]
  posterior.
- **LinUCB**: `BayesianRegressionStat` per arm +
  [LinUcb][com.eignex.kumulant.stat.regression.glm.LinUcb] posterior.
- **High-dimensional sparse**: `DiagonalRegressionStat` per arm +
  [FactorisedGaussian][com.eignex.kumulant.stat.regression.glm.FactorisedGaussian]
  posterior; trades the joint posterior for per-coordinate uncertainty.
- **Non-linear**: `RandomForestRegressionStat` per arm +
  `ThompsonTreePosterior` or `UcbTreePosterior`.
- **Cheap point estimates**: `StochasticRegressionStat` per arm +
  [PointPosterior][com.eignex.kumulant.stat.regression.glm.PointPosterior]
  (no exploration; pure greedy). Useful when paired with an explicit
  exploration policy like epsilon-greedy at a higher layer.

The bandit takes a `template` regressor at construction; each arm gets
its own materialized copy via `template.create(concurrency)`. Optional
`globalTemplate` pools across arms for shared structure / faster
warm-up. For explicit empirical-Bayes pooling, collect regression snapshots,
fit a prior with
[com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat.fitPopulationPrior],
and pass the resulting mean and covariance to new regressors.

## Wire portability

[ContextualBanditSpec] is the sealed root of wire-portable contextual
configs:

[RegressionContextualSpec] accepts the same
[com.eignex.kumulant.schema.spec.RegressionStatSpec] trees used in statistical
schemas, paired with a matching posterior. The result type ties the arm and
optional global regressors to their scorer in Kotlin. Materialization checks
built-in posterior compatibility for decoded configurations.

```kotlin
val spec: ContextualBanditSpec = RegressionContextualSpec(
    nbrArms = 4,
    regression = BayesianRegression(featureSize = 2)
        .filter(Y gt 0.0)
        .weightBy(V(0) * V(0)),
    posterior = LinUcb,
)
val encoded = SchemaJson.encodeToString(ContextualBanditSpec.serializer(), spec)
val decoded = SchemaJson.decodeFromString(ContextualBanditSpec.serializer(), encoded)
val bandit = decoded.materialize()
```

Use the native `BayesianRegression`, `DiagonalRegression`, and
`StochasticRegression` specs for linear models, and `DecisionTreeRegression`
or `RandomForestRegression` for tree models. Built-in linear, tree, and forest
posteriors serialize without a custom serializers module. Custom
`RegressionPosterior` implementations remain available for programmatic use.

Regression decorators retain their statistical-schema semantics: they modify
updates. Posteriors score snapshots at the supplied query vector; feature and
target transformations must be accounted for by the caller's scoring rule.
The native linear specs also expose link functions, optimizers, penalties, and
expression-based learning rates; use those fields instead of the former
`LinearRegressionSpec` variants and scalar `learningRate` field.

These round-trip through skema-based JSON / CBOR and materialise via
[com.eignex.kumulant.bandit.materialize] into the live bandit.

## Interface hierarchy

See [com.eignex.kumulant.bandit] for the cross-cutting interface story
(`Bandit` / `ContextualBandit` / `Snapshotable` / `PerArmBandit` /
`ContextualScorable`).
