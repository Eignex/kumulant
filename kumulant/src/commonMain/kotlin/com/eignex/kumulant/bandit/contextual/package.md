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
  `ThompsonForestPosterior` or `UcbForestPosterior`.
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

- [RegressionContextualSpec]: wraps a [LinearRegressionSpec]
  (Bayesian / Diagonal / Stochastic) and a posterior choice.

These round-trip through skema-based JSON / CBOR and materialise via
[com.eignex.kumulant.bandit.materialize] into the live bandit.

## Interface hierarchy

See [com.eignex.kumulant.bandit] for the cross-cutting interface story
(`Bandit` / `ContextualBandit` / `Snapshotable` / `PerArmBandit` /
`ContextualScorable`).
