# Package com.eignex.kumulant.bandit.contextual

Contextual bandits: each round comes with a feature vector, and the
reward depends on both the chosen arm and the context. Three families,
covering linear / non-linear / non-parametric reward models plus the
adversarial-expert case.

## The three bandits

| Bandit | Reward model | Reach for it when |
|--------|--------------|-------------------|
| [RegressionContextualBandit] | One regression stat per arm; scoring rule is a [com.eignex.kumulant.stat.regression.RegressionPosterior] | The relationship between context and reward is structurally linear / tree-shaped; anything modellable by a [com.eignex.kumulant.stat.regression.glm] or [com.eignex.kumulant.stat.regression.tree] regressor. |
| [KnnContextualBandit] | Nearest-neighbours over a reservoir of `(context, reward)` pairs per arm | The reward surface is non-parametric and hard to model; kNN lets the data speak directly. Memory grows with the reservoir size; pick when the feature space is low-dimensional and observations are scarce. |
| [Exp4Bandit] | Adversarial expert mixture | The context is itself a set of expert recommendations (or a hand-crafted distribution over arms) and you want a regret bound that holds without distributional assumptions. |

## RegressionContextualBandit

The flagship contextual bandit. Each arm owns a regressor (any
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
  posterior; trades full covariance for per-coordinate uncertainty.
- **Non-linear**: `RandomForestRegressionStat` per arm +
  `ThompsonForestPosterior` or `UcbForestPosterior`.
- **Cheap point estimates**: `StochasticRegressionStat` per arm +
  [PointPosterior][com.eignex.kumulant.stat.regression.glm.PointPosterior]
  (no exploration; pure greedy). Useful when paired with an explicit
  exploration policy like epsilon-greedy at a higher layer.

The bandit takes a `template` regressor at construction; each arm gets
its own materialized copy via `template.create(concurrency)`. Optional
`globalRegression` pools across arms for shared structure / faster
warm-up; see also
[HierarchicalBayesianRegression][com.eignex.kumulant.stat.regression.glm.HierarchicalBayesianRegression]
for the explicit cross-arm pooling story.

## KnnContextualBandit

Per-arm reservoir of `(context, reward)` pairs. At choose time, the
bandit walks each arm's reservoir, finds the k nearest neighbours to
the round's context (by euclidean or cosine distance), and scores the
arm by the mean (or a quantile) of their rewards. Optional
exploration bonus.

Reach for it when:

- The feature space is small (kNN cost scales with reservoir × features).
- The reward surface is genuinely non-linear and hard to parameterise.
- Cold-start observations are scarce: kNN starts producing reasonable
  scores from a handful of samples per arm.

[KnnArmResult] carries the reservoir as a serializable snapshot, so
replicas can merge (reservoir union) and ship reservoirs across
processes.

## Exp4Bandit

EXP4; exponential weights with experts. The context isn't a feature
vector but a per-round distribution over arms supplied by each of K
experts. EXP4 maintains weights over experts (not arms) and selects an
arm by mixing the expert distributions according to expert weights.

[Exp4State] captures the expert weights; the bandit is the adversarial
counterpart of [com.eignex.kumulant.bandit.univariate.Exp3Bandit] for
the contextual case.

## Wire portability

[ContextualBanditSpec] is the sealed root of wire-portable contextual
configs:

- [RegressionContextualSpec]: wraps a [LinearRegressionSpec]
  (Bayesian / Diagonal / Stochastic) and a posterior choice.
- [KnnContextualSpec]: reservoir size, distance metric, k, optional
  exploration knob.

Both round-trip through skema-based JSON / CBOR and materialise via
[com.eignex.kumulant.bandit.materialize] into the live bandit.

## Interface hierarchy

See [com.eignex.kumulant.bandit] for the cross-cutting interface story
(`Bandit` / `ContextualBandit` / `Snapshotable` / `PerArmBandit` /
`ContextualScorable`).
