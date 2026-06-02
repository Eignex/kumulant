# Package com.eignex.kumulant.stat.regression

Family root for the regression-modality stats and the cross-cutting
infrastructure they share. The single-output linear-model family lives
in [glm][com.eignex.kumulant.stat.regression.glm], the decision-tree
and random-forest family in
[tree][com.eignex.kumulant.stat.regression.tree].
What sits directly in this package is the small set of stats that don't
fit either subfamily, plus the strategy types both rely on.

## What's directly in this package

| Stat | Modality | Role |
|------|----------|------|
| [CovarianceStat] | Paired | Weighted covariance and Pearson correlation between two streams. The building block for both univariate and multivariate regressions. |
| [SoftmaxRegressionStat] | Regression | Online multinomial logistic regression; K-way classification via softmax cross-entropy. Generalises the GLM with `Link.Logit` from binary to K-way. |
| [GaussianNaiveBayesStat] | Regression | Per-class running Welford mean / variance of each feature plus class priors. The cheap non-parametric multiclass classifier. |

Softmax and Naive Bayes implement [com.eignex.kumulant.core.RegressionStat]
because that interface gives them `update(VectorView, Double)`; the
scalar `y` is the class index. Strictly they are classifiers, not
regressors, but they share the input shape and the result is consumed
the same way (a posterior over classes plus calibration / accuracy
metrics).

## Cross-cutting types

### Optimizer strategy

[Optimizer] is the per-coordinate update rule that single-output and
multinomial linear models share. The runtime API has four
implementations:

- [SgdOptimizer]: stateless; `delta = -lr * weight * gradient`.
- [AdagradOptimizer]: accumulates squared gradients per coordinate.
- [RmspropOptimizer]: exponential moving average of squared gradients.
- [AdamOptimizer]: bias-corrected first and second moments
  (Kingma & Ba 2015).

Wire-portable counterparts live in
[com.eignex.kumulant.schema.optimizer.OptimizerSpec]:
[com.eignex.kumulant.schema.optimizer.Sgd], [com.eignex.kumulant.schema.optimizer.Adagrad],
[com.eignex.kumulant.schema.optimizer.Rmsprop], [com.eignex.kumulant.schema.optimizer.Adam].
Stats accept the spec and materialise their own optimizer instances
per-feature-set; per-coordinate aux state honours the stat's
[com.eignex.kumulant.core.Concurrency] level.

### Posteriors

[RegressionPosterior] is the scoring interface shared by every
regression family. A posterior projects a [com.eignex.kumulant.core.Result]
and a query [com.eignex.kumulant.math.VectorView] to a scalar score,
parametrised by an `exploration` knob and a `Random`. The contextual
bandits ([com.eignex.kumulant.bandit.contextual.RegressionContextualBandit])
consume posteriors at choose time.

Concrete posteriors live with their model families:

- [com.eignex.kumulant.stat.regression.glm.LinearPosterior] for linear
  models: [PointPosterior][com.eignex.kumulant.stat.regression.glm.PointPosterior],
  [FactorisedGaussian][com.eignex.kumulant.stat.regression.glm.FactorisedGaussian],
  [MultivariateGaussian][com.eignex.kumulant.stat.regression.glm.MultivariateGaussian],
  [LinUcb][com.eignex.kumulant.stat.regression.glm.LinUcb].
- `TreePosterior` and `ForestPosterior` for trees: see
  [com.eignex.kumulant.stat.regression.tree].

The posterior interface is intentionally minimal so a downstream
contextual bandit can mix-and-match: one bandit might score arms with a
GLM under [LinUcb][com.eignex.kumulant.stat.regression.glm.LinUcb] and
another with a forest under
`ThompsonForestPosterior`: same code path, different model and
scoring rule.

## When to reach into which subfamily

- Need a linear model? Use [glm][com.eignex.kumulant.stat.regression.glm].
  Pick by required output:
  - Point estimates only, fastest path → `StochasticRegressionStat` with
    [com.eignex.kumulant.schema.optimizer.Sgd] or [com.eignex.kumulant.schema.optimizer.Adam].
  - Per-coordinate uncertainty → `DiagonalRegressionStat`.
  - Full posterior covariance for Thompson sampling / LinUCB →
    `BayesianRegressionStat`.
  - Pooled estimation across many parallel regressors →
    `HierarchicalBayesianRegression`.
- Need a non-linear regressor? Use
  [tree][com.eignex.kumulant.stat.regression.tree]. Single tree for
  cheap; forest for ensembled variance estimates (the natural backbone
  for a tree-based contextual bandit).
- Need K-way classification? [SoftmaxRegressionStat] (parametric,
  online SGD, scales to high dimensions) or [GaussianNaiveBayesStat]
  (non-parametric, cheap, no convergence concerns).
- Need calibrated probabilities? Wrap any of the above's output through
  [com.eignex.kumulant.stat.calibration].

## Merge

Of the stats living directly here, [CovarianceStat] and [GaussianNaiveBayesStat]
merge exactly (Chan-style parallel Welford on the running covariance and on the
per-class moments); [SoftmaxRegressionStat] merges approximately via a
sample-weighted blend of the per-class weight vectors, the usual SGD limitation.
The [glm][com.eignex.kumulant.stat.regression.glm] and
[tree][com.eignex.kumulant.stat.regression.tree] subpackages document their own
merge stories.

## Concurrency

[CovarianceStat] inherits
[com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat]'s
Welford-coupled model: locked under [com.eignex.kumulant.core.Concurrency.Strict]
/ [com.eignex.kumulant.core.Concurrency.HighWrite], racing under
[com.eignex.kumulant.core.Concurrency.Relaxed]. [SoftmaxRegressionStat] and
[GaussianNaiveBayesStat] serialise the update body under
[com.eignex.kumulant.core.Concurrency.Strict] /
[com.eignex.kumulant.core.Concurrency.HighWrite]; under
[com.eignex.kumulant.core.Concurrency.Relaxed] the per-class cells race with
bounded drift. See the subpackages for the glm and tree concurrency designs.
