# Package com.eignex.kumulant.stat.regression.glm

Generalised linear models; the linear-predictor-plus-link family.
Every model in this package shares the shape `eta = bias + x . weights`
followed by `mu = link.invMean(eta)`; they differ in how the
posterior over `weights` is maintained.

## The link family

[Link] is the canonical GLM link. Three variants ship:

- [Link.Identity]: Gaussian regression. `mu = eta`. The default.
- [Link.Logit]: Bernoulli (binary) regression. `mu = sigmoid(eta)`.
  Combine with a stat that exposes `predict(x)` for a calibrated
  probability classifier; with a [com.eignex.kumulant.stat.calibration.PlattCalibratorStat]
  for the canonical post-hoc fix to another classifier's output.
- [Link.Log]: Poisson regression. `mu = exp(eta)`. The right choice
  for non-negative count targets.

All three are *canonical* links: the gradient simplifies to
`(mu - y) * x`. Non-canonical links are not exposed because that
shortcut no longer holds and the per-update cost grows. Add the link
itself if you need one (the curvature method is the only thing tightly
coupled to the Bayesian variants).

## Picking a model

| Stat | When to reach for it |
|------|----------------------|
| [UnivariateRegressionStat] | One scalar feature, one scalar response. OLS / ridge / lasso via [Penalty]. The cheapest entry; reach for it for "fit a line to a stream of (x, y) points." |
| [StochasticRegressionStat] | Online SGD with any [OptimizerSpec][com.eignex.kumulant.schema.optimizer.OptimizerSpec] (Sgd / Adagrad / RMSProp / Adam). Best when you want point estimates only and the per-update cost must stay small. |
| [DiagonalRegressionStat] | Factorised Gaussian posterior; per-coefficient precision without the full covariance matrix. The natural choice for high-dimensional features where the quadratic memory of full Bayesian regression is prohibitive. |
| [BayesianRegressionStat] | Full Gaussian posterior with covariance matrix and Cholesky factor. Closed-form under [Link.Identity]; online Laplace approximation under [Link.Logit] / [Link.Log]. Reach for it when downstream needs uncertainty quantification; Thompson sampling, LinUCB. |
| [HierarchicalBayesianRegression] | Pooled estimation across many parallel regressors. Use it when you have one regressor per arm in a bandit (or per group in any stratified problem) and want them to share strength. |

## Penalties

[Penalty.L1] and [Penalty.L2] are the standard regularisers. They
attach to [StochasticRegressionStat] (only) and use the existing
lazy-update tricks for sparse efficiency (Bottou multiplicative scaling
for L2, cumulative truncated gradient for L1). The constructor checks
that a non-`None` [Penalty] is paired with [com.eignex.kumulant.schema.optimizer.Sgd]
optimizers; pairing penalties with Adam / Adagrad / RMSProp is not
supported (folding regularisation into adaptive-method weight decay is
delicate and would surface as a different config).

[UnivariateRegressionStat] supports [Penalty.L1] / [Penalty.L2]
directly; the closed-form OLS / lasso / ridge math falls out of the
running covariance.

## Posteriors

[LinearPosterior] adapters turn a [LinearRegressionResult] into a
scalar score:

- [PointPosterior]: the deterministic point estimate `bias + x . weights`.
- [FactorisedGaussian]: Thompson sample / UCB from a [DiagonalRegressionResult].
- [MultivariateGaussian]: Thompson sample from a [CovarianceRegressionResult].
- [LinUcb]: UCB-style score `bias + alpha * sqrt(xT Sigma x)`.

The posteriors live here (not in `bandit/`) because they're properties
of the *model*, not the *bandit*. The bandit consumes them.

## Learning-rate schedules

[ConstantRate], [StepDecay], [ExponentialDecay], and friends in
`LearningRates.kt` are wire-portable
[com.eignex.kumulant.schema.expr.ScalarExpr] expressions that produce a
learning rate from the current step counter. Wrap them in
[com.eignex.kumulant.schema.optimizer.Sgd] (or any other [com.eignex.kumulant.schema.optimizer.OptimizerSpec])
to pass through the wire.

## Merge

[UnivariateRegressionStat] merges exactly via Chan-style parallel Welford on its
running covariance. [DiagonalRegressionStat] and [BayesianRegressionStat] merge
exactly by combining Gaussian posteriors: per-coordinate precision-weighted for
the diagonal, full precision add-and-downdate (with Cholesky recomputation) for
the full covariance. [StochasticRegressionStat] merges approximately; SGD keeps
no second-moment information, so the combine is a sample-weighted average of the
weight vectors. [HierarchicalBayesianRegression] does not merge; the manager
refits the population prior from its tracked instances instead.

## Concurrency

[UnivariateRegressionStat] is Welford-coupled: locked under
[com.eignex.kumulant.core.Concurrency.Strict] /
[com.eignex.kumulant.core.Concurrency.HighWrite], racing with bounded drift
under [com.eignex.kumulant.core.Concurrency.Relaxed]. [StochasticRegressionStat]
runs lock-free asynchronous SGD (Hogwild) under
[com.eignex.kumulant.core.Concurrency.Relaxed] and serialises the update body
under [com.eignex.kumulant.core.Concurrency.Strict] /
[com.eignex.kumulant.core.Concurrency.HighWrite]. [DiagonalRegressionStat] and
[BayesianRegressionStat] serialise the whole update under any concurrent level
(the coupled posterior update has no lock-free form); throughput is
contention-bound, so shard and merge for higher write rates.
[HierarchicalBayesianRegression] is a manager, not a hot-path stat; each tracked
[BayesianRegressionStat] honours its own level.
