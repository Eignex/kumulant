# Package com.eignex.kumulant.schema.optimizer

The wire-portable optimizer strategies that the online linear-model stats
take to pick their per-coordinate update rule. An [OptimizerSpec] is pure
configuration: it carries the learning-rate schedule and the optimizer's
hyperparameters, and materializes into a live optimizer in
[com.eignex.kumulant.stat.regression] when the stat is built. As with
every spec, the concurrency mode is supplied at materialize time rather
than stored on the wire.

## Choosing one

[Sgd] is plain stochastic gradient descent: the cheapest path, stateless
apart from the step counter, and the only optimizer that supports the
L1/L2 penalties on the regression stats. [Adam] is the general-purpose
default, giving each coordinate its own adaptive rate from bias-corrected
first and second moment estimates.

All four are consumed by
[com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat] and
[com.eignex.kumulant.stat.regression.SoftmaxRegressionStat]. A multi-output
stat builds one live optimizer per output class from the same spec, so the
configuration is shared while the per-coordinate state is not.
