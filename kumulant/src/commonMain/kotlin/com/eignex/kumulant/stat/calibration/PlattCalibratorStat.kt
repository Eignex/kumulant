package com.eignex.kumulant.stat.calibration

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.schema.optimizer.OptimizerSpec
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.PlattCalibrator
import com.eignex.kumulant.stat.StatDefaults
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat

/**
 * Online Platt scaling: fits a one-feature logistic regression
 * `sigmoid(slope * rawScore + intercept)` over paired
 * `(rawScore, label)` observations where label is in `{0, 1}`. Use to fix the
 * calibration of a classifier whose probability output is poorly aligned with
 * the empirical positive rate.
 *
 * Internally wraps a [StochasticRegressionStat] with `featureSize = 1` and
 * `Link.Logit`. The runtime cost matches a one-dimensional GLM-SGD update per
 * observation.
 *
 * Merge blends sample-weighted parameters and retains the optimizer step for its
 * learning-rate schedule. Residual sums of squares and optimizer moments are omitted.
 *
 * **Use cases:** post-hoc calibration of tree / SVM / naive-Bayes outputs;
 * pair with [ReliabilityStat] to monitor the gap before and after applying
 * the learned mapping.
 *
 * **Memory:** O(1); backed by [StochasticRegressionStat].
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Inherits [StochasticRegressionStat]'s concurrency model.
 */
@Suppress("FunctionNaming")
fun PlattCalibratorStat(
    optimizer: OptimizerSpec = StatDefaults.plattOptimizer(),
    concurrency: Concurrency = Concurrency.None,
): PairedStat<PlattCalibratorResult> = PlattCalibrator(optimizer).materialize(concurrency)
