package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.Covariance
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionResult
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat

/**
 * Online covariance and Pearson correlation between two streams.
 *
 * Derived from [UnivariateRegressionStat]: the same Chan's parallel algorithm drives
 * accumulation, and [CovarianceResult] is projected from [UnivariateRegressionResult]
 * by a serializable result projection.
 *
 * **Use cases:** monitoring joint variability and correlation between two
 * scalar streams (input metric vs output, feature drift detection).
 *
 * **Memory:** O(1); same as [UnivariateRegressionStat].
 *
 * **Update:** O(1) per paired observation.
 *
 * **Concurrency:** Inherits [UnivariateRegressionStat]'s concurrency model.
 */
@Suppress("FunctionNaming")
fun CovarianceStat(concurrency: Concurrency = Concurrency.None): PairedStat<CovarianceResult> =
    Covariance.materialize(concurrency)
