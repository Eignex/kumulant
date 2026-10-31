package com.eignex.kumulant.stat

import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.schema.optimizer.OptimizerSpec
import com.eignex.kumulant.schema.optimizer.Sgd
import com.eignex.kumulant.stat.forecast.SeasonalMode
import com.eignex.kumulant.stat.regression.glm.ConstantRate
import com.eignex.kumulant.stat.regression.glm.Link
import com.eignex.kumulant.stat.regression.glm.Penalty

internal object StatDefaults {
    const val TREE_RANDOM_SEED: Int = 0
    const val FOREST_SIZE: Int = 10
    const val FOREST_BAGGING: Boolean = true
    const val DIAGONAL_REGRESSION_PRIOR_PRECISION: Double = 1.0
    val regressionPenalty: Penalty = Penalty.None
    val regressionLink: Link = Link.Identity
    const val GAUSSIAN_NAIVE_BAYES_VARIANCE_FLOOR: Double = 1e-9
    const val BAYESIAN_REGRESSION_PRIOR_VARIANCE: Double = 1.0
    const val MIN_HASH_NUM_HASHES: Int = 128
    const val MIN_HASH_SEED: Long = -3724518991637283867L
    const val COUNT_MIN_SKETCH_DEPTH: Int = 5
    const val COUNT_MIN_SKETCH_WIDTH: Int = 1024
    const val COUNT_MIN_SKETCH_SEED: Long = -7046029254386353133L
    const val BLOOM_FILTER_BITS: Int = 1 shl 16
    const val BLOOM_FILTER_HASHES: Int = 7
    const val HYPER_LOG_LOG_PRECISION: Int = 14
    const val AUC_NUM_BINS: Int = 256
    const val AUC_LOWER_BOUND: Double = 0.0
    const val AUC_UPPER_BOUND: Double = 1.0
    const val TDIGEST_COMPRESSION: Double = 100.0
    const val RESERVOIR_HISTOGRAM_CAPACITY: Int = 1024
    const val HDR_HISTOGRAM_LOWEST_DISCERNIBLE_VALUE: Double = 0.001
    const val HDR_HISTOGRAM_INITIAL_HIGHEST_TRACKABLE_VALUE: Double = 100.0
    const val HDR_HISTOGRAM_SIGNIFICANT_DIGITS: Int = 3
    const val FRUGAL_QUANTILE_STEP_SIZE: Double = 0.01
    const val FRUGAL_QUANTILE_INITIAL_ESTIMATE: Double = 0.0
    const val DD_SKETCH_RELATIVE_ERROR: Double = 0.01
    const val DD_SKETCH_MIN_INDEXABLE_VALUE: Double = 1e-9
    const val DD_SKETCH_MAX_INDEXABLE_VALUE: Double = 1e12
    const val COUNTER_RATE_TREAT_DECREASE_AS_RESET: Boolean = true
    const val ADWIN_DELTA: Double = 0.002
    const val ADWIN_MAX_BUCKETS_PER_SIZE: Int = 5
    const val PAGE_HINKLEY_DELTA: Double = 0.005
    const val PAGE_HINKLEY_THRESHOLD: Double = 50.0
    const val CUSUM_TARGET: Double = 0.0
    const val CUSUM_REFERENCE_VALUE: Double = 0.5
    const val CUSUM_THRESHOLD: Double = 5.0
    val seasonalMode: SeasonalMode = SeasonalMode.Additive
    const val SMOOTHING_PHI: Double = 1.0
    const val SMOOTHING_PERIOD: Int = 1

    fun diagonalLearningRate(): ScalarExpr = ConstantRate(1.0)
    fun regressionOptimizer(): OptimizerSpec = Sgd()
    fun plattOptimizer(): OptimizerSpec = Sgd(ConstantRate(1e-2))

    fun quantileProbabilities(): DoubleArray = doubleArrayOf(0.5, 0.75, 0.9, 0.95, 0.99, 0.999)
}
