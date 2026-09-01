package com.eignex.kumulant.stat.regression

import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.asClassLabel
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.core.requireAtLeastTwoClasses
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.core.requirePositiveFeatureSize
import com.eignex.kumulant.math.argMaxOf
import com.eignex.kumulant.math.softmaxInPlace
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.StreamDoubleArray
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.ln
import kotlin.math.max

private const val LOG_TWO_PI: Double = 1.8378770664093453

/**
 * Snapshot from [GaussianNaiveBayesStat]: per-class feature statistics and class
 * priors. Each row of [means] and [variances] holds the running mean / variance
 * of every feature conditioned on a given class.
 *
 * Prediction uses the standard Gaussian-NB log-posterior:
 * `log p(c | x)  proportional to  log prior[c] - 0.5 * Sum_i [log(2 pi var) + (x_i - mu)^2 / var]`
 * with [varianceFloor] applied to each per-class variance to keep the log term finite.
 */
@Serializable
@SerialName("GaussianNaiveBayesResult")
data class GaussianNaiveBayesResult(
    /** Number of input features. */
    val featureSize: Int,
    /** Number of classes. */
    val numClasses: Int,
    /** K-by-p matrix of per-class running means; `means[c][i]` is the mean of feature `i` given class `c`. */
    val means: F64DenseMatrix,
    /** K-by-p matrix of per-class running variances (population, weight-normalised). */
    val variances: F64DenseMatrix,
    /** Cumulative observation weight per class; length [numClasses]. */
    val classWeights: F64DenseVector,
    /** Total cumulative observation weight across all classes. */
    override val totalWeights: Double,
    /** Lower bound applied to per-class variances at predict time. */
    val varianceFloor: Double,
) : HasObservationCount {

    init {
        require(means.rows == numClasses && means.cols == featureSize) {
            "means must be ${numClasses}x$featureSize; got ${means.rows}x${means.cols}"
        }
        require(variances.rows == numClasses && variances.cols == featureSize) {
            "variances must be ${numClasses}x$featureSize; got ${variances.rows}x${variances.cols}"
        }
        require(classWeights.size == numClasses) {
            "classWeights must have length $numClasses; got ${classWeights.size}"
        }
        require(varianceFloor > 0.0) { "varianceFloor must be positive; got $varianceFloor" }
    }

    /** Class prior, computed from accumulated class weights. */
    fun prior(c: Int): Double = if (totalWeights > 0.0) classWeights[c] / totalWeights else 1.0 / numClasses

    /** Unnormalised log-posterior `log prior[c] + Sum_i log N(x_i | mu_c, var_c)`. */
    fun logPosterior(x: F64VectorLike, c: Int): Double {
        x.requireFeatureSize(featureSize)
        var s = ln(prior(c).coerceAtLeast(SMALL_PROB))
        for (i in 0 until featureSize) {
            val mu = means[c, i]
            val v = max(variances[c, i], varianceFloor)
            val d = x[i] - mu
            s += -0.5 * (LOG_TWO_PI + ln(v) + d * d / v)
        }
        return s
    }

    /** Writes unnormalised log-posteriors for [x] into [destination]. */
    fun logPosteriorsInto(x: F64VectorLike, destination: DoubleArray) {
        x.requireFeatureSize(featureSize)
        require(destination.size == numClasses) {
            "destination size ${destination.size} must match numClasses $numClasses"
        }
        for (c in 0 until numClasses) destination[c] = logPosterior(x, c)
    }

    /** Normalised class probabilities via log-sum-exp on the log-posterior. */
    fun probabilities(x: F64VectorLike): DoubleArray = DoubleArray(numClasses).also { probabilitiesInto(x, it) }

    /** Writes normalised class probabilities for [x] into [destination]. */
    fun probabilitiesInto(x: F64VectorLike, destination: DoubleArray) {
        logPosteriorsInto(x, destination)
        destination.softmaxInPlace()
    }

    /** Argmax class index for [x]. */
    fun predict(x: F64VectorLike): Int = argMaxOf(numClasses) { k -> logPosterior(x, k) }

    private companion object {
        const val SMALL_PROB: Double = 1e-300
    }
}

/**
 * Online Gaussian Naive Bayes classifier. Tracks per-class, per-feature running
 * mean and variance via weighted Welford, plus per-class accumulated weight as
 * the prior. Predict-time log-likelihoods assume features are conditionally
 * independent within each class.
 *
 * **Use cases:** cheap multiclass classifier with calibrated probabilities,
 * useful as a baseline against [SoftmaxRegressionStat] or as a fallback for
 * sparse / high-cardinality feature spaces where SGD is slow to converge.
 *
 * **Memory:** O([numClasses] * [featureSize]); three flat cells per (class,
 * feature) pair (mean, M2, totalWeights), plus a per-class weight.
 *
 * **Update:** O([featureSize]) per observation (dense; sparse cost is the same
 * because variance updates need to compare against zero).
 *
 * **Concurrency:** Welford-locked under [Concurrency.Strict] / [Concurrency.HighWrite].
 * The per-class Welford state is coupled across cells, so [Concurrency.None]
 * skips synchronisation; [Concurrency.Relaxed] runs without the lock and may
 * drift across cells.
 */
class GaussianNaiveBayesStat(
    override val featureSize: Int,
    /** Number of classes; the input `y` must round to `[0, numClasses)`. */
    val numClasses: Int,
    /** Lower bound applied to per-class variances when computing log-likelihoods.
     *  Prevents `log(0)` blow-ups on early or constant-feature data. */
    val varianceFloor: Double = 1e-9,
    override val concurrency: Concurrency = Concurrency.None,
) : RegressionStat<GaussianNaiveBayesResult> {

    init {
        requirePositiveFeatureSize(featureSize)
        requireAtLeastTwoClasses(numClasses)
        require(varianceFloor > 0.0) { "varianceFloor must be positive; got $varianceFloor" }
    }

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val meanCell: StreamDoubleArray = mode.newDoubleArray(numClasses * featureSize)
    private val m2Cell: StreamDoubleArray = mode.newDoubleArray(numClasses * featureSize)
    private val classWeightCell: StreamDoubleArray = mode.newDoubleArray(numClasses)
    private val totalWeightCell: StreamDouble = mode.newDouble(0.0)

    override fun update(
        x: F64VectorLike,
        y: Double,
        timestampNanos: Long,
        weight: Double,
        workspace: com.eignex.koblas.Workspace?,
    ) {
        x.requireFeatureSize(featureSize)
        if (weight.isNotPositiveWeight()) return
        lock.guarded {
            // See asClassLabel on why an exact integer is required.
            val c = y.asClassLabel(numClasses)
            if (c < 0) return@guarded
            val priorW = classWeightCell.load(c)
            val newW = priorW + weight
            classWeightCell.store(c, newW)
            totalWeightCell.add(weight)
            for (i in 0 until featureSize) {
                val slot = c * featureSize + i
                val mean = meanCell.load(slot)
                val delta = x[i] - mean
                val nextMean = mean + (weight / newW) * delta
                meanCell.store(slot, nextMean)
                m2Cell.add(slot, weight * delta * (x[i] - nextMean))
            }
        }
    }

    override fun read(timestampNanos: Long): GaussianNaiveBayesResult = lock.guarded {
        val meansFlat = DoubleArray(numClasses * featureSize)
        val varsFlat = DoubleArray(numClasses * featureSize)
        for (c in 0 until numClasses) {
            val w = classWeightCell.load(c)
            for (i in 0 until featureSize) {
                val source = c * featureSize + i
                val destination = c + i * numClasses
                meansFlat[destination] = meanCell.load(source)
                varsFlat[destination] = if (w > 0.0) m2Cell.load(source) / w else 0.0
            }
        }
        val cw = DoubleArray(numClasses) { classWeightCell.load(it) }
        GaussianNaiveBayesResult(
            featureSize = featureSize,
            numClasses = numClasses,
            means = F64DenseMatrix.wrap(numClasses, featureSize, meansFlat),
            variances = F64DenseMatrix.wrap(numClasses, featureSize, varsFlat),
            classWeights = F64DenseVector.wrap(cw),
            totalWeights = totalWeightCell.load(),
            varianceFloor = varianceFloor,
        )
    }

    /**
     * Weight-pooled merge: combines per-class running means and M2 using Chan's
     * parallel-Welford formula. Exact under weighted updates.
     */
    override fun merge(values: GaussianNaiveBayesResult, workspace: com.eignex.koblas.Workspace?) {
        require(values.featureSize == featureSize && values.numClasses == numClasses) {
            "merge: shape mismatch (${values.numClasses}x${values.featureSize}) vs (${numClasses}x$featureSize)"
        }
        lock.guarded {
            for (c in 0 until numClasses) {
                val w1 = classWeightCell.load(c)
                val w2 = values.classWeights[c]
                val wNew = w1 + w2
                if (wNew <= 0.0) continue
                for (i in 0 until featureSize) {
                    val slot = c * featureSize + i
                    val mean1 = meanCell.load(slot)
                    val mean2 = values.means[c, i]
                    val m2Local = m2Cell.load(slot)
                    val m2Other = values.variances[c, i] * w2
                    val delta = mean2 - mean1
                    val combinedMean = (mean1 * w1 + mean2 * w2) / wNew
                    val combinedM2 = m2Local + m2Other + delta * delta * w1 * w2 / wNew
                    meanCell.store(slot, combinedMean)
                    m2Cell.store(slot, combinedM2)
                }
                classWeightCell.store(c, wNew)
            }
            totalWeightCell.add(values.totalWeights)
        }
    }

    override fun reset() = lock.guarded {
        for (i in 0 until numClasses * featureSize) {
            meanCell.store(i, 0.0)
            m2Cell.store(i, 0.0)
        }
        for (c in 0 until numClasses) classWeightCell.store(c, 0.0)
        totalWeightCell.store(0.0)
    }

    override fun create(concurrency: Concurrency?) =
        GaussianNaiveBayesStat(featureSize, numClasses, varianceFloor, concurrency ?: this.concurrency)
}
