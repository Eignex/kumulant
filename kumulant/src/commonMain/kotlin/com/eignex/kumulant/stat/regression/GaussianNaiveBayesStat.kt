package com.eignex.kumulant.stat.regression

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.DenseVector
import com.eignex.koblas.VectorView
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.StreamDoubleArray
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.exp
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
    val means: DenseMatrix,
    /** K-by-p matrix of per-class running variances (population, weight-normalised). */
    val variances: DenseMatrix,
    /** Cumulative observation weight per class; length [numClasses]. */
    val classWeights: DenseVector,
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
    fun logPosterior(x: VectorView, c: Int): Double {
        require(x.size == featureSize) { "x.size=${x.size}, expected $featureSize" }
        var s = ln(prior(c).coerceAtLeast(SMALL_PROB))
        for (i in 0 until featureSize) {
            val mu = means[c, i]
            val v = max(variances[c, i], varianceFloor)
            val d = x[i] - mu
            s += -0.5 * (LOG_TWO_PI + ln(v) + d * d / v)
        }
        return s
    }

    /** Normalised class probabilities via log-sum-exp on the log-posterior. */
    fun probabilities(x: VectorView): DoubleArray {
        val logs = DoubleArray(numClasses) { logPosterior(x, it) }
        var maxL = logs[0]
        for (k in 1 until numClasses) if (logs[k] > maxL) maxL = logs[k]
        var z = 0.0
        for (k in 0 until numClasses) {
            logs[k] = exp(logs[k] - maxL)
            z += logs[k]
        }
        if (z > 0.0) for (k in 0 until numClasses) logs[k] /= z
        return logs
    }

    /** Argmax class index for [x]. */
    fun predict(x: VectorView): Int {
        var best = 0
        var bestL = logPosterior(x, 0)
        for (k in 1 until numClasses) {
            val l = logPosterior(x, k)
            if (l > bestL) {
                bestL = l
                best = k
            }
        }
        return best
    }

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
        require(featureSize > 0) { "featureSize must be positive" }
        require(numClasses >= 2) { "numClasses must be >= 2; got $numClasses" }
        require(varianceFloor > 0.0) { "varianceFloor must be positive; got $varianceFloor" }
    }

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val meanCell: StreamDoubleArray = mode.newDoubleArray(numClasses * featureSize)
    private val m2Cell: StreamDoubleArray = mode.newDoubleArray(numClasses * featureSize)
    private val classWeightCell: StreamDoubleArray = mode.newDoubleArray(numClasses)
    private val totalWeightCell: StreamDouble = mode.newDouble(0.0)

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        require(x.size == featureSize) { "x.size=${x.size}, expected $featureSize" }
        if (weight <= 0.0 || weight.isNaN()) return
        lock.guarded {
            // toInt() truncates toward zero, so NaN and anything in (-1, 0) both became class 0 and
            // slipped past the range check as a valid label. Round-tripping through Double is what
            // rejects them: it demands an exact integer, which NaN fails alongside 1.5 and -0.5. The
            // tree classifiers already guard this; see Stat on why a label is not a value.
            val c = y.toInt()
            if (c.toDouble() != y || c !in 0 until numClasses) return@guarded
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
        val meansFlat = DoubleArray(numClasses * featureSize) { meanCell.load(it) }
        val varsFlat = DoubleArray(numClasses * featureSize) { idx ->
            val c = idx / featureSize
            val w = classWeightCell.load(c)
            if (w > 0.0) m2Cell.load(idx) / w else 0.0
        }
        val cw = DoubleArray(numClasses) { classWeightCell.load(it) }
        GaussianNaiveBayesResult(
            featureSize = featureSize,
            numClasses = numClasses,
            means = DenseMatrix.of(
                Array(numClasses) { k -> meansFlat.copyOfRange(k * featureSize, (k + 1) * featureSize) },
            ),
            variances = DenseMatrix.of(
                Array(numClasses) { k -> varsFlat.copyOfRange(k * featureSize, (k + 1) * featureSize) },
            ),
            classWeights = DenseVector.of(cw),
            totalWeights = totalWeightCell.load(),
            varianceFloor = varianceFloor,
        )
    }

    /**
     * Weight-pooled merge: combines per-class running means and M2 using Chan's
     * parallel-Welford formula. Exact under weighted updates.
     */
    override fun merge(values: GaussianNaiveBayesResult) {
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
