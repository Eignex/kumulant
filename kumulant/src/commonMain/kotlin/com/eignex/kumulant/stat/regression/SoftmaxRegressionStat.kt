package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.DenseMatrix
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.math.forEachStored
import com.eignex.kumulant.schema.ScalarExpr
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.StreamDoubleArray
import com.eignex.kumulant.stream.getValue
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.exp
import kotlin.math.ln

/**
 * Snapshot from [SoftmaxRegressionStat]: per-class linear-model parameters plus
 * cumulative bookkeeping. The K-by-p [weights] matrix and length-K [biases] vector
 * define the linear predictors `eta[k] = biases[k] + weights[k] . x`; the predicted
 * class probability is the softmax over the K logits.
 */
@Serializable
@SerialName("SoftmaxRegressionResult")
data class SoftmaxRegressionResult(
    /** Number of input features (columns of [weights]). */
    val featureSize: Int,
    /** Number of classes (rows of [weights] and length of [biases]). */
    val numClasses: Int,
    /** K-by-p weight matrix; `weights[k][i]` is the coefficient on feature `i` for class `k`. */
    val weights: DenseMatrix,
    /** Per-class intercept; length [numClasses]. */
    val biases: DenseVector,
    /** Cumulative observation weight folded in. */
    val totalWeights: Double,
    /** Number of `update` calls absorbed. */
    val step: Long,
    /** Accumulated weighted negative log-likelihood (cross-entropy) over the stream. */
    val crossEntropy: Double,
) : Result {
    init {
        require(weights.rows == numClasses && weights.cols == featureSize) {
            "weights must be ${numClasses}x$featureSize; got ${weights.rows}x${weights.cols}"
        }
        require(biases.size == numClasses) { "biases must have length $numClasses; got ${biases.size}" }
    }

    /** Linear predictor for class [k]: `biases[k] + weights[k] . x`. */
    fun logit(x: VectorView, k: Int): Double {
        require(x.size == featureSize) { "x.size=${x.size}, expected $featureSize" }
        var s = biases[k]
        for (i in 0 until featureSize) s += weights[k, i] * x[i]
        return s
    }

    /** Softmax probabilities across all classes for [x]; length [numClasses]. */
    fun probabilities(x: VectorView): DoubleArray {
        val etas = DoubleArray(numClasses) { logit(x, it) }
        var maxEta = etas[0]
        for (k in 1 until numClasses) if (etas[k] > maxEta) maxEta = etas[k]
        var z = 0.0
        for (k in 0 until numClasses) {
            etas[k] = exp(etas[k] - maxEta)
            z += etas[k]
        }
        if (z > 0.0) for (k in 0 until numClasses) etas[k] /= z
        return etas
    }

    /** Argmax class index for [x]. */
    fun predict(x: VectorView): Int {
        var best = 0
        var bestEta = logit(x, 0)
        for (k in 1 until numClasses) {
            val e = logit(x, k)
            if (e > bestEta) {
                bestEta = e
                best = k
            }
        }
        return best
    }
}

/**
 * Online multinomial logistic regression by stochastic gradient descent on the
 * softmax cross-entropy loss. Generalises [StochasticRegressionStat] with
 * [Link.Logit] from binary to K-way classification.
 *
 * Per observation (true class `c = y.toInt()`, learning rate `eta`):
 *  ```
 *  p[k]      = softmax(W[k] . x + b[k])
 *  grad[k]   = (p[k] - 1[k == c]) * x
 *  W[k]     -= eta * weight * grad[k]
 *  b[k]     -= eta * weight * (p[k] - 1[k == c])
 *  ```
 *
 * Updates are sparse over the nonzeros of `x`. Penalties and richer optimisers
 * are out of scope here; reach for [DiagonalRegressionStat] or
 * [BayesianRegressionStat] for binary problems that need uncertainty.
 *
 * **Memory:** O([numClasses] * [featureSize]) — flat weight matrix plus per-class bias.
 *
 * **Update:** O([numClasses] * nnz(x)) per observation.
 *
 * **Concurrency:** Welford-locked under [Concurrency.Strict] / [Concurrency.HighWrite].
 * [Concurrency.Relaxed] runs lock-free but multi-cell updates may interleave
 * (HOGWILD-style); [Concurrency.None] runs without synchronisation.
 */
class SoftmaxRegressionStat(
    override val featureSize: Int,
    /** Number of classes; the input `y` must round to `[0, numClasses)`. */
    val numClasses: Int,
    /** Per-step learning-rate schedule applied to coefficient and bias updates. */
    val learningRate: ScalarExpr = ConstantRate(1e-2),
    override val concurrency: Concurrency = Concurrency.None,
) : RegressionStat<SoftmaxRegressionResult> {

    init {
        require(featureSize > 0) { "featureSize must be positive" }
        require(numClasses >= 2) { "numClasses must be >= 2; got $numClasses" }
    }

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()

    // Flat row-major K x p layout: weightsCell[k * p + i].
    private val weightsCell: StreamDoubleArray = mode.newDoubleArray(numClasses * featureSize)
    private val biasCell: StreamDoubleArray = mode.newDoubleArray(numClasses)
    private val totalWeightsCell: StreamDouble = mode.newDouble(0.0)
    private val stepCell = mode.newLong(0L)
    private val crossEntropyCell: StreamDouble = mode.newDouble(0.0)

    /** Live view of the cumulative observation weight folded in. */
    val totalWeights: Double by totalWeightsCell

    /** Live view of the per-observation step counter. */
    val step: Long by stepCell

    /** Live view of the accumulated weighted cross-entropy. */
    val crossEntropy: Double by crossEntropyCell

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) = lock.withLock {
        require(x.size == featureSize) { "x.size=${x.size}, expected $featureSize" }
        if (weight <= 0.0) return@withLock
        val c = y.toInt()
        if (c !in 0 until numClasses) return@withLock
        val s = stepCell.addAndGet(1L)
        val eta = learningRate.eval(s.toDouble())

        // Softmax over current logits.
        val etas = DoubleArray(numClasses)
        for (k in 0 until numClasses) {
            var dot = biasCell.load(k)
            x.forEachStored { i, v -> dot += weightsCell.load(k * featureSize + i) * v }
            etas[k] = dot
        }
        var maxEta = etas[0]
        for (k in 1 until numClasses) if (etas[k] > maxEta) maxEta = etas[k]
        var z = 0.0
        for (k in 0 until numClasses) {
            etas[k] = exp(etas[k] - maxEta)
            z += etas[k]
        }
        if (z <= 0.0) return@withLock
        val invZ = 1.0 / z
        // Probabilities live in etas[] now.
        for (k in 0 until numClasses) etas[k] *= invZ
        val logProbC = ln(etas[c].coerceAtLeast(SOFTMAX_EPS))
        crossEntropyCell.add(-logProbC * weight)

        for (k in 0 until numClasses) {
            val target = if (k == c) 1.0 else 0.0
            val dEta = etas[k] - target
            val coeff = -eta * weight * dEta
            x.forEachStored { i, v -> weightsCell.add(k * featureSize + i, coeff * v) }
            biasCell.add(k, coeff)
        }
        totalWeightsCell.add(weight)
    }

    override fun read(timestampNanos: Long): SoftmaxRegressionResult = lock.withLock {
        val w = DoubleArray(numClasses * featureSize) { weightsCell.load(it) }
        val b = DoubleArray(numClasses) { biasCell.load(it) }
        SoftmaxRegressionResult(
            featureSize = featureSize,
            numClasses = numClasses,
            weights = DenseMatrix.of(Array(numClasses) { k -> w.copyOfRange(k * featureSize, (k + 1) * featureSize) }),
            biases = DenseVector.of(b),
            totalWeights = totalWeightsCell.load(),
            step = stepCell.load(),
            crossEntropy = crossEntropyCell.load(),
        )
    }

    override fun merge(values: SoftmaxRegressionResult) {
        require(values.featureSize == featureSize && values.numClasses == numClasses) {
            "merge: shape mismatch (${values.numClasses}x${values.featureSize}) vs (${numClasses}x$featureSize)"
        }
        lock.withLock {
            val w1 = totalWeightsCell.load()
            val w2 = values.totalWeights
            val wNew = w1 + w2
            if (wNew > 0.0) {
                for (k in 0 until numClasses) {
                    for (i in 0 until featureSize) {
                        val mine = weightsCell.load(k * featureSize + i)
                        val theirs = values.weights[k, i]
                        weightsCell.store(k * featureSize + i, (mine * w1 + theirs * w2) / wNew)
                    }
                    biasCell.store(k, (biasCell.load(k) * w1 + values.biases[k] * w2) / wNew)
                }
            }
            totalWeightsCell.store(wNew)
            stepCell.add(values.step)
            crossEntropyCell.add(values.crossEntropy)
        }
    }

    override fun reset() = lock.withLock {
        for (i in 0 until numClasses * featureSize) weightsCell.store(i, 0.0)
        for (k in 0 until numClasses) biasCell.store(k, 0.0)
        totalWeightsCell.store(0.0)
        stepCell.store(0L)
        crossEntropyCell.store(0.0)
    }

    override fun create(concurrency: Concurrency?) =
        SoftmaxRegressionStat(featureSize, numClasses, learningRate, concurrency ?: this.concurrency)

    private companion object {
        const val SOFTMAX_EPS = 1e-15
    }
}
