package com.eignex.kumulant.stat.regression

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.DenseVector
import com.eignex.koblas.VectorView
import com.eignex.koblas.forEachStored
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.asClassLabel
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.core.requireFeatureSize
import com.eignex.kumulant.math.softmaxInPlace
import com.eignex.kumulant.schema.optimizer.OptimizerSpec
import com.eignex.kumulant.schema.optimizer.Sgd
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.StreamDoubleArray
import com.eignex.kumulant.stream.getValue
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
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
    override val totalWeights: Double,
    /** Number of `update` calls absorbed. */
    val step: Long,
    /** Accumulated weighted negative log-likelihood (cross-entropy) over the stream. */
    val crossEntropy: Double,
) : HasObservationCount {
    init {
        require(weights.rows == numClasses && weights.cols == featureSize) {
            "weights must be ${numClasses}x$featureSize; got ${weights.rows}x${weights.cols}"
        }
        require(biases.size == numClasses) { "biases must have length $numClasses; got ${biases.size}" }
    }

    /** Linear predictor for class [k]: `biases[k] + weights[k] . x`. */
    fun logit(x: VectorView, k: Int): Double {
        x.requireFeatureSize(featureSize)
        var s = biases[k]
        for (i in 0 until featureSize) s += weights[k, i] * x[i]
        return s
    }

    /** Softmax probabilities across all classes for [x]; length [numClasses]. */
    fun probabilities(x: VectorView): DoubleArray {
        val etas = DoubleArray(numClasses) { logit(x, it) }
        etas.softmaxInPlace()
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
 * softmax cross-entropy loss. Generalises [com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat]
 * with `Link.Logit` from binary to K-way classification.
 *
 * Update step per observation (true class `c = y.toInt()`):
 *  ```
 *  p[k]      = softmax(W[k] . x + b[k])
 *  grad[k]i  = (p[k] - 1[k == c]) * x[i]    // per-coordinate gradient
 *  W[k]i    += optimizer.computeDelta(k, i, grad[k]i, weight)
 *  b[k]     += biasOptimizer.computeDelta(k, p[k] - 1[k == c], weight)
 *  ```
 *
 * One [OptimizerSpec] is materialised per class for the weight matrix; bias
 * is a single optimizer over `numClasses` slots.
 *
 * **Memory:** O([numClasses] * [featureSize]) for weights + per-optimizer aux state.
 * **Update:** O([numClasses] * nnz(x)) per observation.
 * **Concurrency:** Welford-locked; the optimizer aux state honours the same
 * [Concurrency] passed in.
 */
class SoftmaxRegressionStat(
    override val featureSize: Int,
    /** Number of classes; the input `y` must round to `[0, numClasses)`. */
    val numClasses: Int,
    /** Per-class weight-matrix optimizer; one instance is materialised per class. */
    val optimizer: OptimizerSpec = Sgd(),
    /** Bias optimizer, materialised once over `numClasses` slots. Defaults to [optimizer]. */
    val biasOptimizer: OptimizerSpec = optimizer,
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

    private val weightOptimizers: Array<Optimizer> = Array(numClasses) {
        optimizer.materialize(featureSize, concurrency)
    }
    private val biasOpt: Optimizer = biasOptimizer.materialize(numClasses, concurrency)

    /** Live view of the cumulative observation weight folded in. */
    val totalWeights: Double by totalWeightsCell

    /** Live view of the per-observation step counter. */
    val step: Long by stepCell

    /** Live view of the accumulated weighted cross-entropy. */
    val crossEntropy: Double by crossEntropyCell

    override fun update(x: VectorView, y: Double, timestampNanos: Long, weight: Double) {
        x.requireFeatureSize(featureSize)
        if (weight.isNotPositiveWeight()) return
        lock.guarded {
            // See asClassLabel on why an exact integer is required. The tree classifiers used to
            // truncate instead, so this stat and those disagreed about y = 1.5.
            val c = y.asClassLabel(numClasses)
            if (c < 0) return@guarded
            stepCell.addAndGet(1L)

            // Softmax over current logits.
            val etas = DoubleArray(numClasses)
            for (k in 0 until numClasses) {
                var dot = biasCell.load(k)
                x.forEachStored { i, v -> dot += weightsCell.load(k * featureSize + i) * v }
                etas[k] = dot
            }
            // Probabilities live in etas[] from here on. A false return means every exponential
            // underflowed, so there is no distribution to take a gradient against.
            if (!etas.softmaxInPlace()) return@guarded
            val logProbC = ln(etas[c].coerceAtLeast(SOFTMAX_EPS))
            crossEntropyCell.add(-logProbC * weight)

            biasOpt.advance()
            for (k in 0 until numClasses) weightOptimizers[k].advance()

            for (k in 0 until numClasses) {
                val target = if (k == c) 1.0 else 0.0
                val dEta = etas[k] - target
                val opt = weightOptimizers[k]
                x.forEachStored { i, v ->
                    val grad = dEta * v
                    weightsCell.add(k * featureSize + i, opt.computeDelta(i, grad, weight))
                }
                biasCell.add(k, biasOpt.computeDelta(k, dEta, weight))
            }
            totalWeightsCell.add(weight)
        }
    }

    override fun read(timestampNanos: Long): SoftmaxRegressionResult = lock.guarded {
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
        lock.guarded {
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

    override fun reset() = lock.guarded {
        for (i in 0 until numClasses * featureSize) weightsCell.store(i, 0.0)
        for (k in 0 until numClasses) biasCell.store(k, 0.0)
        totalWeightsCell.store(0.0)
        stepCell.store(0L)
        crossEntropyCell.store(0.0)
        for (opt in weightOptimizers) opt.reset()
        biasOpt.reset()
    }

    override fun create(concurrency: Concurrency?) =
        SoftmaxRegressionStat(featureSize, numClasses, optimizer, biasOptimizer, concurrency ?: this.concurrency)

    private companion object {
        const val SOFTMAX_EPS = 1e-15
    }
}
