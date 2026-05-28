package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.schema.ScalarExpr
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.StreamDoubleArray
import com.eignex.kumulant.stream.StreamLong
import com.eignex.kumulant.stream.getValue
import com.eignex.kumulant.stream.welfordMode
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * Online optimizer strategy. Given a per-coordinate raw gradient, returns the
 * delta to add to that weight cell. Owns any per-coordinate auxiliary state
 * (Adam's first/second moments, Adagrad's running squared gradient, etc.).
 *
 * Lifecycle, called by the host stat once per `update`:
 *  1. [advance]; bump per-update counters (Adam's step `t`).
 *  2. For each touched coordinate, [computeDelta]; return the weight delta.
 *  3. The host stat applies the delta to its weight cell.
 *
 * Stateless optimizers (`Sgd`) ignore [advance]. Concurrency: per-coordinate
 * aux state honours the [Concurrency] passed at materialization; multi-cell
 * coupled state (Adam) uses Welford locking semantics.
 */
sealed interface Optimizer {
    /** Number of weight coordinates this optimizer manages. */
    val featureSize: Int

    /** Advance per-update counters (Adam step `t`, etc.). Called once per stat `update`. */
    fun advance() {}

    /**
     * Per-coordinate update. Reads/writes any auxiliary state owned by the optimizer,
     * then returns the delta the caller should add to `w[coordIndex]`.
     */
    fun computeDelta(coordIndex: Int, gradient: Double, observationWeight: Double): Double

    /** Reset all internal state to its initial values. */
    fun reset()
}

/**
 * Plain SGD: `delta = -learningRate(step) * weight * gradient`. Stateless apart
 * from the global step counter feeding the schedule.
 */
class SgdOptimizer(
    override val featureSize: Int,
    /** Per-step learning-rate schedule. */
    val learningRate: ScalarExpr,
    concurrency: Concurrency = Concurrency.None,
) : Optimizer {

    private val mode = concurrency.welfordMode()
    private val stepCell: StreamLong = mode.newLong(0L)
    private val cachedLr: StreamDouble = mode.newDouble(0.0)

    /** Live view of the per-update step counter; drives the schedule. */
    val step: Long by stepCell

    override fun advance() {
        val t = stepCell.addAndGet(1L)
        cachedLr.store(learningRate.eval(t.toDouble()))
    }

    override fun computeDelta(coordIndex: Int, gradient: Double, observationWeight: Double): Double {
        val lr = cachedLr.load()
        return -lr * observationWeight * gradient
    }

    override fun reset() {
        stepCell.store(0L)
        cachedLr.store(0.0)
    }
}

/**
 * Adagrad: accumulates squared gradients per coordinate; the effective per-coord
 * learning rate is `lr / sqrt(sumG2[i] + epsilon)`. Adapts faster on rare features.
 */
class AdagradOptimizer(
    override val featureSize: Int,
    /** Base learning-rate schedule. */
    val learningRate: ScalarExpr,
    /** Numerical-stability epsilon added under the square root. */
    val epsilon: Double = 1e-10,
    concurrency: Concurrency = Concurrency.None,
) : Optimizer {

    init {
        require(featureSize > 0) { "featureSize must be positive" }
        require(epsilon > 0.0) { "epsilon must be positive" }
    }

    private val mode = concurrency.welfordMode()
    private val stepCell: StreamLong = mode.newLong(0L)
    private val cachedLr: StreamDouble = mode.newDouble(0.0)
    private val sumG2: StreamDoubleArray = mode.newDoubleArray(featureSize)

    override fun advance() {
        val t = stepCell.addAndGet(1L)
        cachedLr.store(learningRate.eval(t.toDouble()))
    }

    override fun computeDelta(coordIndex: Int, gradient: Double, observationWeight: Double): Double {
        val g = observationWeight * gradient
        val acc = sumG2.load(coordIndex) + g * g
        sumG2.store(coordIndex, acc)
        val lr = cachedLr.load()
        return -lr * g / sqrt(acc + epsilon)
    }

    override fun reset() {
        stepCell.store(0L)
        cachedLr.store(0.0)
        for (i in 0 until featureSize) sumG2.store(i, 0.0)
    }
}

/**
 * RMSProp: exponential moving average of squared gradients with decay [rho];
 * effective per-coord learning rate is `lr / sqrt(emaG2[i] + epsilon)`.
 */
class RmspropOptimizer(
    override val featureSize: Int,
    /** Base learning-rate schedule. */
    val learningRate: ScalarExpr,
    /** EMA decay for the squared gradient. */
    val rho: Double = 0.9,
    /** Numerical-stability epsilon. */
    val epsilon: Double = 1e-8,
    concurrency: Concurrency = Concurrency.None,
) : Optimizer {

    init {
        require(featureSize > 0) { "featureSize must be positive" }
        require(rho in 0.0..1.0) { "rho must be in [0, 1]; got $rho" }
        require(epsilon > 0.0) { "epsilon must be positive" }
    }

    private val mode = concurrency.welfordMode()
    private val stepCell: StreamLong = mode.newLong(0L)
    private val cachedLr: StreamDouble = mode.newDouble(0.0)
    private val emaG2: StreamDoubleArray = mode.newDoubleArray(featureSize)

    override fun advance() {
        val t = stepCell.addAndGet(1L)
        cachedLr.store(learningRate.eval(t.toDouble()))
    }

    override fun computeDelta(coordIndex: Int, gradient: Double, observationWeight: Double): Double {
        val g = observationWeight * gradient
        val ema = rho * emaG2.load(coordIndex) + (1.0 - rho) * g * g
        emaG2.store(coordIndex, ema)
        val lr = cachedLr.load()
        return -lr * g / sqrt(ema + epsilon)
    }

    override fun reset() {
        stepCell.store(0L)
        cachedLr.store(0.0)
        for (i in 0 until featureSize) emaG2.store(i, 0.0)
    }
}

/**
 * Adam with bias-corrected first and second moments. Default hyperparameters
 * `beta1=0.9`, `beta2=0.999`, `epsilon=1e-8` follow Kingma & Ba 2015.
 */
class AdamOptimizer(
    override val featureSize: Int,
    /** Base learning-rate schedule. */
    val learningRate: ScalarExpr,
    /** First-moment EMA decay. */
    val beta1: Double = 0.9,
    /** Second-moment EMA decay. */
    val beta2: Double = 0.999,
    /** Numerical-stability epsilon. */
    val epsilon: Double = 1e-8,
    concurrency: Concurrency = Concurrency.None,
) : Optimizer {

    init {
        require(featureSize > 0) { "featureSize must be positive" }
        require(beta1 in 0.0..1.0) { "beta1 must be in [0, 1]; got $beta1" }
        require(beta2 in 0.0..1.0) { "beta2 must be in [0, 1]; got $beta2" }
        require(epsilon > 0.0) { "epsilon must be positive" }
    }

    private val mode = concurrency.welfordMode()
    private val stepCell: StreamLong = mode.newLong(0L)
    private val cachedLr: StreamDouble = mode.newDouble(0.0)
    private val cachedBc1: StreamDouble = mode.newDouble(1.0)
    private val cachedBc2: StreamDouble = mode.newDouble(1.0)
    private val m: StreamDoubleArray = mode.newDoubleArray(featureSize)
    private val v: StreamDoubleArray = mode.newDoubleArray(featureSize)

    override fun advance() {
        val t = stepCell.addAndGet(1L)
        cachedLr.store(learningRate.eval(t.toDouble()))
        cachedBc1.store(1.0 - beta1.pow(t.toDouble()))
        cachedBc2.store(1.0 - beta2.pow(t.toDouble()))
    }

    override fun computeDelta(coordIndex: Int, gradient: Double, observationWeight: Double): Double {
        val g = observationWeight * gradient
        val mNext = beta1 * m.load(coordIndex) + (1.0 - beta1) * g
        val vNext = beta2 * v.load(coordIndex) + (1.0 - beta2) * g * g
        m.store(coordIndex, mNext)
        v.store(coordIndex, vNext)
        val mHat = mNext / cachedBc1.load()
        val vHat = vNext / cachedBc2.load()
        val lr = cachedLr.load()
        return -lr * mHat / (sqrt(vHat) + epsilon)
    }

    override fun reset() {
        stepCell.store(0L)
        cachedLr.store(0.0)
        cachedBc1.store(1.0)
        cachedBc2.store(1.0)
        for (i in 0 until featureSize) {
            m.store(i, 0.0)
            v.store(i, 0.0)
        }
    }
}
