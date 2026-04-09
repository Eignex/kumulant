package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.concurrent.getValue
import com.eignex.kumulant.core.HasMean
import com.eignex.kumulant.core.HasSampleVariance
import com.eignex.kumulant.core.HasShapeMoments
import com.eignex.kumulant.core.HasSum
import com.eignex.kumulant.core.HasTotalWeights
import com.eignex.kumulant.core.HasVariance
import com.eignex.kumulant.core.MomentsResult
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.SumResult
import com.eignex.kumulant.core.WeightedMeanResult
import com.eignex.kumulant.core.WeightedVarianceResult
import kotlin.math.exp

class Sum(
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<SumResult>, HasSum {

    private val _sum = mode.newDouble(0.0)
    override val sum: Double by _sum

    override fun update(
        value: Double,
        timestampNanos: Long,
        weight: Double
    ) {
        _sum.add(value * weight)
    }

    override fun read(timestampNanos: Long) = SumResult(sum, name)

    override fun merge(values: SumResult) {
        _sum.add(values.sum)
    }

    override fun reset() {
        _sum.store(0.0)
    }

    override fun create(mode: StreamMode?, name: String?) = Sum(mode ?: this.mode, name ?: this.name)
}

class Mean(
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<WeightedMeanResult>,
    HasTotalWeights,
    HasMean {

    private val _totalWeights = mode.newDouble(0.0)
    private val _mean = mode.newDouble(0.0)

    override val totalWeights: Double by _totalWeights
    override val mean: Double by _mean

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val oldMean = _mean.load()
        val nextW = _totalWeights.addAndGet(weight)

        val delta = value - oldMean
        val r = delta * (weight / nextW)

        _mean.add(r)
    }

    override fun read(timestampNanos: Long) =
        WeightedMeanResult(totalWeights, mean, name)

    override fun merge(values: WeightedMeanResult) {
        if (values.totalWeights <= 0.0) return

        val nextW = totalWeights + values.totalWeights
        val delta = values.mean - mean
        val deltaM = delta * (values.totalWeights / nextW)

        _mean.add(deltaM)
        _totalWeights.add(values.totalWeights)
    }

    override fun reset() {
        _totalWeights.store(0.0)
        _mean.store(0.0)
    }

    override fun create(mode: StreamMode?, name: String?) = Mean(mode ?: this.mode, name ?: this.name)
}

class Variance(
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<WeightedVarianceResult>, HasMean, HasSampleVariance {

    private val _totalWeights = mode.newDouble(0.0)
    private val _mean = mode.newDouble(0.0)
    private val _sst = mode.newDouble(0.0)

    override val totalWeights: Double by _totalWeights
    override val mean: Double by _mean
    override val sst: Double by _sst

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val oldMean = mean
        val nextW = _totalWeights.addAndGet(weight)

        val delta = value - oldMean
        val r = delta * (weight / nextW)

        _mean.add(r)
        _sst.add((nextW - weight) * delta * r)
    }

    override fun merge(values: WeightedVarianceResult) {
        val w1 = _totalWeights.load()
        val w2 = values.totalWeights
        if (w2 <= 0.0) return

        val m1 = _mean.load()
        val m2 = values.mean
        val sst2 = values.sst

        val nextW = w1 + w2
        val delta = m2 - m1

        val deltaM = delta * (w2 / nextW)
        _mean.add(deltaM)

        val sstShift = (delta * delta) * (w1 * w2 / nextW)
        _sst.add(sst2 + sstShift)

        _totalWeights.add(w2)
    }

    override fun reset() {
        _totalWeights.store(0.0)
        _mean.store(0.0)
        _sst.store(0.0)
    }

    override fun read(timestampNanos: Long) =
        WeightedVarianceResult(totalWeights, mean, variance, name)

    override fun create(mode: StreamMode?, name: String?) = Variance(mode ?: this.mode, name ?: this.name)
}

class Moments(
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null,
) : SeriesStat<MomentsResult>, HasMean, HasSampleVariance, HasShapeMoments {

    private val _w = mode.newDouble(0.0)
    private val _m1 = mode.newDouble(0.0)
    private val _m2 = mode.newDouble(0.0)
    private val _m3 = mode.newDouble(0.0)
    private val _m4 = mode.newDouble(0.0)

    override val totalWeights: Double by _w
    override val mean: Double by _m1
    override val m2: Double by _m2
    override val sst: Double get() = m2
    override val m3: Double by _m3
    override val m4: Double by _m4

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return

        val oldM1 = _m1.load()
        val oldM2 = _m2.load()
        val oldM3 = _m3.load()
        val nextW = _w.addAndGet(weight)
        val oldW = nextW - weight

        val delta = value - oldM1
        val deltaW = delta * (weight / nextW)
        val deltaW2 = deltaW * deltaW
        val term1 = delta * deltaW * oldW

        // Update raw sums using deltas
        _m4.add(term1 * deltaW2 * (nextW * nextW - 3 * nextW + 3) + 6 * deltaW2 * oldM2 - 4 * deltaW * oldM3)
        _m3.add(term1 * deltaW * (nextW - 2) - 3 * deltaW * oldM2)
        _m2.add(term1)
        _m1.add(deltaW)
    }

    override fun merge(values: MomentsResult) {
        val w1 = _w.load()
        val w2 = values.totalWeights
        if (w2 <= 0.0) return

        val m1 = _m1.load()
        val m2 = _m2.load()
        val m3 = _m3.load()

        val nextW = w1 + w2
        val delta = values.mean - m1

        val delta2 = delta * delta
        val delta3 = delta2 * delta
        val delta4 = delta3 * delta

        val nextWSq = nextW * nextW
        val nextWCu = nextWSq * nextW
        // Incremental M3 using incoming raw m2/m3
        val m3Delta = values.m3 +
            delta3 * (w1 * w2 * (w1 - w2) / nextWSq) +
            3.0 * delta * (w1 * values.m2 - w2 * m2) / nextW

        // Incremental M4 using incoming raw m2/m3/m4
        val m4Delta = values.m4 +
            delta4 * (w1 * w2 * (w1 * w1 - w1 * w2 + w2 * w2) / nextWCu) +
            6.0 * delta2 * (w1 * w1 * values.m2 + w2 * w2 * m2) / nextWSq +
            4.0 * delta * (w1 * values.m3 - w2 * m3) / nextW

        _m4.add(m4Delta)
        _m3.add(m3Delta)
        _m2.add(values.m2 + (delta2 * w1 * w2 / nextW))
        _m1.add(delta * (w2 / nextW))
        _w.add(w2)
    }

    override fun reset() {
        _w.store(0.0)
        _m1.store(0.0)
        _m2.store(0.0)
        _m3.store(0.0)
        _m4.store(0.0)
    }

    override fun read(timestampNanos: Long) =
        MomentsResult(totalWeights, mean, m2, m3, m4, name)

    override fun create(mode: StreamMode?, name: String?) = Moments(mode ?: this.mode, name ?: this.name)
}

class RollingMean(
    val alpha: Double,
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<WeightedMeanResult>, HasMean, HasTotalWeights {

    private val _biasedMean = mode.newDouble(0.0)
    private val _totalWeights = mode.newDouble(0.0)

    private fun getCorrection(weight: Double): Double {
        return if (weight == 0.0) 0.0 else 1.0 - exp(-alpha * weight)
    }

    override val totalWeights: Double by _totalWeights
    override val mean: Double
        get() {
            val biased = _biasedMean.load()
            val weight = _totalWeights.load()
            val correction = getCorrection(weight)
            return if (correction == 0.0) 0.0 else biased / correction
        }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val a = getCorrection(weight)
        _biasedMean.add(a * (value - _biasedMean.load()))
        _totalWeights.add(weight)
    }

    override fun merge(values: WeightedMeanResult) {
        val localMean = this.mean
        val localWeight = _totalWeights.load()
        val localEffectiveWeight = getCorrection(localWeight)

        val remoteMean = values.mean
        val remoteWeight = values.totalWeights
        val remoteEffectiveWeight = getCorrection(remoteWeight)

        val totalEffectiveWeight = localEffectiveWeight + remoteEffectiveWeight
        if (totalEffectiveWeight == 0.0) return

        val mergedMean =
            (localMean * localEffectiveWeight + remoteMean * remoteEffectiveWeight) / totalEffectiveWeight

        val newTotalWeight = localWeight + remoteWeight

        val newCorrection = getCorrection(newTotalWeight)
        val targetBiasedMean = mergedMean * newCorrection

        val delta = targetBiasedMean - _biasedMean.load()

        _biasedMean.add(delta)
        _totalWeights.add(remoteWeight)
    }

    override fun reset() {
        _biasedMean.store(0.0)
        _totalWeights.store(0.0)
    }

    override fun read(timestampNanos: Long) =
        WeightedMeanResult(totalWeights, mean, name)

    override fun create(mode: StreamMode?, name: String?) = RollingMean(alpha, mode ?: this.mode, name ?: this.name)
}

class RollingVariance(
    val alpha: Double,
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<WeightedVarianceResult>, HasMean, HasVariance, HasTotalWeights {

    private val _biasedMean = mode.newDouble(0.0)
    private val _biasedM2 = mode.newDouble(0.0)
    private val _totalWeights = mode.newDouble(0.0)

    private fun getCorrection(weight: Double): Double {
        return if (weight == 0.0) 0.0 else 1.0 - exp(-alpha * weight)
    }

    override val totalWeights: Double by _totalWeights

    override val mean: Double
        get() {
            val biased = _biasedMean.load()
            val weight = _totalWeights.load()
            val correction = getCorrection(weight)
            return if (correction == 0.0) 0.0 else biased / correction
        }

    override val variance: Double
        get() {
            val biasedVar = _biasedM2.load()
            val weight = _totalWeights.load()
            val correction = getCorrection(weight)
            return if (correction == 0.0) 0.0 else biasedVar / correction
        }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val a = getCorrection(weight)

        val currentRawMean = _biasedMean.load()
        val delta = value - currentRawMean
        val increment = a * delta
        val newRawMean = currentRawMean + increment

        val currentBiasedM2 = _biasedM2.load()
        _biasedM2.add(a * (delta * (value - newRawMean) - currentBiasedM2))
        _biasedMean.add(increment)
        _totalWeights.add(weight)
    }

    override fun merge(values: WeightedVarianceResult) {
        val remoteWeightRaw = values.totalWeights
        if (remoteWeightRaw <= 0.0) return

        val localWeightRaw = _totalWeights.load()
        val w1 = getCorrection(localWeightRaw)
        val w2 = getCorrection(remoteWeightRaw)
        val wSum = w1 + w2

        if (wSum == 0.0) return

        val localMean = this.mean
        val localVar = this.variance
        val remoteMean = values.mean
        val remoteVar = values.variance

        // Merge Means
        val mergedMean = (localMean * w1 + remoteMean * w2) / wSum

        val deltaMean = localMean - remoteMean
        val weightedVarSum = (w1 * localVar) + (w2 * remoteVar)
        val betweenGroupTerm = (w1 * w2 * deltaMean * deltaMean) / wSum

        val mergedVariance = (weightedVarSum + betweenGroupTerm) / wSum

        val newTotalWeight = localWeightRaw + remoteWeightRaw
        val newCorrection = getCorrection(newTotalWeight)

        val targetBiasedMean = mergedMean * newCorrection
        _biasedMean.add(targetBiasedMean - _biasedMean.load())

        val targetBiasedM2 = mergedVariance * newCorrection
        _biasedM2.add(targetBiasedM2 - _biasedM2.load())

        _totalWeights.add(remoteWeightRaw)
    }

    override fun reset() {
        _biasedMean.store(0.0)
        _biasedM2.store(0.0)
        _totalWeights.store(0.0)
    }

    override fun read(timestampNanos: Long) = WeightedVarianceResult(
        totalWeights,
        mean,
        variance,
        name
    )

    override fun create(mode: StreamMode?, name: String?) = RollingVariance(alpha, mode ?: this.mode, name ?: this.name)
}
