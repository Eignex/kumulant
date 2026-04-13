package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.MomentsResult
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.SumResult
import com.eignex.kumulant.core.WeightedMeanResult
import com.eignex.kumulant.core.WeightedVarianceResult

class Sum(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<SumResult> {

    private val value = mode.newDouble(0.0)

    override fun update(
        value: Double,
        timestampNanos: Long,
        weight: Double
    ) {
        this.value.add(value * weight)
    }

    override fun read(timestampNanos: Long) = SumResult(value.load())

    override fun merge(values: SumResult) {
        value.add(values.sum)
    }

    override fun reset() {
        value.store(0.0)
    }

    override fun create(mode: StreamMode?) = Sum(mode ?: this.mode)
}

class Mean(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<WeightedMeanResult> {

    private val totalWeights = mode.newDouble(0.0)
    private val value = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val oldMean = this.value.load()
        val nextW = totalWeights.addAndGet(weight)

        val delta = value - oldMean
        val r = delta * (weight / nextW)

        this.value.add(r)
    }

    override fun read(timestampNanos: Long) =
        WeightedMeanResult(totalWeights.load(), value.load())

    override fun merge(values: WeightedMeanResult) {
        if (values.totalWeights <= 0.0) return

        val nextW = totalWeights.load() + values.totalWeights
        val delta = values.mean - value.load()
        val deltaM = delta * (values.totalWeights / nextW)

        value.add(deltaM)
        totalWeights.add(values.totalWeights)
    }

    override fun reset() {
        totalWeights.store(0.0)
        value.store(0.0)
    }

    override fun create(mode: StreamMode?) = Mean(mode ?: this.mode)
}

class Variance(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<WeightedVarianceResult> {

    private val totalWeights = mode.newDouble(0.0)
    private val mean = mode.newDouble(0.0)
    private val sst = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val oldMean = mean.load()
        val nextW = totalWeights.addAndGet(weight)

        val delta = value - oldMean
        val r = delta * (weight / nextW)

        mean.add(r)
        sst.add((nextW - weight) * delta * r)
    }

    override fun merge(values: WeightedVarianceResult) {
        val w1 = totalWeights.load()
        val w2 = values.totalWeights
        if (w2 <= 0.0) return

        val m1 = mean.load()
        val m2 = values.mean
        val sst2 = values.variance * values.totalWeights

        val nextW = w1 + w2
        val delta = m2 - m1

        val deltaM = delta * (w2 / nextW)
        mean.add(deltaM)

        val sstShift = (delta * delta) * (w1 * w2 / nextW)
        sst.add(sst2 + sstShift)

        totalWeights.add(w2)
    }

    override fun reset() {
        totalWeights.store(0.0)
        mean.store(0.0)
        sst.store(0.0)
    }

    override fun read(timestampNanos: Long): WeightedVarianceResult {
        val totalW = totalWeights.load()
        val variance = if (totalW > 0.0) sst.load() / totalW else 0.0
        return WeightedVarianceResult(totalW, mean.load(), variance)
    }

    override fun create(mode: StreamMode?) = Variance(mode ?: this.mode)
}

class Moments(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<MomentsResult> {

    private val totalWeights = mode.newDouble(0.0)
    private val mean = mode.newDouble(0.0)
    private val m2 = mode.newDouble(0.0)
    private val m3 = mode.newDouble(0.0)
    private val m4 = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return

        val oldM1 = mean.load()
        val oldM2 = m2.load()
        val oldM3 = m3.load()
        val nextW = totalWeights.addAndGet(weight)
        val oldW = nextW - weight

        val delta = value - oldM1
        val deltaW = delta * (weight / nextW)
        val deltaW2 = deltaW * deltaW
        val term1 = delta * deltaW * oldW

        // Update raw sums using deltas
        m4.add(term1 * deltaW2 * (nextW * nextW - 3 * nextW + 3) + 6 * deltaW2 * oldM2 - 4 * deltaW * oldM3)
        m3.add(term1 * deltaW * (nextW - 2) - 3 * deltaW * oldM2)
        m2.add(term1)
        mean.add(deltaW)
    }

    override fun merge(values: MomentsResult) {
        val w1 = totalWeights.load()
        val w2 = values.totalWeights
        if (w2 <= 0.0) return

        val m1 = mean.load()
        val m2Local = m2.load()
        val m3Local = m3.load()

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
            3.0 * delta * (w1 * values.m2 - w2 * m2Local) / nextW

        // Incremental M4 using incoming raw m2/m3/m4
        val m4Delta = values.m4 +
            delta4 * (w1 * w2 * (w1 * w1 - w1 * w2 + w2 * w2) / nextWCu) +
            6.0 * delta2 * (w1 * w1 * values.m2 + w2 * w2 * m2Local) / nextWSq +
            4.0 * delta * (w1 * values.m3 - w2 * m3Local) / nextW

        m4.add(m4Delta)
        m3.add(m3Delta)
        m2.add(values.m2 + (delta2 * w1 * w2 / nextW))
        mean.add(delta * (w2 / nextW))
        totalWeights.add(w2)
    }

    override fun reset() {
        totalWeights.store(0.0)
        mean.store(0.0)
        m2.store(0.0)
        m3.store(0.0)
        m4.store(0.0)
    }

    override fun read(timestampNanos: Long) =
        MomentsResult(
            totalWeights.load(),
            mean.load(),
            m2.load(),
            m3.load(),
            m4.load()
        )

    override fun create(mode: StreamMode?) = Moments(mode ?: this.mode)
}
