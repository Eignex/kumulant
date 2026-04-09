package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamDouble
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.currentTimeNanos
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.DecayingRateResult
import com.eignex.kumulant.core.HasRate
import com.eignex.kumulant.core.RateResult
import com.eignex.kumulant.core.SeriesStat
import kotlin.math.exp
import kotlin.time.Duration

class Rate(
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<RateResult>, HasRate {

    private val _totalValues = mode.newDouble(0.0)
    private val _startTimestampNanos = mode.newLong(Long.MIN_VALUE)
    val startTimestampNanos: Long get() = _startTimestampNanos.load()

    override fun update(
        value: Double, timestampNanos: Long, weight: Double
    ) {
        if (_startTimestampNanos.load() == Long.MIN_VALUE) {
            _startTimestampNanos.store(timestampNanos)
        }
        _totalValues.add(value * weight)
    }

    override fun copy(
        mode: StreamMode?,
        name: String?
    ) = Rate(mode ?: this.mode, name ?: this.name)

    override fun read(timestampNanos: Long): RateResult {
        val start = if (_startTimestampNanos.load() == Long.MIN_VALUE) timestampNanos
        else _startTimestampNanos.load()
        return RateResult(
            startTimestampNanos = start,
            totalValue = _totalValues.load(),
            timestampNanos = timestampNanos,
            name = name
        )
    }

    override fun merge(values: RateResult) {
        if (values.totalValue == 0.0) return

        _totalValues.add(values.totalValue)

        val currentStart = _startTimestampNanos.load()
        if (currentStart == Long.MIN_VALUE || values.startTimestampNanos < currentStart) {
            _startTimestampNanos.store(values.startTimestampNanos)
        }
    }

    override fun reset() {
        _startTimestampNanos.store(Long.MIN_VALUE)
        _totalValues.store(0.0)
    }

    override val rate: Double get() = read().rate
}

class DecayingRate(
    val halfLife: Duration,
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null,
) : SeriesStat<DecayingRateResult>, HasRate {

    private val alpha = 0.69314718056 / halfLife.inWholeNanoseconds.toDouble()
    private val rotationThresholdNanos = halfLife.inWholeNanoseconds * 50

    private class Epoch(
        val landmarkNanos: Long, val accumulator: StreamDouble
    )

    private val epochRef = mode.newReference(
        Epoch(
            currentTimeNanos(), mode.newDouble(0.0)
        )
    )

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        while (true) {
            val currentEpoch = epochRef.load()

            if (timestampNanos - currentEpoch.landmarkNanos > rotationThresholdNanos) {
                tryRotateEpoch(currentEpoch, timestampNanos)
                continue
            }

            val dt = timestampNanos - currentEpoch.landmarkNanos
            val scaleFactor = exp(alpha * dt)
            currentEpoch.accumulator.add(value * weight * scaleFactor)
            return
        }
    }

    override fun copy(
        mode: StreamMode?,
        name: String?
    ) = DecayingRate(halfLife, mode ?: this.mode, name ?: this.name)

    private fun tryRotateEpoch(oldEpoch: Epoch, now: Long) {
        val oldVal = oldEpoch.accumulator.load()
        val dt = now - oldEpoch.landmarkNanos
        val carriedOverValue = oldVal * exp(-alpha * dt)

        val newAccumulator = mode.newDouble(carriedOverValue)
        val newEpoch = Epoch(now, newAccumulator)

        epochRef.compareAndSet(oldEpoch, newEpoch)
    }

    override fun read(timestampNanos: Long): DecayingRateResult {
        val currentEpoch = epochRef.load()
        val totalAccumulated = currentEpoch.accumulator.load()

        val dt = (timestampNanos - currentEpoch.landmarkNanos).toDouble()
        val currentEnergy = totalAccumulated * exp(-alpha * dt)
        val ratePerSec = currentEnergy * alpha * 1e9

        return DecayingRateResult(ratePerSec, timestampNanos, name)
    }

    override fun merge(values: DecayingRateResult) {
        if (values.rate <= 0.0) return

        while (true) {
            val currentEpoch = epochRef.load()
            val now = values.timestampNanos

            if (now - currentEpoch.landmarkNanos > rotationThresholdNanos) {
                tryRotateEpoch(currentEpoch, now)
                continue
            }

            val incomingEnergy = values.rate / (alpha * 1e9)
            val dt = (now - currentEpoch.landmarkNanos).toDouble()
            val scaledIncomingEnergy = incomingEnergy * exp(alpha * dt)

            currentEpoch.accumulator.add(scaledIncomingEnergy)
            break
        }
    }

    override fun reset() {
        epochRef.compareAndSet(
            epochRef.load(), Epoch(
                currentTimeNanos(), mode.newDouble(0.0)
            )
        )
    }

    override val rate: Double get() = read().rate
}
