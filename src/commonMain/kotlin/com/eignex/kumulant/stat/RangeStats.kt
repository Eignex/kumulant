package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.MaxResult
import com.eignex.kumulant.core.MinResult
import com.eignex.kumulant.core.RangeResult
import com.eignex.kumulant.core.SeriesStat

/**
 * Tracks the minimum and maximum of a stream.
 */
class Range(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<RangeResult> {

    private val min = mode.newDouble(Double.POSITIVE_INFINITY)
    private val max = mode.newDouble(Double.NEGATIVE_INFINITY)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (value < min.load()) min.store(value)
        if (value > max.load()) max.store(value)
    }

    override fun merge(values: RangeResult) {
        if (values.min < min.load()) min.store(values.min)
        if (values.max > max.load()) max.store(values.max)
    }

    override fun reset() {
        min.store(Double.POSITIVE_INFINITY)
        max.store(Double.NEGATIVE_INFINITY)
    }

    override fun read(timestampNanos: Long) = RangeResult(min.load(), max.load())

    override fun create(mode: StreamMode?) = Range(mode ?: this.mode)
}

/** Tracks the minimum of a stream. */
class Min(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<MinResult> {

    private val value = mode.newDouble(Double.POSITIVE_INFINITY)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (value < this.value.load()) this.value.store(value)
    }

    override fun merge(values: MinResult) {
        if (values.min < value.load()) value.store(values.min)
    }

    override fun reset() {
        value.store(Double.POSITIVE_INFINITY)
    }

    override fun read(timestampNanos: Long) = MinResult(value.load())

    override fun create(mode: StreamMode?) = Min(mode ?: this.mode)
}

/** Tracks the maximum of a stream. */
class Max(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<MaxResult> {

    private val value = mode.newDouble(Double.NEGATIVE_INFINITY)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (value > this.value.load()) this.value.store(value)
    }

    override fun merge(values: MaxResult) {
        if (values.max > this.value.load()) this.value.store(values.max)
    }

    override fun reset() {
        value.store(Double.NEGATIVE_INFINITY)
    }

    override fun read(timestampNanos: Long) = MaxResult(value.load())

    override fun create(mode: StreamMode?) = Max(mode ?: this.mode)
}
