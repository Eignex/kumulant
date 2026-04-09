package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.concurrent.getValue
import com.eignex.kumulant.core.HasMax
import com.eignex.kumulant.core.HasMin
import com.eignex.kumulant.core.HasRange
import com.eignex.kumulant.core.MaxResult
import com.eignex.kumulant.core.MinResult
import com.eignex.kumulant.core.RangeResult
import com.eignex.kumulant.core.SeriesStat

/**
 * Tracks the minimum and maximum of a stream.
 */
class Range(
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<RangeResult>, HasRange {

    private val _min = mode.newDouble(Double.POSITIVE_INFINITY)
    private val _max = mode.newDouble(Double.NEGATIVE_INFINITY)

    override val min: Double by _min
    override val max: Double by _max

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (value < _min.load()) _min.store(value)
        if (value > _max.load()) _max.store(value)
    }

    override fun merge(values: RangeResult) {
        if (values.min < _min.load()) _min.store(values.min)
        if (values.max > _max.load()) _max.store(values.max)
    }

    override fun reset() {
        _min.store(Double.POSITIVE_INFINITY)
        _max.store(Double.NEGATIVE_INFINITY)
    }

    override fun read(timestampNanos: Long) = RangeResult(min, max, name)

    override fun copy(mode: StreamMode?, name: String?) = Range(mode ?: this.mode, name ?: this.name)
}

class Min(
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<MinResult>, HasMin {

    private val _min = mode.newDouble(Double.POSITIVE_INFINITY)
    override val min: Double by _min

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (value < _min.load()) _min.store(value)
    }

    override fun merge(values: MinResult) {
        if (values.min < _min.load()) _min.store(values.min)
    }

    override fun reset() {
        _min.store(Double.POSITIVE_INFINITY)
    }

    override fun read(timestampNanos: Long) = MinResult(min, name)

    override fun copy(mode: StreamMode?, name: String?) = Min(mode ?: this.mode, name ?: this.name)
}

class Max(
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<MaxResult>, HasMax {

    private val _max = mode.newDouble(Double.NEGATIVE_INFINITY)
    override val max: Double by _max

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (value > _max.load()) _max.store(value)
    }

    override fun merge(values: MaxResult) {
        if (values.max > _max.load()) _max.store(values.max)
    }

    override fun reset() {
        _max.store(Double.NEGATIVE_INFINITY)
    }

    override fun read(timestampNanos: Long) = MaxResult(max, name)

    override fun copy(mode: StreamMode?, name: String?) = Max(mode ?: this.mode, name ?: this.name)
}

// Planned: Percentile — exact min/max at arbitrary quantile boundaries using a sorted structure
// Planned: AbsRange — tracks min/max of |value| for signal-envelope statistics
