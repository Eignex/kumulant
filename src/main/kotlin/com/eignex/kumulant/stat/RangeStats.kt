package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.HasRange
import com.eignex.kumulant.core.RangeResult
import com.eignex.kumulant.core.SeriesStat

/**
 * Tracks the minimum and maximum of a stream.
 *
 * Uses CAS loops on atomic references for lock-free operation under concurrent modes.
 */
class Range(
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<RangeResult>, HasRange {

    private val _min = mode.newReference(Double.POSITIVE_INFINITY)
    private val _max = mode.newReference(Double.NEGATIVE_INFINITY)

    override val min: Double get() = _min.load()
    override val max: Double get() = _max.load()

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        while (true) {
            val current = _min.load()
            if (value >= current || _min.compareAndSet(current, value)) break
        }
        while (true) {
            val current = _max.load()
            if (value <= current || _max.compareAndSet(current, value)) break
        }
    }

    override fun merge(values: RangeResult) {
        update(values.min, System.nanoTime())
        update(values.max, System.nanoTime())
    }

    override fun reset() {
        _min.store(Double.POSITIVE_INFINITY)
        _max.store(Double.NEGATIVE_INFINITY)
    }

    override fun read(timestampNanos: Long) = RangeResult(min, max, name)

    override fun copy(mode: StreamMode?, name: String?) = Range(mode ?: this.mode, name ?: this.name)
}
