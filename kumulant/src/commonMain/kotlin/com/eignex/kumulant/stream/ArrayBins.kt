package com.eignex.kumulant.stream

import kotlin.math.max
import kotlin.math.min

/**
 * A lock-free, dynamically resizing array-backed bin manager.
 * It stores a contiguous array of StreamDouble references to avoid Map allocation overhead.
 * Resizing creates a new array but copies the original StreamDouble instances,
 * guaranteeing zero dropped writes during concurrent access.
 */
internal class ArrayBins(private val mode: StreamMode) {

    private class State(val offset: Int, val bins: Array<StreamDouble>)

    private val stateRef = mode.newReference(State(0, emptyArray()))

    /** Add [weight] to bin [index], growing the underlying array as needed. */
    fun add(index: Int, weight: Double) {
        while (true) {
            val state = stateRef.load()
            val offset = state.offset
            val bins = state.bins
            val length = bins.size

            if (length > 0 && index >= offset && index < offset + length) {
                bins[index - offset].add(weight)
                return
            }

            val newLength: Int
            val newOffset: Int

            if (length == 0) {
                newLength = INITIAL_BINS
                newOffset = index - INITIAL_CENTER_OFFSET
            } else {
                newOffset = min(offset, index)
                val maxIndex = max(offset + length - 1, index)

                var capacity = length
                while (newOffset + capacity <= maxIndex) {
                    capacity = (capacity * GROWTH_FACTOR).toInt() + 1
                }
                newLength = capacity
            }

            val newBins = Array(newLength) { i ->
                val targetIndex = newOffset + i
                if (targetIndex >= offset && targetIndex < offset + length) {
                    bins[targetIndex - offset]
                } else {
                    mode.newDouble(0.0)
                }
            }

            val newState = State(newOffset, newBins)

            stateRef.compareAndSet(state, newState)
        }
    }

    /** Return a point-in-time copy of populated bins as an index-to-weight map. */
    fun snapshot(): Map<Int, Double> {
        val state = stateRef.load()
        val result = mutableMapOf<Int, Double>()
        for (i in state.bins.indices) {
            val w = state.bins[i].load()
            if (w > 0.0) {
                result[state.offset + i] = w
            }
        }
        return result
    }

    /** Drop all bins, returning the manager to its empty state. */
    fun clear() {
        stateRef.store(State(0, emptyArray()))
    }
}

/**
 * Bins allocated the first time an index arrives.
 *
 * These three governed the memory profile of every [com.eignex.kumulant.stat.quantile.DDSketchStat] and
 * [com.eignex.kumulant.stat.quantile.LinearHistogramStat] as three bare literals - `128`, `64`, `1.5` -
 * with the `64 = 128 / 2` relationship left as a coincidence a reader had to notice. It is not a
 * coincidence: the first index is placed at the *centre* of the initial span so that the next index in
 * either direction fits without regrowing, and an offset that was not half the span would make one
 * direction re-grow immediately.
 */
private const val INITIAL_BINS: Int = 128

/** Half of [INITIAL_BINS], so the first index lands in the middle of the initial span. */
private const val INITIAL_CENTER_OFFSET: Int = INITIAL_BINS / 2

/** Geometric growth factor when the span has to widen. */
private const val GROWTH_FACTOR: Double = 1.5
