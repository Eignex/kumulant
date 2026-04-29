package com.eignex.kumulant.stream

import kotlin.math.max
import kotlin.math.min

/**
 * A lock-free, dynamically resizing array-backed bin manager.
 * It stores a contiguous array of StreamDouble references to avoid Map allocation overhead.
 * Resizing creates a new array but copies the original StreamDouble instances,
 * guaranteeing zero dropped writes during concurrent access.
 */
class ArrayBins(val mode: StreamMode) {

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
                newLength = 128
                newOffset = index - 64
            } else {
                newOffset = min(offset, index)
                val maxIndex = max(offset + length - 1, index)

                var capacity = length
                while (newOffset + capacity <= maxIndex) {
                    capacity = (capacity * 1.5).toInt() + 1
                }
                newLength = capacity
            }

            // Create new array, preserving existing StreamDouble instances
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
