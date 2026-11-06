package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import com.eignex.util.concurrent.DoubleCounter
import kotlin.math.max
import kotlin.math.min

// Resizing reuses existing cells so writers holding an older array still update the live bins.
// Each counter follows the concurrency policy; a multi-bin snapshot is not atomic.
internal class ArrayBins(concurrency: Concurrency) {
    private val mode = concurrency.additiveMode()

    private class State(val offset: Int, val bins: Array<DoubleCounter>)

    private val stateRef = concurrency.monotonicMode().newReference(State(0, emptyArray()))

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

    fun clear() {
        stateRef.store(State(0, emptyArray()))
    }
}

// Centre the first index in the initial span to leave room for growth in either direction.
private const val INITIAL_BINS: Int = 128

private const val INITIAL_CENTER_OFFSET: Int = INITIAL_BINS / 2

private const val GROWTH_FACTOR: Double = 1.5
