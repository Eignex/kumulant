package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.stream.monotonicMode

// Hysteresis adapter: maps a noisy numeric stream onto a debounced 0.0/1.0 signal.
//
// The state flips to 1.0 when an input rises above `high`, and back to 0.0 when an input
// falls below `low`. Inputs in the deadband `[low, high]` (after the initial sample)
// keep the current state. The first update establishes the initial state by treating
// the input as if entering from neutral: value > high seeds 1.0, value < low seeds 0.0,
// and anything in between seeds 0.0.
//
// Each call forwards the current debounced state (not just transitions), so downstream
// stats that consume a series naturally observe per-update progress.
//
// Concurrency: per-cell atomics with bounded drift (category 1). The state cell may
// briefly observe stale reads under contention; rapid bouncing values can produce a
// slightly different transition trace than a strict serial replay would. Never throws.

/** Debounce a noisy numeric stream into a 0.0/1.0 stream using two-threshold hysteresis. */
internal fun <R : Result> SeriesStat<R>.hysteresis(low: Double, high: Double): SeriesStat<R> =
    HysteresisSeriesStat(this, low, high)

internal class HysteresisSeriesStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val low: Double,
    private val high: Double,
) : SeriesStat<R>,
    Stat<R> by delegate {

    init {
        require(low <= high) { "hysteresis low ($low) must be <= high ($high)" }
    }

    private val mode = delegate.concurrency.monotonicMode()
    private val state = mode.newLong(STATE_UNSET)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val current = state.load()
        val next = when {
            value > high -> STATE_HIGH
            value < low -> STATE_LOW
            current == STATE_UNSET -> STATE_LOW
            else -> current
        }
        state.store(next)
        delegate.update(if (next == STATE_HIGH) 1.0 else 0.0, timestampNanos, weight)
    }

    override fun reset() {
        delegate.reset()
        state.store(STATE_UNSET)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        HysteresisSeriesStat(delegate.create(concurrency), low, high)

    companion object {
        private const val STATE_UNSET: Long = -1L
        private const val STATE_LOW: Long = 0L
        private const val STATE_HIGH: Long = 1L
    }
}
