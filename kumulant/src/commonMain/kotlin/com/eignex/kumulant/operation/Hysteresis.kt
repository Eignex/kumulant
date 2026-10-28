package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.monotonicMode
import kotlin.time.Duration

// The first sample seeds 1 above high and 0 otherwise; later deadband samples retain the state.
// Every accepted update forwards the current state. Inert weights preserve it.
// Per-cell atomics permit bounded drift in transition order under contention.

internal fun <R : Result> SeriesStat<R>.hysteresis(low: Double, high: Double): SeriesStat<R> =
    HysteresisSeriesStat(this, low, high)

internal class HysteresisSeriesStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val low: Double,
    private val high: Double,
) : SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {

    init {
        require(low <= high) { "hysteresis low ($low) must be <= high ($high)" }
    }

    private val mode = delegate.concurrency.monotonicMode()
    private val state = mode.newLong(STATE_UNSET)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
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

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        HysteresisSeriesStat(delegate.windowed(duration, slices, concurrency), low, high)

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        HysteresisSeriesStat(delegate.create(concurrency), low, high)

    companion object {
        private const val STATE_UNSET: Long = -1L
        private const val STATE_LOW: Long = 0L
        private const val STATE_HIGH: Long = 1L
    }
}
