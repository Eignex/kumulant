package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.time.Duration

/**
 * A pre-update operator that carries state from one observation to the next.
 *
 * Windowing slices a stat into per-bucket replicas, so an operator wrapped *outside* a window would
 * have that state rebuilt on every slice rotation: a lag ring restarts its warm-up, a throttle gate
 * restarts its count, a resample bucket never closes. On a stream slower than one observation per
 * slice the operator then never fires at all. Windowing the operator's delegate and re-applying the
 * operator on top keeps one instance of the state for the whole stream, which is what the caller
 * asked for.
 */
internal interface WindowsInside<R : Result> {
    /** This operator re-applied over its own windowed delegate. */
    fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R>
}
