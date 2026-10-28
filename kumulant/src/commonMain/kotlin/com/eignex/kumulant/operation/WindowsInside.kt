package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat
import kotlin.time.Duration

// Windowing passes through decorators to preserve whole-stream operator state across slice rotations.
// S preserves the operator modality while its delegate is windowed.
internal interface WindowsInside<R : Result, S : Stat<R>> {
    /** This operator re-applied over its own windowed delegate. */
    fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): S
}
