package com.eignex.kumulant.stream

import kotlin.time.TimeSource

private val monoStart = TimeSource.Monotonic.markNow()

internal fun currentTimeNanos(): Long = monoStart.elapsedNow().inWholeNanoseconds

// Timestamps use nanoseconds; rates are reported per second.
internal const val NANOS_PER_SECOND: Double = 1_000_000_000.0
