package com.eignex.kumulant.stream

/**
 * Cell encoding picked by [HighWrite][com.eignex.kumulant.core.Concurrency.HighWrite] for naively additive stats. JVM
 * uses a striped-adder mode (LongAdder/DoubleAdder); other platforms fall back
 * to [AtomicMode] since LongAdder/DoubleAdder are JVM-only.
 */
internal expect val highWriteMode: StreamMode
