package com.eignex.kumulant.stream

/**
 * Cell encoding picked by `Concurrency.HighWrite` for naively additive stats. JVM
 * uses `AdderMode` (striped `LongAdder`/`DoubleAdder`); other platforms fall back
 * to [AtomicMode] since `LongAdder`/`DoubleAdder` are JVM-only.
 */
internal expect val highWriteMode: StreamMode
