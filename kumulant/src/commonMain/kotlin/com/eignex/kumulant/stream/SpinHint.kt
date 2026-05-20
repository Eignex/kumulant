package com.eignex.kumulant.stream

/**
 * Yield the current thread inside a busy-wait loop. On the JVM this calls
 * `Thread.yield()` so a spinning reader doesn't starve the in-flight writer
 * it's waiting on under heavy contention; on single-threaded targets (JS,
 * Wasm) it's a no-op.
 */
internal expect fun spinHint()
