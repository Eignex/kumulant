package com.eignex.kumulant.stream

import com.eignex.util.concurrent.AtomicCells
import com.eignex.util.concurrent.parallelRun
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

internal fun runConcurrently(threads: Int, iterationsPerThread: Int, body: (threadId: Int, iter: Int) -> Unit) {
    val ready = AtomicCells.newLong()
    parallelRun(
        List(threads) { thread ->
            {
                ready.add(1)
                awaitConcurrent { ready.load() == threads.toLong() }
                repeat(iterationsPerThread) { body(thread, it) }
            }
        },
    )
}

internal fun awaitConcurrent(condition: () -> Boolean) {
    val started = TimeSource.Monotonic.markNow()
    while (!condition()) {
        check(started.elapsedNow() < 10.seconds) { "workers did not synchronize within 10s" }
    }
}
