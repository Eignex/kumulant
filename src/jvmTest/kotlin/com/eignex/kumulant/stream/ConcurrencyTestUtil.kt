package com.eignex.kumulant.stream

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

internal fun runConcurrently(
    threads: Int,
    iterationsPerThread: Int,
    body: (threadId: Int, iter: Int) -> Unit,
) {
    val pool = Executors.newFixedThreadPool(threads)
    val start = CountDownLatch(1)
    val done = CountDownLatch(threads)
    try {
        repeat(threads) { t ->
            pool.submit {
                try {
                    start.await()
                    for (i in 0 until iterationsPerThread) body(t, i)
                } finally {
                    done.countDown()
                }
            }
        }
        start.countDown()
        check(done.await(30, TimeUnit.SECONDS)) { "workers did not finish within 30s" }
    } finally {
        pool.shutdownNow()
    }
}
