package com.eignex.kumulant.bench

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Drive every [StatSpec] from N threads under [Concurrency.Strict] and verify the
 * final snapshot scalar matches the analytical reference within the spec's tolerance.
 * Catches lost updates, torn reads, and lock-acquisition bugs.
 *
 * Under [Concurrency.Relaxed] additive stats are still exact (single atomic add per
 * update); coupled stats may drift but must not throw. Both levels are exercised.
 */
class ConcurrencyTest {

    private val threadCount = 4
    private val updatesPerThread = 2_500

    @Test
    fun `every spec is exact under Strict concurrency`() {
        for (spec in allSpecs) {
            runConcurrent(spec, Concurrency.Strict, exact = true)
        }
    }

    @Test
    fun `every spec runs without exception under Relaxed concurrency`() {
        for (spec in allSpecs) {
            runConcurrent(spec, Concurrency.Relaxed, exact = false)
        }
    }

    private fun <R : Result> runConcurrent(spec: StatSpec<R>, level: Concurrency, exact: Boolean) {
        val stat = spec.factory(level)
        val executor = Executors.newFixedThreadPool(threadCount)
        val start = CountDownLatch(1)
        val done = CountDownLatch(threadCount)
        var caught: Throwable? = null
        repeat(threadCount) { tid ->
            executor.submit {
                try {
                    start.await()
                    for (pair in spec.updates(tid, updatesPerThread)) {
                        stat.update(pair[0], 0L, pair[1])
                    }
                } catch (t: Throwable) {
                    caught = t
                } finally {
                    done.countDown()
                }
            }
        }
        start.countDown()
        check(done.await(30, TimeUnit.SECONDS)) { "${spec.name}: timed out at $level" }
        executor.shutdownNow()
        caught?.let { throw AssertionError("${spec.name} threw under $level", it) }

        val got = spec.scalar(stat.read(0L))
        if (exact) {
            // Reference: the spec's analytical reduction over the concatenation of
            // every thread's deterministic workload.
            val combined = (0 until threadCount).asSequence().flatMap { tid ->
                spec.updates(tid, updatesPerThread)
            }
            val want = spec.reference(combined)
            assertTrue(
                abs(got - want) <= spec.tolerance,
                "${spec.name} @ $level: got=$got want=$want diff=${abs(got - want)} tol=${spec.tolerance}",
            )
        }
    }
}
