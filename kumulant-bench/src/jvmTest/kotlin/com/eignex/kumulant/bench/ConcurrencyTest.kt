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
 * Drive every [StatSpec] under every [Concurrency] level and verify the snapshot.
 *
 * - [Concurrency.None] — single-threaded; assertion is identical to the commonTest
 *   correctness test but pinned here so a JVM-only contributor can run the whole
 *   matrix in one place.
 * - [Concurrency.Strict] / [Concurrency.HighWrite] — concurrent; exact match against
 *   the analytical reference for [StatSpec.orderIndependent] stats; finiteness only
 *   for order-dependent ones.
 * - [Concurrency.Relaxed] — concurrent; drift permitted on coupled stats, so we only
 *   require the call to not throw and the snapshot to be finite.
 */
class ConcurrencyTest {

    private val threadCount = 4
    private val updatesPerThread = 2_500

    @Test
    fun `every spec under None`() {
        for (spec in allSpecs) runSerial(spec, Concurrency.None, exact = spec.orderIndependent)
    }

    @Test
    fun `every spec under Strict`() {
        for (spec in allSpecs) runConcurrent(spec, Concurrency.Strict, exact = spec.orderIndependent)
    }

    @Test
    fun `every spec under HighWrite`() {
        for (spec in allSpecs) runConcurrent(spec, Concurrency.HighWrite, exact = spec.orderIndependent)
    }

    @Test
    fun `every spec under Relaxed`() {
        for (spec in allSpecs) runConcurrent(spec, Concurrency.Relaxed, exact = false)
    }

    private fun <R : Result> runSerial(spec: StatSpec<R>, level: Concurrency, exact: Boolean) {
        val stat = spec.factory(level)
        val combined = (0 until threadCount).asSequence().flatMap { tid ->
            spec.updates(tid, updatesPerThread)
        }
        for (u in combined) stat.update(u.value, u.timestampNanos, u.weight)
        val got = spec.scalar(stat.read(spec.readAt(threadCount * updatesPerThread)))
        assertTrue(got.isFinite(), "${spec.name} @ $level: non-finite snapshot $got")
        if (exact) {
            val want = spec.reference(
                (0 until threadCount).asSequence().flatMap { spec.updates(it, updatesPerThread) }
            )
            assertTrue(
                abs(got - want) <= spec.tolerance,
                "${spec.name} @ $level: got=$got want=$want diff=${abs(got - want)} tol=${spec.tolerance}",
            )
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
                    for (u in spec.updates(tid, updatesPerThread)) {
                        stat.update(u.value, u.timestampNanos, u.weight)
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

        val got = spec.scalar(stat.read(spec.readAt(threadCount * updatesPerThread)))
        assertTrue(got.isFinite(), "${spec.name} @ $level: non-finite snapshot $got")
        if (exact) {
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
