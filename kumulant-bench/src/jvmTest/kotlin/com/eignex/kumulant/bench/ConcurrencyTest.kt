package com.eignex.kumulant.bench

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ConcurrencyTest {
    @Test
    fun `worker failures are propagated without waiting for another blocked worker`() {
        val blocked = CountDownLatch(1)

        val failure = assertFailsWith<IllegalArgumentException> {
            runWorkers(listOf({ blocked.await() }, { throw IllegalArgumentException("worker failed") }))
        }

        assertEquals("worker failed", failure.message)
    }

    @Test
    fun `reader failures stop a concurrent workload`() {
        val failure = assertFailsWith<IllegalStateException> {
            runWorkers(listOf({ CountDownLatch(1).await() }), reader = { error("reader failed") })
        }

        assertEquals("reader failed", failure.message)
    }

    @Test
    fun `a stalled worker is interrupted when the workload times out`() {
        assertFailsWith<IllegalStateException> {
            runWorkers(listOf({ CountDownLatch(1).await() }), timeoutMillis = 20L)
        }
    }

    @Test
    fun `every scheduled update completes before the runner returns`() {
        val count = AtomicInteger()

        runWorkers(List(4) { { repeat(100) { count.incrementAndGet() } } })

        assertEquals(400, count.get())
    }

    @Test
    fun `shared stat updates preserve observations while snapshots are read`() {
        val result = measureConcurrency(varianceStatSpec, com.eignex.kumulant.core.Concurrency.Strict, 4, false, 64)

        assertEquals(0.0, result.countError)
        assertTrue(result.reads >= 1)
    }

    @Test
    fun `concurrent merges preserve all source observations`() {
        val result = measureConcurrency(sumStatSpec, com.eignex.kumulant.core.Concurrency.Strict, 4, true, 64)

        assertEquals(0.0, result.countError)
    }

    @Test
    fun `unsynchronized stats cannot be shared between writers`() {
        assertFailsWith<IllegalArgumentException> {
            measureConcurrency(sumStatSpec, com.eignex.kumulant.core.Concurrency.None, 4, false, 64)
        }
    }
}
