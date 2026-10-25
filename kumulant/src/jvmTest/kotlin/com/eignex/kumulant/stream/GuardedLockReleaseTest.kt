package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.summary.MeanStat
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class GuardedLockReleaseTest {

    @Test
    fun `a throw inside the guarded body leaves the lock usable on the same thread`() {
        val mean = MeanStat(Concurrency.Strict)
        mean.update(10.0, weight = 1.0)
        assertFailsWith<IllegalArgumentException> { mean.update(10.0, weight = -1.0) }
        mean.update(30.0, weight = 1.0)
        assertEquals(2.0, mean.read().totalWeights, 1e-9)
        assertEquals(20.0, mean.read().mean, 1e-9)
    }

    @Test
    fun `a throw inside the guarded body does not wedge other threads`() {
        val mean = MeanStat(Concurrency.Strict)
        mean.update(10.0, weight = 1.0)
        assertFailsWith<IllegalArgumentException> { mean.update(10.0, weight = -1.0) }

        // A leaked ReentrantLock only blocks a different thread; re-entry by its owner would pass.
        val pool = Executors.newSingleThreadExecutor()
        try {
            val future = pool.submit { mean.update(30.0, weight = 1.0) }
            future.get(5, TimeUnit.SECONDS)
        } finally {
            pool.shutdownNow()
        }
        assertEquals(2.0, mean.read().totalWeights, 1e-9)
    }

    @Test
    fun `reads still complete after a failed update`() {
        val mean = MeanStat(Concurrency.Strict)
        mean.update(5.0, weight = 1.0)
        assertFailsWith<IllegalArgumentException> { mean.update(5.0, weight = -2.0) }
        val pool = Executors.newSingleThreadExecutor()
        try {
            val future = pool.submit<Double> { mean.read().mean }
            assertTrue(future.get(5, TimeUnit.SECONDS).isFinite())
        } finally {
            pool.shutdownNow()
        }
    }
}
