package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.summary.MeanStat
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The lock must be released when the guarded body throws.
 *
 * `Mutex.withLock` gave this for free. The `enter`/`exit` split that replaced it on the hot
 * path relies on an explicit `finally`, so a mistake there would not show up as a wrong
 * answer: it would leave the mutex held and deadlock the next writer. Several stats throw
 * from inside the guarded body - the weight guards in the Welford family, the dimension
 * checks in the vector family - so this is a reachable path, not a hypothetical.
 */
class GuardedLockReleaseTest {

    @Test
    fun `a throw inside the guarded body leaves the lock usable on the same thread`() {
        val mean = MeanStat(Concurrency.Strict)
        mean.update(10.0, weight = 1.0)
        // Exhausting the accumulated weight throws from inside the guarded body.
        assertFailsWith<IllegalArgumentException> { mean.update(10.0, weight = -1.0) }
        // If the lock had leaked, a ReentrantLock would let the owning thread back in and this
        // would pass regardless; the cross-thread case below is the real check.
        mean.update(30.0, weight = 1.0)
        assertEquals(2.0, mean.read().totalWeights, 1e-9)
        assertEquals(20.0, mean.read().mean, 1e-9)
    }

    @Test
    fun `a throw inside the guarded body does not wedge other threads`() {
        val mean = MeanStat(Concurrency.Strict)
        mean.update(10.0, weight = 1.0)
        assertFailsWith<IllegalArgumentException> { mean.update(10.0, weight = -1.0) }

        // A ReentrantLock is held by the thread that took it, so a leaked lock only becomes
        // visible from a different thread. Without the finally, this times out.
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
