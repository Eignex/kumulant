package com.eignex.kumulant.bench

import com.eignex.koblas.Workspace
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class CholeskyRetryBenchmarkTest {
    @Test
    fun `positive definite fixture succeeds without a diagonal shift`() {
        val result = retryFactor(retryMatrix("success"), Workspace())

        assertNotNull(result.factor)
        assertEquals(1, result.attempts)
        assertEquals(0.0, result.shift)
    }

    @Test
    fun `retry fixture records the first successful diagonal shift`() {
        val result = retryFactor(retryMatrix("retry"), Workspace())

        assertNotNull(result.factor)
        assertEquals(4, result.attempts)
        assertEquals(1e-8, result.shift)
    }

    @Test
    fun `exhaustion fixture records the last failed diagonal shift`() {
        val result = retryFactor(retryMatrix("exhaustion"), Workspace())

        assertNull(result.factor)
        assertEquals(4, result.attempts)
        assertEquals(1e-8, result.shift)
    }
}
