package com.eignex.kumulant.stat.score

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

private const val DELTA = 1e-12

class PinballLossTest {

    @Test
    fun `tau equals one half reduces to half MAE`() {
        val stat = PinballLossStat(0.5).apply {
            update(x = 1.0, y = 3.0, timestampNanos = 0L, weight = 1.0) // diff = 2 → 0.5*2 = 1
            update(x = 4.0, y = 1.0, timestampNanos = 0L, weight = 1.0) // diff = -3 → -0.5 * -3 = 1.5
        }
        assertEquals(1.25, stat.read(0L).mean, DELTA)
    }

    @Test
    fun `tau equals 0_9 penalizes under-prediction more`() {
        val tau = 0.9
        val stat = PinballLossStat(tau).apply {
            // y > forecast (under-prediction): loss = tau * |diff|
            update(x = 0.0, y = 1.0, timestampNanos = 0L, weight = 1.0)
            // y < forecast (over-prediction): loss = (1-tau) * |diff|
            update(x = 1.0, y = 0.0, timestampNanos = 0L, weight = 1.0)
        }
        val expected = (tau * 1.0 + (1.0 - tau) * 1.0) / 2.0
        assertEquals(expected, stat.read(0L).mean, DELTA)
    }

    @Test
    fun `tau outside unit interval rejected`() {
        assertFailsWith<IllegalArgumentException> { PinballLossStat(-0.1) }
        assertFailsWith<IllegalArgumentException> { PinballLossStat(1.1) }
    }

    @Test
    fun `zero residual gives zero loss for any tau`() {
        for (tau in listOf(0.05, 0.5, 0.95)) {
            val stat = PinballLossStat(tau).apply { update(2.0, 2.0, 0L, 1.0) }
            assertEquals(0.0, stat.read(0L).mean, DELTA)
        }
    }
}
