package com.eignex.kumulant.stat.regression

import kotlin.test.Test
import kotlin.test.assertEquals

class CovarianceResultTest {

    @Test
    fun `varX and varY return zero when no weight has accumulated`() {
        val r = CovarianceResult(totalWeights = 0.0, meanX = 0.0, meanY = 0.0, sxy = 5.0, sxx = 3.0, syy = 7.0)
        assertEquals(0.0, r.varX, 1e-12)
        assertEquals(0.0, r.varY, 1e-12)
        assertEquals(0.0, r.covariance, 1e-12)
    }

    @Test
    fun `varX and varY divide squared deviations by totalWeights`() {
        val r = CovarianceResult(totalWeights = 4.0, meanX = 0.0, meanY = 0.0, sxy = 2.0, sxx = 8.0, syy = 12.0)
        assertEquals(2.0, r.varX, 1e-12)
        assertEquals(3.0, r.varY, 1e-12)
        assertEquals(0.5, r.covariance, 1e-12)
    }

    @Test
    fun `correlation is zero when either denominator factor is zero`() {
        val r = CovarianceResult(totalWeights = 4.0, meanX = 0.0, meanY = 0.0, sxy = 1.0, sxx = 0.0, syy = 4.0)
        assertEquals(0.0, r.correlation, 1e-12)
    }
}
