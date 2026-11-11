package com.eignex.kumulant.stat.regression

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
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

    @Test
    fun `serialized snapshots retain covariance evidence for continued updates`() {
        val source = CovarianceStat().apply {
            update(1.0, 2.0)
            update(3.0, 5.0, weight = 2.0)
        }
        val snapshot = Json.decodeFromString<CovarianceResult>(Json.encodeToString(source.read()))
        val restored = CovarianceStat().apply { merge(snapshot) }

        source.update(8.0, 11.0)
        restored.update(8.0, 11.0)

        assertEquals(source.read().covariance, restored.read().covariance, 1e-12)
        assertEquals(source.read().correlation, restored.read().correlation, 1e-12)
    }
}
