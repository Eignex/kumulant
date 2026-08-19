package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class EwmaVarianceStatTest {

    @Test
    fun `EwmaVarianceStat create produces fresh independent stat`() {
        val v1 = EwmaVarianceStat(alpha = 0.5).apply {
            update(1.0)
            update(2.0)
        }
        val v2 = v1.create()
        repeat(10) { v1.update(1000.0) }
        assertEquals(0.0, v2.read().totalWeights, DELTA)
        assertTrue(v1.read().totalWeights > 0.0)
    }

    @Test
    fun `EwmaVarianceStat tracking volatility shift`() {
        val stat = EwmaVarianceStat(alpha = 0.1)

        repeat(50) { stat.update(10.0, 1.0) }
        val lowVar = stat.read().variance

        stat.update(1000.0, 1.0)
        val highVar = stat.read().variance

        assertTrue(highVar > lowVar, "VarianceStat should spike on outlier")
    }

    @Test
    fun `EwmaVarianceStat empty merge`() {
        val stat = EwmaVarianceStat(alpha = 0.1)
        stat.update(10.0, 1.0)
        stat.update(20.0, 1.0)
        val currentVar = stat.read().variance

        stat.merge(WeightedVarianceResult(0.0, 0.0, 0.0))

        assertEquals(currentVar, stat.read().variance, DELTA)
    }

    @Test
    fun `EwmaVarianceStat bias correction prevents zero division`() {
        val stat = EwmaVarianceStat(alpha = 0.1)

        assertEquals(0.0, stat.read().mean, DELTA)
        assertEquals(0.0, stat.read().variance, DELTA)
    }

    @Test
    fun `EwmaVarianceStat reset clears state`() {
        val varStat = EwmaVarianceStat(alpha = 0.5)
        varStat.update(10.0)
        varStat.update(20.0)
        varStat.reset()
        assertEquals(0.0, varStat.read().mean, DELTA)
        assertEquals(0.0, varStat.read().variance, DELTA)
        assertEquals(0.0, varStat.read().totalWeights, DELTA)
    }

    @Test
    fun `EwmaVarianceStat before any update returns zero state`() {
        val v = EwmaVarianceStat(alpha = 0.1)
        val r = v.read()
        assertEquals(0.0, r.totalWeights, DELTA)
        assertEquals(0.0, r.mean, DELTA)
        assertEquals(0.0, r.variance, DELTA)
    }

    @Test
    fun `a constant stream has zero variance whatever its level`() {
        val stat = EwmaVarianceStat(alpha = 0.1)
        repeat(25) { stat.update(100.0) }
        assertEquals(0.0, stat.read(0L).variance, 1e-9)
    }

    @Test
    fun `variance is invariant to shifting the whole stream`() {
        fun varianceOf(offset: Double): Double {
            val stat = EwmaVarianceStat(alpha = 0.2)
            for (v in listOf(1.0, 3.0, 2.0, 5.0, 4.0)) stat.update(v + offset)
            return stat.read(0L).variance
        }
        assertEquals(varianceOf(0.0), varianceOf(1000.0), 1e-6)
    }
}
