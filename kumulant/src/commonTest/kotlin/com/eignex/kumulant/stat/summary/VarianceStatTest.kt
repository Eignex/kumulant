package com.eignex.kumulant.stat.summary

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private const val DELTA = 1e-12

class VarianceStatTest {
    @Test
    fun `create produces fresh independent stat`() {
        val v1 = VarianceStat().apply {
            update(10.0)
            update(20.0)
        }
        val v2 = v1.create()
        v1.update(30.0)
        assertEquals(3.0, v1.read().totalWeights, DELTA)
        assertEquals(0.0, v2.read().totalWeights, DELTA)
    }

    @Test
    fun `test variance sequence`() {
        val vari = VarianceStat()
        (1..10).forEach { vari.update(it.toDouble(), 1.0) }
        assertEquals(8.25, vari.read().variance, DELTA)
    }

    @Test
    fun `test variance methods`() {
        val vari = VarianceStat()
        vari.update(10.0, 1.0)
        vari.update(20.0, 1.0)
        val result = vari.read()
        assertEquals(2.0, result.totalWeights, DELTA)
        assertEquals(15.0, result.mean, DELTA)
        assertEquals(50.0, result.sst, DELTA)
        assertEquals(25.0, result.variance, DELTA)
        assertEquals(5.0, result.stdDev, DELTA)
    }

    @Test
    fun `test single value variance`() {
        val vari = VarianceStat()
        vari.update(10.0, 1.0)
        assertTrue(vari.read().variance.isNaN() || vari.read().variance == 0.0)
    }

    @Test
    fun `test zero variance`() {
        val vari = VarianceStat()
        repeat(10) { vari.update(5.0) }
        assertEquals(5.0, vari.read().mean, DELTA)
        assertEquals(0.0, vari.read().variance, DELTA)
    }

    @Test
    fun `test merge`() {
        val v1 =
            VarianceStat().apply { (1..5).forEach { update(it.toDouble(), 1.0) } }
        val v2 =
            VarianceStat().apply { (6..10).forEach { update(it.toDouble(), 1.0) } }

        v1.merge(v2.read())
        assertEquals(8.25, v1.read().variance, DELTA)
        assertEquals(5.5, v1.read().mean, DELTA)
    }

    @Test
    fun `test empty merge`() {
        val v1 = VarianceStat()
        v1.update(1.0)
        v1.merge(VarianceStat().read())
        assertEquals(1.0, v1.read().totalWeights, DELTA)
    }

    @Test
    fun `test reset`() {
        val vari = VarianceStat()
        vari.update(10.0)
        vari.update(20.0)
        vari.reset()
        assertEquals(0.0, vari.read().totalWeights, DELTA)
        assertEquals(0.0, vari.read().mean, DELTA)
        assertEquals(0.0, vari.read().variance, DELTA)
    }

    @Test
    fun `read before any update returns zero variance and zero mean`() {
        val v = VarianceStat().read()
        assertEquals(0.0, v.totalWeights, DELTA)
        assertEquals(0.0, v.mean, DELTA)
        assertEquals(0.0, v.variance, DELTA)
    }

    @Test
    fun `variance over constant stream is zero`() {
        val v = VarianceStat()
        repeat(100) { v.update(7.0) }
        assertEquals(0.0, v.read().variance, 1e-6)
    }

    @Test
    fun `handles large magnitudes without overflow`() {
        val v = VarianceStat()
        val large = 1e9
        v.update(large)
        v.update(-large)
        val result = v.read()
        assertFalse(result.variance.isNaN())
        assertFalse(result.variance.isInfinite())
        assertEquals(1e18, result.variance, 1e12)
    }
}
