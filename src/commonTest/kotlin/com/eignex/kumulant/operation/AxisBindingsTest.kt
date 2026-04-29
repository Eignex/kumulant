package com.eignex.kumulant.operation

import com.eignex.kumulant.stat.regression.OLS

import com.eignex.kumulant.stat.summary.Sum

import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-9

class AxisBindingsTest {

    @Test
    fun `withTimeAsX feeds timestamp-in-seconds into x and value into y`() {
        val stat = OLS().withTimeAsX()
        stat.update(value = 10.0, timestampNanos = 1_000_000_000L)
        stat.update(value = 12.0, timestampNanos = 2_000_000_000L)
        stat.update(value = 14.0, timestampNanos = 3_000_000_000L)

        val result = stat.read()

        assertEquals(2.0, result.slope, DELTA)

        assertEquals(8.0, result.intercept, DELTA)
    }

    @Test
    fun `withTimeAsY feeds value into x and timestamp-in-seconds into y`() {
        val stat = OLS().withTimeAsY()
        stat.update(value = 10.0, timestampNanos = 1_000_000_000L)
        stat.update(value = 12.0, timestampNanos = 2_000_000_000L)
        stat.update(value = 14.0, timestampNanos = 3_000_000_000L)

        val result = stat.read()

        assertEquals(0.5, result.slope, DELTA)
    }

    @Test
    fun `withTimeAsX create preserves the time-axis binding`() {
        val original = OLS().withTimeAsX()
        val clone = original.create()
        clone.update(value = 10.0, timestampNanos = 1_000_000_000L)
        clone.update(value = 20.0, timestampNanos = 2_000_000_000L)

        val result = clone.read()
        assertEquals(10.0, result.slope, DELTA)
    }

    @Test
    fun `withTimeAsY create preserves the time-axis binding`() {
        val original = OLS().withTimeAsY()
        val clone = original.create()
        clone.update(value = 10.0, timestampNanos = 1_000_000_000L)
        clone.update(value = 20.0, timestampNanos = 2_000_000_000L)

        val result = clone.read()
        assertEquals(0.1, result.slope, DELTA)
    }

    @Test
    fun `withTimeAsX respects weight`() {
        val stat = OLS().withTimeAsX()
        stat.update(value = 10.0, timestampNanos = 1_000_000_000L, weight = 3.0)
        stat.update(value = 20.0, timestampNanos = 2_000_000_000L, weight = 3.0)

        val result = stat.read()
        assertEquals(6.0, result.totalWeights, DELTA)
    }

    @Test
    fun `withFixedX supplies constant x to paired stat`() {
        val stat = Sum().atX().withFixedX(7.0)
        stat.update(99.0)
        stat.update(99.0)
        assertEquals(14.0, stat.read().sum, DELTA)
    }

    @Test
    fun `withFixedY supplies constant y to paired stat`() {
        val stat = Sum().atY().withFixedY(3.0)
        stat.update(99.0)
        stat.update(99.0)
        assertEquals(6.0, stat.read().sum, DELTA)
    }
}
