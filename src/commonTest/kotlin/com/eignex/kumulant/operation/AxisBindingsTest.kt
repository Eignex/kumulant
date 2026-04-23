package com.eignex.kumulant.operation

import com.eignex.kumulant.stat.OLS
import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-9

class AxisBindingsTest {

    @Test
    fun `withTimeAsX feeds timestamp-in-seconds into x and value into y`() {
        val stat = OLS().withTimeAsX()
        stat.update(value = 10.0, timestampNanos = 1_000_000_000L) // t = 1.0 s
        stat.update(value = 12.0, timestampNanos = 2_000_000_000L) // t = 2.0 s
        stat.update(value = 14.0, timestampNanos = 3_000_000_000L) // t = 3.0 s

        val result = stat.read()
        // Slope of y over t: values grow by 2 per second.
        assertEquals(2.0, result.slope, DELTA)
        // Intercept: y = 2t + 8.
        assertEquals(8.0, result.intercept, DELTA)
    }

    @Test
    fun `withTimeAsY feeds value into x and timestamp-in-seconds into y`() {
        val stat = OLS().withTimeAsY()
        stat.update(value = 10.0, timestampNanos = 1_000_000_000L)
        stat.update(value = 12.0, timestampNanos = 2_000_000_000L)
        stat.update(value = 14.0, timestampNanos = 3_000_000_000L)

        val result = stat.read()
        // Slope of t over y: t grows by 1 second per 2 value-units = 0.5.
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
}
