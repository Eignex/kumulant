package com.eignex.kumulant.stream

import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class CasMaxLongTest {

    @Test
    fun `casMax should accept strictly higher value`() {
        val cell = SerialLong(5L)
        casMax(cell, 10L)
        assertEquals(10L, cell.load())
    }

    @Test
    fun `casMax should reject equal value`() {
        val cell = SerialLong(7L)
        casMax(cell, 7L)
        assertEquals(7L, cell.load())
    }

    @Test
    fun `casMax should reject lower value`() {
        val cell = SerialLong(20L)
        casMax(cell, 3L)
        assertEquals(20L, cell.load())
    }

    @Test
    fun `casMax should accumulate running max across calls`() {
        val cell = SerialLong(0L)
        listOf(3L, 1L, 7L, 2L, 5L).forEach { casMax(cell, it) }
        assertEquals(7L, cell.load())
    }
}

class CasMaxDoubleTest {

    @Test
    fun `casMax should accept strictly higher value`() {
        val cell = SerialDouble(1.0)
        casMax(cell, 2.5)
        assertEquals(2.5, cell.load(), DELTA)
    }

    @Test
    fun `casMax should reject equal value`() {
        val cell = SerialDouble(2.0)
        casMax(cell, 2.0)
        assertEquals(2.0, cell.load(), DELTA)
    }

    @Test
    fun `casMax should ignore NaN candidate`() {
        val cell = SerialDouble(1.0)
        casMax(cell, Double.NaN)
        assertEquals(1.0, cell.load(), DELTA)
    }

    @Test
    fun `casMax should accept positive infinity`() {
        val cell = SerialDouble(1e300)
        casMax(cell, Double.POSITIVE_INFINITY)
        assertEquals(Double.POSITIVE_INFINITY, cell.load())
    }
}

class CasMinDoubleTest {

    @Test
    fun `casMin should accept strictly lower value`() {
        val cell = SerialDouble(5.0)
        casMin(cell, 2.0)
        assertEquals(2.0, cell.load(), DELTA)
    }

    @Test
    fun `casMin should reject equal value`() {
        val cell = SerialDouble(3.0)
        casMin(cell, 3.0)
        assertEquals(3.0, cell.load(), DELTA)
    }

    @Test
    fun `casMin should ignore NaN candidate`() {
        val cell = SerialDouble(1.0)
        casMin(cell, Double.NaN)
        assertEquals(1.0, cell.load(), DELTA)
    }

    @Test
    fun `casMin should accept negative infinity`() {
        val cell = SerialDouble(-1e300)
        casMin(cell, Double.NEGATIVE_INFINITY)
        assertEquals(Double.NEGATIVE_INFINITY, cell.load())
    }
}

class CasOrLongTest {

    @Test
    fun `casOr should set bits in empty cell`() {
        val cell = SerialLong(0L)
        casOr(cell, 0b0101L)
        assertEquals(0b0101L, cell.load())
    }

    @Test
    fun `casOr should accumulate bits across calls`() {
        val cell = SerialLong(0L)
        casOr(cell, 0b0001L)
        casOr(cell, 0b0010L)
        casOr(cell, 0b1000L)
        assertEquals(0b1011L, cell.load())
    }

    @Test
    fun `casOr should be no-op when all requested bits are already set`() {
        val cell = SerialLong(0b1111L)
        casOr(cell, 0b0101L)
        assertEquals(0b1111L, cell.load())
    }
}

class CasMaxLongArrayTest {

    @Test
    fun `casMax on array should act per slot`() {
        val arr = SerialLongArray(longArrayOf(0L, 0L, 0L))
        casMax(arr, 0, 5L)
        casMax(arr, 1, 7L)
        casMax(arr, 2, 1L)
        casMax(arr, 1, 3L)
        assertEquals(5L, arr.load(0))
        assertEquals(7L, arr.load(1))
        assertEquals(1L, arr.load(2))
    }
}

class CasMinLongArrayTest {

    @Test
    fun `casMin on array should act per slot`() {
        val arr = SerialLongArray(longArrayOf(100L, 100L, 100L))
        casMin(arr, 0, 50L)
        casMin(arr, 1, 25L)
        casMin(arr, 1, 60L)
        assertEquals(50L, arr.load(0))
        assertEquals(25L, arr.load(1))
        assertEquals(100L, arr.load(2))
    }
}

class CasOrLongArrayTest {

    @Test
    fun `casOr on array should act per slot`() {
        val arr = SerialLongArray(longArrayOf(0L, 0L))
        casOr(arr, 0, 0b0001L)
        casOr(arr, 0, 0b0010L)
        casOr(arr, 1, 0b1000L)
        assertEquals(0b0011L, arr.load(0))
        assertEquals(0b1000L, arr.load(1))
    }
}
