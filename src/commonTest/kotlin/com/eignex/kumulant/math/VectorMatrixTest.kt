package com.eignex.kumulant.math

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class VectorMatrixTest {

    @Test
    fun `DenseVector zero factory builds a zero-filled vector`() {
        val z = DenseVector.zero(3)
        assertEquals(3, z.size)
        for (i in 0 until 3) assertEquals(0.0, z[i])
    }

    @Test
    fun `DenseVector of copies its input`() {
        val src = doubleArrayOf(1.0, 2.0, 3.0)
        val v = DenseVector.of(src)
        src[0] = 99.0
        assertEquals(1.0, v[0])
    }

    @Test
    fun `DenseVector toDoubleArray returns a copy`() {
        val v = DenseVector.of(doubleArrayOf(1.0, 2.0))
        val copy = v.toDoubleArray()
        copy[0] = 99.0
        assertEquals(1.0, v[0])
    }

    @Test
    fun `DenseVector equals respects content`() {
        val a = DenseVector.of(doubleArrayOf(1.0, 2.0))
        val b = DenseVector.of(doubleArrayOf(1.0, 2.0))
        val c = DenseVector.of(doubleArrayOf(1.0, 3.0))
        assertEquals(a, a)
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
        assertNotEquals(a, c)
        assertNotEquals<Any?>(a, "not-a-vector")
    }

    @Test
    fun `DenseVector toString includes size`() {
        assertTrue("size=2" in DenseVector.zero(2).toString())
    }

    @Test
    fun `SparseVector rejects mismatched arrays`() {
        assertFailsWith<IllegalArgumentException> {
            SparseVector.of(5, intArrayOf(0, 1), doubleArrayOf(1.0))
        }
    }

    @Test
    fun `SparseVector of copies its inputs`() {
        val idx = intArrayOf(0, 2)
        val vals = doubleArrayOf(1.0, 3.0)
        val v = SparseVector.of(5, idx, vals)
        idx[0] = 4
        vals[1] = 99.0
        assertEquals(1.0, v[0])
        assertEquals(3.0, v[2])
    }

    @Test
    fun `SparseVector get returns zero for missing indices`() {
        val v = SparseVector.of(4, intArrayOf(0, 2), doubleArrayOf(5.0, 7.0))
        assertEquals(5.0, v[0])
        assertEquals(0.0, v[1])
        assertEquals(7.0, v[2])
        assertEquals(0.0, v[3])
    }

    @Test
    fun `SparseVector toDoubleArray materialises stored entries`() {
        val v = SparseVector.of(4, intArrayOf(1, 3), doubleArrayOf(2.0, 4.0))
        assertTrue(v.toDoubleArray().contentEquals(doubleArrayOf(0.0, 2.0, 0.0, 4.0)))
    }

    @Test
    fun `SparseVector equals and hashCode respect content`() {
        val a = SparseVector.of(4, intArrayOf(0, 2), doubleArrayOf(1.0, 3.0))
        val b = SparseVector.of(4, intArrayOf(0, 2), doubleArrayOf(1.0, 3.0))
        val different = SparseVector.of(4, intArrayOf(0, 2), doubleArrayOf(1.0, 4.0))
        val sizeDiff = SparseVector.of(5, intArrayOf(0, 2), doubleArrayOf(1.0, 3.0))
        assertEquals(a, a)
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
        assertNotEquals(a, different)
        assertNotEquals(a, sizeDiff)
        assertNotEquals<Any?>(a, "x")
    }

    @Test
    fun `SparseVector toString includes nnz`() {
        val s = SparseVector.of(4, intArrayOf(0, 3), doubleArrayOf(1.0, 1.0)).toString()
        assertTrue("nnz=2" in s)
    }

    @Test
    fun `DenseMatrix toArray on 0x0 returns empty array`() {
        val m = DenseMatrix(0, 0)
        assertEquals(0, m.toArray().size)
    }

    @Test
    fun `DenseMatrix of rejects ragged rows`() {
        assertFailsWith<IllegalArgumentException> {
            DenseMatrix.of(arrayOf(doubleArrayOf(1.0, 2.0), doubleArrayOf(3.0)))
        }
    }

    @Test
    fun `DenseMatrix of supports an empty row set`() {
        val m = DenseMatrix.of(arrayOf())
        assertEquals(0, m.rows)
        assertEquals(0, m.cols)
    }

    @Test
    fun `DenseMatrix diagonal seeds entries`() {
        val m = DenseMatrix.diagonal(3, 2.5)
        for (i in 0 until 3) for (j in 0 until 3) {
            assertEquals(if (i == j) 2.5 else 0.0, m[i, j])
        }
    }

    @Test
    fun `DenseMatrix toArray returns a fresh copy`() {
        val m = DenseMatrix.diagonal(2, 1.0)
        val arr = m.toArray()
        arr[0][0] = 99.0
        assertEquals(1.0, m[0, 0])
    }

    @Test
    fun `DenseMatrix equals and hashCode respect content`() {
        val a = DenseMatrix.diagonal(2, 1.0)
        val b = DenseMatrix.diagonal(2, 1.0)
        val c = DenseMatrix.diagonal(2, 2.0)
        val sizeDiff = DenseMatrix.diagonal(3, 1.0)
        assertEquals(a, a)
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
        assertNotEquals(a, c)
        assertNotEquals(a, sizeDiff)
        assertNotEquals<Any?>(a, "x")
    }

    @Test
    fun `DenseMatrix toString shows shape`() {
        assertTrue("2x3" in DenseMatrix(2, 3).toString())
    }

    @Test
    fun `axpy with alpha zero is a no-op`() {
        val y = DenseVector.of(doubleArrayOf(1.0, 2.0, 3.0))
        axpy(y, 0.0, DenseVector.of(doubleArrayOf(9.0, 9.0, 9.0)))
        assertTrue(y.toDoubleArray().contentEquals(doubleArrayOf(1.0, 2.0, 3.0)))
    }

    @Test
    fun `addOuter with alpha zero is a no-op`() {
        val M = DenseMatrix.diagonal(2, 1.0)
        addOuter(M, 0.0, DenseVector.of(doubleArrayOf(1.0, 1.0)), DenseVector.of(doubleArrayOf(1.0, 1.0)))
        for (i in 0 until 2) for (j in 0 until 2) {
            assertEquals(if (i == j) 1.0 else 0.0, M[i, j])
        }
    }

    @Test
    fun `addOuter dense skips rows where x is zero`() {
        val M = DenseMatrix.diagonal(3, 0.0)
        addOuter(M, 1.0, DenseVector.of(doubleArrayOf(0.0, 2.0, 0.0)), DenseVector.of(doubleArrayOf(1.0, 1.0, 1.0)))
        assertEquals(0.0, M[0, 0]); assertEquals(0.0, M[0, 1]); assertEquals(0.0, M[0, 2])
        assertEquals(2.0, M[1, 0]); assertEquals(2.0, M[1, 1]); assertEquals(2.0, M[1, 2])
        assertEquals(0.0, M[2, 0]); assertEquals(0.0, M[2, 1]); assertEquals(0.0, M[2, 2])
    }

    @Test
    fun `addOuter sparse skips zero stored entries`() {
        val M = DenseMatrix.diagonal(3, 0.0)
        addOuter(
            M,
            1.0,
            SparseVector.of(3, intArrayOf(0, 1), doubleArrayOf(0.0, 1.0)),
            SparseVector.of(3, intArrayOf(2), doubleArrayOf(5.0)),
        )
        for (i in 0 until 3) for (j in 0 until 3) {
            assertEquals(if (i == 1 && j == 2) 5.0 else 0.0, M[i, j], "M[$i,$j]")
        }
    }

    @Test
    fun `dot rejects mismatched sizes`() {
        assertFailsWith<IllegalArgumentException> {
            DenseVector.of(doubleArrayOf(1.0)) dot DenseVector.of(doubleArrayOf(1.0, 2.0))
        }
    }

    @Test
    fun `axpy rejects mismatched sizes`() {
        assertFailsWith<IllegalArgumentException> {
            axpy(DenseVector.of(doubleArrayOf(1.0)), 1.0, DenseVector.of(doubleArrayOf(1.0, 2.0)))
        }
    }

    @Test
    fun `addOuter rejects mismatched shapes`() {
        assertFailsWith<IllegalArgumentException> {
            addOuter(
                DenseMatrix(2, 2),
                1.0,
                DenseVector.of(doubleArrayOf(1.0, 2.0, 3.0)),
                DenseVector.of(doubleArrayOf(1.0, 2.0)),
            )
        }
    }

    @Test
    fun `matVec rejects mismatched shapes`() {
        assertFailsWith<IllegalArgumentException> {
            matVec(DenseMatrix(2, 3), DenseVector.of(doubleArrayOf(1.0, 2.0)))
        }
    }

    @Test
    fun `mathBackend identifier is non-empty`() {
        assertTrue(mathBackend.isNotEmpty())
    }
}
