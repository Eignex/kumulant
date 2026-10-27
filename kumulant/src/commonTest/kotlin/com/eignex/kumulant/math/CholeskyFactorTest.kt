package com.eignex.kumulant.math

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Workspace
import kotlin.math.abs
import kotlin.math.ln
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

class CholeskyFactorTest {
    @Test
    fun `vector destinations support independent and shared storage`() {
        val factor = DenseMatrix.wrap(2, 2, doubleArrayOf(4.0, 2.0, 2.0, 5.0)).factorCholesky()
        for (inPlace in listOf(false, true)) {
            val right = doubleArrayOf(8.0, 12.0)
            val out = if (inPlace) right else DoubleArray(2) { Double.NaN }

            val result = factor.solveInto(right, out, Workspace())

            assertSame(out, result)
            assertContentEquals(doubleArrayOf(1.0, 2.0), result)
        }
    }

    @Test
    fun `matrix destinations support independent and shared storage`() {
        val factor = DenseMatrix.wrap(2, 2, doubleArrayOf(4.0, 2.0, 2.0, 5.0)).factorCholesky()
        for (inPlace in listOf(false, true)) {
            val right = DenseMatrix.wrap(2, 2, doubleArrayOf(8.0, 12.0, -8.0, -12.0))
            val out = if (inPlace) DenseMatrix.wrap(2, 2, right.values) else DenseMatrix.zero(2, 2)

            val result = factor.solveInto(right, out, Workspace())

            assertSame(out, result)
            assertContentEquals(doubleArrayOf(1.0, 2.0, -1.0, -2.0), result.values)
        }
    }

    @Test
    fun `reusable triangular destinations support both orientations`() {
        val factor = DenseMatrix.wrap(2, 2, doubleArrayOf(4.0, 2.0, 2.0, 5.0)).factorCholesky()
        val workspace = Workspace()
        val out = DenseMatrix.zero(2, 1)
        for (transpose in listOf(false, true)) {
            val right = DenseMatrix.wrap(2, 1, if (transpose) doubleArrayOf(4.0, 4.0) else doubleArrayOf(2.0, 5.0))

            factor.solveLowerInto(right, out, transpose, workspace)

            assertContentEquals(doubleArrayOf(1.0, 2.0), out.values)
        }
    }

    @Test
    fun `reusable solves preserve independent inputs and factor storage`() {
        val factor = DenseMatrix.wrap(2, 2, doubleArrayOf(4.0, 2.0, 2.0, 5.0)).factorCholesky()
        val original = factor.lowerFactor().values
        val right = DenseMatrix.wrap(2, 1, doubleArrayOf(8.0, 12.0))
        val out = DenseMatrix.zero(2, 1)

        factor.solveInto(right, out)
        factor.solveLowerInto(right, out)
        factor.solveInto(right.values, out.values)

        assertContentEquals(doubleArrayOf(8.0, 12.0), right.values)
        assertContentEquals(original, factor.lowerFactor().values)
    }

    @Test
    fun `rank update factors the outer product added to the original matrix`() {
        val factor = DenseMatrix.wrap(2, 2, doubleArrayOf(4.0, 2.0, 2.0, 5.0)).factorCholesky()

        val updated = factor.rankUpdated(doubleArrayOf(1.0, -2.0), Workspace())

        val solution = updated.solve(doubleArrayOf(5.0, 18.0))
        assertEquals(1.0, solution[0], 1e-13)
        assertEquals(2.0, solution[1], 1e-13)
    }

    @Test
    fun `rank update preserves its vector and original factor`() {
        val factor = DenseMatrix.diagonal(2, 1.0).factorCholesky()
        val update = doubleArrayOf(1.0, -2.0)

        factor.rankUpdated(update)

        assertContentEquals(doubleArrayOf(1.0, -2.0), update)
        assertContentEquals(doubleArrayOf(1.0, 0.0, 0.0, 1.0), factor.lowerFactor().values)
    }

    @Test
    fun `rank update reports a nonrepresentable factor`() {
        val factor = DenseMatrix.diagonal(1, 1.0).factorCholesky().rankUpdated(doubleArrayOf(Double.MAX_VALUE))

        assertFailsWith<NotPositiveDefinite> { factor.rankUpdated(doubleArrayOf(Double.MAX_VALUE)) }
    }

    @Test
    fun `extension agrees with full factorization across block boundaries`() {
        for (workspace in listOf(Workspace(), null)) for (size in listOf(0, 1, 63, 64, 65, 127, 128, 129)) {
            val base = DenseMatrix.wrap(
                size,
                size,
                DoubleArray(size * size) { if (it % size == it / size) 2.25 else 0.25 },
            )
            val factor = base.factorCholesky()
            for (added in if (size < 2) listOf(0, 1, 4, 16) else listOf(1, 16)) {
                val cross = DenseMatrix.wrap(size, added, DoubleArray(size * added) { 0.25 })
                val diagonal = DenseMatrix.wrap(
                    added,
                    added,
                    DoubleArray(added * added) { if (it % added == it / added) 2.25 else 0.25 },
                )
                val order = size + added
                val full = DenseMatrix.wrap(
                    order,
                    order,
                    DoubleArray(order * order) { if (it % order == it / order) 2.25 else 0.25 },
                )
                val expected = full.factorCholesky().lowerFactor().values

                val actual = factor.extended(cross, diagonal, workspace).lowerFactor().values

                var error = 0.0
                for (i in actual.indices) error = maxOf(error, abs(actual[i] - expected[i]))
                assertTrue(error < 1e-12, "size=$size added=$added workspace=${workspace != null} factor error=$error")
            }
        }
    }

    @Test
    fun `extension owns its inputs and leaves the original factor unchanged`() {
        val factor = DenseMatrix.diagonal(2, 1.0).factorCholesky()
        val cross = DenseMatrix.wrap(2, 1, doubleArrayOf(0.5, 0.25))
        val diagonal = DenseMatrix.diagonal(1, 2.0)

        val extended = factor.extended(cross, diagonal)
        val original = extended.lowerFactor().values
        cross.values.fill(Double.NaN)
        diagonal.values.fill(Double.NaN)

        assertContentEquals(original, extended.lowerFactor().values)
        assertContentEquals(doubleArrayOf(1.0, 0.0, 0.0, 1.0), factor.lowerFactor().values)
    }

    @Test
    fun `extension preserves both supplied blocks`() {
        val factor = DenseMatrix.diagonal(2, 1.0).factorCholesky()
        val cross = DenseMatrix.wrap(2, 1, doubleArrayOf(0.5, 0.25))
        val diagonal = DenseMatrix.diagonal(1, 2.0)

        factor.extended(cross, diagonal, Workspace())

        assertContentEquals(doubleArrayOf(0.5, 0.25), cross.values)
        assertContentEquals(doubleArrayOf(2.0), diagonal.values)
    }

    @Test
    fun `extension reports the failing pivot in the enlarged matrix`() {
        val factor = DenseMatrix.diagonal(2, 1.0).factorCholesky()
        val diagonal = DenseMatrix.diagonal(doubleArrayOf(1.0, -1.0))

        val error = assertFailsWith<NotPositiveDefinite> { factor.extended(DenseMatrix.zero(2, 2), diagonal) }

        assertEquals(3, error.pivotIndex)
        assertEquals(-1.0, error.pivot)
        assertEquals(1, (error.cause as NotPositiveDefinite).pivotIndex)
    }

    @Test
    fun `workspace remains reusable after a rejected extension`() {
        val workspace = Workspace()
        val factor = DenseMatrix.diagonal(2, 1.0).factorCholesky()
        val cross = DenseMatrix.zero(2, 1)
        assertFailsWith<NotPositiveDefinite> { factor.extended(cross, DenseMatrix.diagonal(1, -1.0), workspace) }

        val result = factor.extended(cross, DenseMatrix.diagonal(1, 1.0), workspace)

        assertContentEquals(DenseMatrix.diagonal(3, 1.0).values, result.lowerFactor().values)
    }

    @Test
    fun `extension ignores the upper triangle of the diagonal block`() {
        val factor = DenseMatrix.diagonal(1, 1.0).factorCholesky()
        val diagonal = DenseMatrix.wrap(2, 2, doubleArrayOf(4.0, 0.0, Double.NaN, 9.0))

        val actual = factor.extended(DenseMatrix.zero(1, 2), diagonal).lowerFactor().values

        assertContentEquals(doubleArrayOf(1.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0, 0.0, 3.0), actual)
    }

    @Test
    fun `reusable methods reject incompatible shapes`() {
        val factor = DenseMatrix.diagonal(2, 1.0).factorCholesky()

        assertFailsWith<IllegalArgumentException> { factor.solveInto(DoubleArray(2), DoubleArray(1)) }
        assertFailsWith<IllegalArgumentException> { factor.solveInto(DenseMatrix.zero(2, 1), DenseMatrix.zero(2, 2)) }
        assertFailsWith<IllegalArgumentException> {
            factor.solveLowerInto(
                DenseMatrix.zero(3, 1),
                DenseMatrix.zero(3, 1),
            )
        }
        assertFailsWith<IllegalArgumentException> { factor.extended(DenseMatrix.zero(3, 1), DenseMatrix.zero(1, 1)) }
        assertFailsWith<IllegalArgumentException> { factor.extended(DenseMatrix.zero(2, 1), DenseMatrix.zero(2, 2)) }
        assertFailsWith<IllegalArgumentException> { factor.rankUpdated(DoubleArray(1)) }
    }

    @Test
    fun `updates and extensions reject nonfinite selected inputs`() {
        val factor = DenseMatrix.diagonal(1, 1.0).factorCholesky()
        for (value in listOf(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)) {
            assertFailsWith<IllegalArgumentException> { factor.rankUpdated(doubleArrayOf(value)) }
            assertFailsWith<IllegalArgumentException> {
                factor.extended(
                    DenseMatrix.wrap(1, 1, doubleArrayOf(value)),
                    DenseMatrix.diagonal(1, 1.0),
                )
            }
            assertFailsWith<IllegalArgumentException> {
                factor.extended(
                    DenseMatrix.zero(1, 1),
                    DenseMatrix.diagonal(1, value),
                )
            }
        }
    }

    @Test
    fun `workspace factorization preserves its input and returns an owned factor`() {
        val matrix = DenseMatrix.wrap(2, 2, doubleArrayOf(4.0, 2.0, Double.NaN, 5.0))
        val workspace = Workspace()

        val factor = matrix.factorCholesky(workspace)
        matrix.factorCholesky(workspace)

        assertContentEquals(doubleArrayOf(4.0, 2.0, Double.NaN, 5.0), matrix.values)
        matrix.values.fill(0.0)
        assertContentEquals(doubleArrayOf(2.0, 1.0, 0.0, 2.0), factor.lowerFactor().values)
    }

    @Test
    fun `factor owns its input and returned lower matrix`() {
        val matrix = DenseMatrix.wrap(2, 2, doubleArrayOf(4.0, 2.0, Double.NaN, 5.0))
        val original = matrix.values.copyOf()

        val factor = matrix.factorCholesky()
        val lower = factor.lowerFactor()

        assertContentEquals(original, matrix.values)
        assertContentEquals(doubleArrayOf(2.0, 1.0, 0.0, 2.0), lower.values)
        matrix.values.fill(0.0)
        lower.values.fill(0.0)
        assertContentEquals(doubleArrayOf(2.0, 1.0, 0.0, 2.0), factor.lowerFactor().values)
        assertEquals(2, factor.size)
        assertEquals(ln(16.0), factor.logDeterminant(), 1e-14)
    }

    @Test
    fun `vector and matrix systems recover their known solutions`() {
        val matrix = DenseMatrix.wrap(3, 3, doubleArrayOf(4.0, 2.0, -2.0, 2.0, 10.0, 5.0, -2.0, 5.0, 21.0))
        val factor = matrix.factorCholesky()
        val right = doubleArrayOf(10.0, 17.0, -13.0)
        val rights = DenseMatrix.wrap(3, 2, right + right.map { -it })
        val original = rights.values.copyOf()
        val expected = doubleArrayOf(1.0, 2.0, -1.0)

        val solution = factor.solve(right)
        val solutions = factor.solve(rights)

        for (i in expected.indices) {
            assertEquals(expected[i], solution[i], 1e-13)
            assertEquals(expected[i], solutions[i, 0], 1e-13)
            assertEquals(-expected[i], solutions[i, 1], 1e-13)
        }
        assertContentEquals(doubleArrayOf(10.0, 17.0, -13.0), right)
        assertContentEquals(original, rights.values)
    }

    @Test
    fun `lower triangular solves support either orientation`() {
        val matrix = DenseMatrix.wrap(3, 3, doubleArrayOf(4.0, 2.0, -2.0, 2.0, 10.0, 5.0, -2.0, 5.0, 21.0))
        val factor = matrix.factorCholesky()
        for (transpose in listOf(false, true)) {
            val right = if (transpose) doubleArrayOf(5.0, 4.0, -4.0) else doubleArrayOf(2.0, 7.0, -1.0)
            val original = right.copyOf()

            val solution = factor.solveLower(DenseMatrix.wrap(3, 1, right), transpose)

            for ((i, expected) in doubleArrayOf(1.0, 2.0, -1.0).withIndex()) {
                assertEquals(expected, solution[i, 0], 1e-13)
            }
            assertContentEquals(original, right)
        }
    }

    @Test
    fun `allocating matrix solves match destination solves across block boundaries`() {
        for (size in listOf(63, 129, 257)) {
            val matrix = DenseMatrix.wrap(
                size,
                size,
                DoubleArray(size * size) { if (it % size == it / size) 2.25 else 0.25 },
            )
            val factor = matrix.factorCholesky()
            for (columns in listOf(3, 16)) {
                val right = DenseMatrix.wrap(size, columns, DoubleArray(size * columns) { it % 7 - 3.0 })
                val original = right.values.copyOf()
                val out = DenseMatrix.zero(size, columns)

                val solved = factor.solve(right).values
                val lower = factor.solveLower(right).values
                val upper = factor.solveLower(right, transpose = true).values

                assertContentEquals(factor.solveInto(right, out, Workspace()).values, solved)
                assertContentEquals(factor.solveLowerInto(right, out, workspace = Workspace()).values, lower)
                assertContentEquals(factor.solveLowerInto(right, out, true, Workspace()).values, upper)
                assertContentEquals(original, right.values)
            }
        }
    }

    @Test
    fun `empty matrices and zero right hand sides are supported`() {
        val factor = DenseMatrix.zero(0, 0).factorCholesky()

        assertEquals(0, factor.size)
        assertEquals(0.0, factor.logDeterminant())
        assertTrue(factor.solve(doubleArrayOf()).isEmpty())
        assertEquals(3, factor.solve(DenseMatrix.zero(0, 3)).cols)
        val destination = DenseMatrix.zero(0, 3)
        assertSame(destination, factor.solveInto(DenseMatrix.zero(0, 3), destination, Workspace()))
        assertSame(destination, factor.solveLowerInto(destination, destination, workspace = Workspace()))
        assertTrue(factor.solveInto(doubleArrayOf(), doubleArrayOf(), Workspace()).isEmpty())
        assertEquals(0, factor.rankUpdated(doubleArrayOf(), Workspace()).size)
        assertEquals(0, DenseMatrix.diagonal(2, 4.0).factorCholesky().solve(DenseMatrix.zero(2, 0)).cols)
    }

    @Test
    fun `indefinite and singular matrices report their failing pivot without regularization`() {
        for (diagonal in listOf(0.0, -1.0)) {
            val matrix = DenseMatrix.diagonal(2, 1.0)
            matrix[1, 1] = diagonal

            val error = assertFailsWith<NotPositiveDefinite> { matrix.factorCholesky() }

            assertEquals(1, error.pivotIndex)
            assertEquals(diagonal, error.pivot)
        }
    }

    @Test
    fun `public factorization and solves reject invalid shapes and nonfinite input`() {
        assertFailsWith<IllegalArgumentException> { DenseMatrix.zero(2, 3).factorCholesky() }
        for (value in listOf(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)) {
            val matrix = DenseMatrix.diagonal(2, 1.0)
            matrix[1, 0] = value
            assertFailsWith<IllegalArgumentException> { matrix.factorCholesky() }
        }
        val factor = DenseMatrix.diagonal(2, 1.0).factorCholesky()
        assertFailsWith<IllegalArgumentException> { factor.solve(doubleArrayOf(1.0)) }
        assertFailsWith<IllegalArgumentException> { factor.solve(DenseMatrix.zero(3, 1)) }
        assertFailsWith<IllegalArgumentException> { factor.solveLower(DenseMatrix.zero(3, 1)) }
    }
}
