package com.eignex.kumulant.core

import com.eignex.kumulant.operation.atIndex
import com.eignex.kumulant.operation.atIndices
import com.eignex.kumulant.operation.atX
import com.eignex.kumulant.operation.atY
import com.eignex.kumulant.operation.filter
import com.eignex.kumulant.operation.foldPaired
import com.eignex.kumulant.operation.foldVector
import com.eignex.kumulant.operation.transformValue
import com.eignex.kumulant.operation.transformVector
import com.eignex.kumulant.operation.transformX
import com.eignex.kumulant.operation.transformY
import com.eignex.kumulant.operation.withFixedX
import com.eignex.kumulant.operation.withFixedY
import com.eignex.kumulant.operation.withWeight
import com.eignex.kumulant.stat.Mean
import com.eignex.kumulant.stat.Sum
import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class StatOperationsTest {

    @Test
    fun `transformValue applies transform before accumulation`() {
        val stat = Sum().transformValue { it * 2.0 }
        stat.update(3.0)
        stat.update(5.0)
        assertEquals(16.0, stat.read().sum, DELTA)
    }

    @Test
    fun `transformValue abs value`() {
        val stat = Sum().transformValue { if (it < 0) -it else it }
        stat.update(-4.0)
        stat.update(3.0)
        assertEquals(7.0, stat.read().sum, DELTA)
    }

    @Test
    fun `filter drops values that fail predicate`() {
        val stat = Sum().filter { it > 0.0 }
        stat.update(5.0)
        stat.update(-3.0)
        stat.update(2.0)
        assertEquals(7.0, stat.read().sum, DELTA)
    }

    @Test
    fun `filter passes all when predicate always true`() {
        val stat = Mean().filter { true }
        stat.update(1.0)
        stat.update(3.0)
        assertEquals(2.0, stat.read().mean, DELTA)
    }

    @Test
    fun `atX feeds x to underlying stat`() {
        val stat = Sum().atX()
        stat.update(10.0, 99.0)
        assertEquals(10.0, stat.read().sum, DELTA)
    }

    @Test
    fun `atY feeds y to underlying stat`() {
        val stat = Sum().atY()
        stat.update(99.0, 7.0)
        assertEquals(7.0, stat.read().sum, DELTA)
    }

    @Test
    fun `foldPaired computes difference y-x`() {
        val stat = Sum().foldPaired { x, y -> y - x }
        stat.update(3.0, 10.0)
        stat.update(1.0, 4.0)
        assertEquals(10.0, stat.read().sum, DELTA)
    }

    @Test
    fun `atIndex picks single element from vector`() {
        val stat = Sum().atIndex(1)
        stat.update(doubleArrayOf(10.0, 20.0, 30.0))
        assertEquals(20.0, stat.read().sum, DELTA)
    }

    @Test
    fun `atIndices extracts two elements for paired stat`() {
        val stat = Sum().atX().atIndices(0, 2)
        stat.update(doubleArrayOf(5.0, 99.0, 3.0))
        assertEquals(5.0, stat.read().sum, DELTA)
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

    @Test
    fun `filter on paired stat drops pairs failing predicate`() {
        val stat = Sum().atX().filter { x, _ -> x > 0.0 }
        stat.update(5.0, 1.0)
        stat.update(-3.0, 1.0)
        assertEquals(5.0, stat.read().sum, DELTA)
    }

    @Test
    fun `filter on vector stat drops vectors failing predicate`() {
        val stat = Sum().atIndex(0).filter { v -> v[0] > 0.0 }
        stat.update(doubleArrayOf(4.0))
        stat.update(doubleArrayOf(-2.0))
        assertEquals(4.0, stat.read().sum, DELTA)
    }

    @Test
    fun `series filter rejecting all leaves stat in zero state`() {
        val stat = Sum().filter { false }
        stat.update(1.0)
        stat.update(2.0)
        stat.update(3.0)
        assertEquals(0.0, stat.read().sum, DELTA)
    }

    @Test
    fun `paired filter rejecting all leaves stat in zero state`() {
        val stat = Sum().atX().filter { _, _ -> false }
        stat.update(1.0, 2.0)
        stat.update(3.0, 4.0)
        assertEquals(0.0, stat.read().sum, DELTA)
    }

    @Test
    fun `vector filter rejecting all leaves stat in zero state`() {
        val stat = Sum().atIndex(0).filter { _ -> false }
        stat.update(doubleArrayOf(1.0))
        stat.update(doubleArrayOf(2.0))
        assertEquals(0.0, stat.read().sum, DELTA)
    }

    @Test
    fun `series filter excludes NaN values`() {
        val stat = Sum().filter { !it.isNaN() }
        stat.update(1.0)
        stat.update(Double.NaN)
        stat.update(2.0)
        assertEquals(3.0, stat.read().sum, DELTA)
    }

    @Test
    fun `paired filter excludes pairs containing NaN`() {
        val stat = Sum().atY().filter { x, y -> !x.isNaN() && !y.isNaN() }
        stat.update(1.0, 10.0)
        stat.update(Double.NaN, 20.0)
        stat.update(3.0, Double.NaN)
        stat.update(4.0, 30.0)
        assertEquals(40.0, stat.read().sum, DELTA)
    }

    @Test
    fun `vector filter excludes vectors containing NaN`() {
        val stat = Sum().atIndex(0).filter { v -> v.none { it.isNaN() } }
        stat.update(doubleArrayOf(1.0, 2.0))
        stat.update(doubleArrayOf(Double.NaN, 3.0))
        stat.update(doubleArrayOf(4.0, 5.0))
        assertEquals(5.0, stat.read().sum, DELTA)
    }

    @Test
    fun `foldVector computes sum of elements`() {
        val stat = Sum().foldVector { v -> v.sum() }
        stat.update(doubleArrayOf(1.0, 2.0, 3.0))
        assertEquals(6.0, stat.read().sum, DELTA)
    }

    @Test
    fun `create of transformValue stat is independent`() {
        val s1 = Sum().transformValue { it * 10.0 }
        s1.update(1.0)
        val s2 = s1.create()
        s2.update(1.0)
        assertEquals(10.0, s1.read().sum, DELTA)
        assertEquals(10.0, s2.read().sum, DELTA)
    }

    @Test
    fun `reset on transformValue clears underlying stat`() {
        val stat = Sum().transformValue { it * 2.0 }
        stat.update(5.0)
        stat.reset()
        assertEquals(0.0, stat.read().sum, DELTA)
    }

    @Test
    fun `transformX applies transform only to x on paired stat`() {
        val stat = Sum().atX().transformX { it * 10.0 }
        stat.update(2.0, 999.0)
        assertEquals(20.0, stat.read().sum, DELTA)
    }

    @Test
    fun `transformY applies transform only to y on paired stat`() {
        val stat = Sum().atY().transformY { it + 3.0 }
        stat.update(100.0, 7.0)
        assertEquals(10.0, stat.read().sum, DELTA)
    }

    @Test
    fun `transformVector rewrites vector before delegate update`() {
        val stat = Sum().atIndex(0).transformVector { v ->
            doubleArrayOf(v[0] * 2.0, v[1])
        }
        stat.update(doubleArrayOf(4.0, 9.0))
        assertEquals(8.0, stat.read().sum, DELTA)
    }

    @Test
    fun `withWeight on paired stat overrides incoming weight`() {
        val stat = Sum().atX().withWeight(3.0)
        stat.update(2.0, 9.0, weight = 100.0)
        assertEquals(6.0, stat.read().sum, DELTA)
    }

    @Test
    fun `withWeight on vector stat overrides incoming weight`() {
        val stat = Sum().atIndex(0).withWeight(4.0)
        stat.update(doubleArrayOf(2.5), weight = 0.1)
        assertEquals(10.0, stat.read().sum, DELTA)
    }
}
