package com.eignex.kumulant.schema

import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.schema.expr.V
import com.eignex.kumulant.schema.expr.VElements
import com.eignex.kumulant.schema.expr.eval
import com.eignex.kumulant.schema.expr.gt
import com.eignex.kumulant.schema.expr.plus
import com.sun.management.ThreadMXBean
import java.lang.management.ManagementFactory
import kotlin.test.Test
import kotlin.test.assertTrue

class ExprAllocationTest {

    private class Vector(private val values: DoubleArray) : F64VectorLike {
        override val size: Int get() = values.size
        override fun get(i: Int): Double = values[i]
        override fun toDoubleArray(): DoubleArray = error("unexpected materialisation")
    }

    private val bean = ManagementFactory.getThreadMXBean() as ThreadMXBean

    private fun bytesPerCall(input: F64VectorLike, body: (F64VectorLike) -> Unit): Double {
        repeat(50_000) { body(input) }
        val id = Thread.currentThread().threadId()
        var best = Double.MAX_VALUE
        repeat(5) {
            val before = bean.getThreadAllocatedBytes(id)
            repeat(100_000) { body(input) }
            best = minOf(best, (bean.getThreadAllocatedBytes(id) - before).toDouble() / 100_000)
        }
        return best
    }

    @Test
    fun `vector aware scalar bool and fixed output vector evaluation do not scale allocations with input width`() {
        val scalar = V(0) + V(1)
        val predicate = V(0) gt 0.0
        val vector = VElements(listOf(V(0)))
        fun measure(size: Int): Double = bytesPerCall(Vector(DoubleArray(size) { 1.0 })) { input ->
            scalar.eval(v = input)
            predicate.eval(v = input)
            vector.eval(v = input)
        }

        val baseline = measure(8)
        for (size in listOf(32, 128, 512)) {
            val bytes = measure(size)
            assertTrue(bytes - baseline <= 32.0, "input-width allocation grew from $baseline B to $bytes B at $size")
        }
    }
}
