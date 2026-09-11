package com.eignex.kumulant.schema

import com.eignex.koblas.Vector
import com.eignex.kumulant.bytesPerCall
import com.eignex.kumulant.schema.expr.V
import com.eignex.kumulant.schema.expr.VElements
import com.eignex.kumulant.schema.expr.eval
import com.eignex.kumulant.schema.expr.gt
import com.eignex.kumulant.schema.expr.plus
import kotlin.test.Test
import kotlin.test.assertTrue

class ExprAllocationTest {

    private class AllocationVector(private val values: DoubleArray) : Vector {
        override val size: Int get() = values.size
        override fun get(i: Int): Double = values[i]
        override fun toDoubleArray(): DoubleArray = error("unexpected materialisation")
    }

    @Test
    fun `vector aware scalar bool and fixed output vector evaluation do not scale allocations with input width`() {
        val scalar = V(0) + V(1)
        val predicate = V(0) gt 0.0
        val vector = VElements(listOf(V(0)))
        fun evaluate(input: Vector) {
            scalar.eval(v = input)
            predicate.eval(v = input)
            vector.eval(v = input)
        }
        val widths = intArrayOf(8, 32, 128, 512)
        val inputs = widths.map { width -> AllocationVector(DoubleArray(width) { 1.0 }) }

        val bytes = bytesPerCall(
            { evaluate(inputs[0]) },
            { evaluate(inputs[1]) },
            { evaluate(inputs[2]) },
            { evaluate(inputs[3]) },
        )

        val baseline = bytes[0]
        for (w in 1 until widths.size) {
            assertTrue(
                bytes[w] - baseline <= 32.0,
                "input-width allocation grew from $baseline B to ${bytes[w]} B at ${widths[w]}",
            )
        }
    }
}
