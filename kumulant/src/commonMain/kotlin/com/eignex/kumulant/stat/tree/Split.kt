package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.schema.BoolExpr
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Binary predicate routing a context vector to "pos" (true) or "neg" (false).
 *
 * Built-in implementations: [ThresholdSplit] (numeric `x[i] <= t`) and [ExprSplit]
 * (wrapping a wire-portable [BoolExpr] over the context vector). The interface is
 * sealed so tree snapshots round-trip cleanly through `kotlinx.serialization`;
 * callers needing custom predicates compose them as [BoolExpr] AST nodes and wrap
 * them in [ExprSplit].
 */
@Serializable
sealed interface Split {
    /** Evaluate the predicate against the context [row]. */
    fun direction(row: VectorView): Boolean
}

/** Route by `row[featureIndex] <= threshold`. Threshold is inclusive on the "pos" side. */
@Serializable
@SerialName("ThresholdSplit")
data class ThresholdSplit(
    /** Index into the context vector that the split inspects. */
    val featureIndex: Int,
    /** Inclusive threshold separating pos (<=) from neg (>). */
    val threshold: Double,
) : Split {
    override fun direction(row: VectorView): Boolean = row[featureIndex] <= threshold
    override fun toString(): String = "x[$featureIndex] <= $threshold"
}

/**
 * Route by an arbitrary [BoolExpr] evaluated against the context vector. The expression
 * sees the context's first coordinate as `X`, the second as `Y`, and the full vector
 * via `V(i)` — matching the existing kumulant AST conventions. Wire-portable through
 * skema's polymorphism on [BoolExpr].
 */
@Serializable
@SerialName("ExprSplit")
data class ExprSplit(
    /** Predicate expression over the context vector. */
    val expr: BoolExpr,
) : Split {
    override fun direction(row: VectorView): Boolean {
        val arr = row.toDoubleArray()
        val x = if (arr.isNotEmpty()) arr[0] else 0.0
        val y = if (arr.size >= 2) arr[1] else 0.0
        return expr.eval(x, y, arr)
    }
    override fun toString(): String = "expr($expr)"
}
