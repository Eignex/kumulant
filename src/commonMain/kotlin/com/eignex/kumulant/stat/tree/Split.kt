package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.schema.BoolExpr

/**
 * Binary predicate routing a context vector to "pos" (true) or "neg" (false). The
 * interface is intentionally open so external libraries can plug in their own
 * predicate flavours — e.g. klause-coupled splits that read typed handles out of
 * a constraint-solver sample, or domain-specific splits that consult auxiliary
 * state alongside the context.
 *
 * Built-in implementations: [ThresholdSplit] (numeric `x[i] <= t`) and [ExprSplit]
 * (wrapping a wire-portable [BoolExpr] over the context vector). Wire serialization
 * is per-implementation; consumers needing portable splits should use one of those
 * two or supply their own `@Serializable` subtype.
 */
interface Split {
    /** Evaluate the predicate against the context [row]. */
    fun direction(row: VectorView): Boolean
}

/** Route by `row[featureIndex] <= threshold`. Threshold is inclusive on the "pos" side. */
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
