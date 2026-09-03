package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.schema.expr.BoolExpr
import com.eignex.kumulant.schema.expr.eval
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Growth-time routing predicate over a feature [Row]. The tree engine
 * ([RegressionTree]) only ever calls [direction] to route an observation to a child, so
 * any feature representation can drive growth by supplying its own [Split]s — for example
 * a downstream library's typed, constraint-coupled splits over a non-vector row.
 *
 * The interface is intentionally **open and non-serializable**: it is the in-memory SPI
 * for growing trees over arbitrary features. Wire-portable splits over a dense
 * [F64VectorLike] context live under the [SerializableSplit] hierarchy, which the
 * serializable tree snapshots ([TreeNodeResult]) embed.
 */
interface Split<in Row> {
    /** Evaluate the predicate against the context [row]; true routes "pos", false "neg". */
    fun direction(row: Row): Boolean
}

/**
 * Wire-portable [Split] over a dense [F64VectorLike] context. Sealed + serializable so tree
 * snapshots round-trip cleanly through `kotlinx.serialization`. Built-in implementations:
 * [ThresholdSplit] (numeric `x[i] <= t`) and [ExprSplit] (wrapping a [BoolExpr]); callers
 * needing custom predicates compose them as [BoolExpr] AST nodes and wrap in [ExprSplit].
 */
@Serializable
sealed interface SerializableSplit : Split<F64VectorLike>

/** Route by `row[featureIndex] <= threshold`. Threshold is inclusive on the "pos" side. */
@Serializable
@SerialName("ThresholdSplit")
data class ThresholdSplit(
    /** Index into the context vector that the split inspects. */
    val featureIndex: Int,
    /** Inclusive threshold separating pos (<=) from neg (>). */
    val threshold: Double,
) : SerializableSplit {
    override fun direction(row: F64VectorLike): Boolean = row[featureIndex] <= threshold
    override fun toString(): String = "x[$featureIndex] <= $threshold"
}

/**
 * Route by an arbitrary [BoolExpr] evaluated against the context vector. The expression
 * sees the context's first coordinate as `X`, the second as `Y`, and the full vector
 * via `V(i)`; matching the existing kumulant AST conventions. Wire-portable through
 * skema's polymorphism on [BoolExpr].
 */
@Serializable
@SerialName("ExprSplit")
data class ExprSplit(
    /** Predicate expression over the context vector. */
    val expr: BoolExpr,
) : SerializableSplit {
    override fun direction(row: F64VectorLike): Boolean {
        val x = if (row.size >= 1) row[0] else 0.0
        val y = if (row.size >= 2) row[1] else 0.0
        return expr.eval(x, y, row)
    }
    override fun toString(): String = "expr($expr)"
}
