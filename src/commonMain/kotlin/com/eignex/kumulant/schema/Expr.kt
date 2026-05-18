package com.eignex.kumulant.schema

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.pow
import kotlin.math.sqrt

private val EMPTY_VECTOR = DoubleArray(0)

/**
 * Wire-serializable AST for scalar expressions over the input env: a primary
 * `x`, an optional `y` (paired stats), and an optional `v` (vector stats).
 * Nodes are pure data; evaluation is recursive interpretation.
 *
 * Polymorphic via skema's `$type` discriminator. The DSL extensions in this
 * file ([Double.times], `infix gt`, etc.) make construction more readable.
 */
@Serializable
sealed interface ScalarExpr {
    /** Evaluate this expression against the per-update inputs. */
    fun eval(x: Double, y: Double = 0.0, v: DoubleArray = EMPTY_VECTOR): Double
}

/** Refers to the primary scalar input `x`. */
@Serializable
@SerialName("X")
data object X : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = x
}

/** Refers to the secondary scalar input `y` (paired stats only). */
@Serializable
@SerialName("Y")
data object Y : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = y
}

/** `v[index]` - out-of-bounds throws at eval time. */
@Serializable
@SerialName("V")
data class V(
    /** Vector coordinate to read. */
    val index: Int,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = v[index]
}

/** Wire spec for a constant scalar. */
@Serializable
@SerialName("Const")
data class Const(
    /** Literal value returned by [eval]. */
    val v: Double,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = this.v
}

/** Wire spec for `l + r`. */
@Serializable
@SerialName("Add")
data class Add(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = l.eval(x, y, v) + r.eval(x, y, v)
}

/** Wire spec for `l - r`. */
@Serializable
@SerialName("Sub")
data class Sub(
    /** Left operand (minuend). */
    val l: ScalarExpr,
    /** Right operand (subtrahend). */
    val r: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = l.eval(x, y, v) - r.eval(x, y, v)
}

/** Wire spec for `l * r`. */
@Serializable
@SerialName("Mul")
data class Mul(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = l.eval(x, y, v) * r.eval(x, y, v)
}

/** Wire spec for `l / r`. */
@Serializable
@SerialName("Div")
data class Div(
    /** Dividend. */
    val l: ScalarExpr,
    /** Divisor. */
    val r: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = l.eval(x, y, v) / r.eval(x, y, v)
}

/** Wire spec for `-a`. */
@Serializable
@SerialName("Neg")
data class Neg(
    /** Operand to negate. */
    val a: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = -a.eval(x, y, v)
}

/** Wire spec for `|a|`. */
@Serializable
@SerialName("Abs")
data class Abs(
    /** Operand whose absolute value is returned. */
    val a: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = abs(a.eval(x, y, v))
}

/** Wire spec for the natural logarithm `ln(a)`. */
@Serializable
@SerialName("Log")
data class Log(
    /** Operand passed to `ln`. */
    val a: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = ln(a.eval(x, y, v))
}

/** Wire spec for `exp(a)`. */
@Serializable
@SerialName("Exp")
data class Exp(
    /** Operand passed to `exp`. */
    val a: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = exp(a.eval(x, y, v))
}

/** Wire spec for `sqrt(a)`. */
@Serializable
@SerialName("Sqrt")
data class Sqrt(
    /** Operand passed to `sqrt`. */
    val a: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = sqrt(a.eval(x, y, v))
}

/** Wire spec for `a ^ b`. */
@Serializable
@SerialName("Pow")
data class Pow(
    /** Base. */
    val a: ScalarExpr,
    /** Exponent. */
    val b: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = a.eval(x, y, v).pow(b.eval(x, y, v))
}

/** Wire spec for `min(l, r)`. */
@Serializable
@SerialName("MinExpr")
data class MinExpr(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double =
        kotlin.math.min(l.eval(x, y, v), r.eval(x, y, v))
}

/** Wire spec for `max(l, r)`. */
@Serializable
@SerialName("MaxExpr")
data class MaxExpr(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double =
        kotlin.math.max(l.eval(x, y, v), r.eval(x, y, v))
}

/** Wire spec for a ternary `if (cond) then else otherwise`. */
@Serializable
@SerialName("IfExpr")
data class IfExpr(
    /** Boolean predicate selecting the branch. */
    val cond: BoolExpr,
    /** Branch returned when [cond] is true. */
    val then: ScalarExpr,
    /** Branch returned when [cond] is false. */
    val otherwise: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double =
        if (cond.eval(x, y, v)) then.eval(x, y, v) else otherwise.eval(x, y, v)
}

/**
 * Reduction over the entire vector input. Distinct from element-level
 * arithmetic ([Add], [Mul]) - this collapses a `DoubleArray` of arbitrary
 * length to a single scalar via the chosen operation.
 */
@Serializable
enum class VFoldOp {
    /** `Sum v[i]`. */
    Sum,

    /** `Product v[i]`. */
    Product,

    /** Arithmetic mean of the vector entries. Empty input throws. */
    Mean,

    /** Minimum entry. Empty input throws. */
    Min,

    /** Maximum entry. Empty input throws. */
    Max,

    /** Euclidean (L2) norm `sqrt(Sum v[i]^2)`. */
    Norm2,
}

/** Wire spec for a whole-vector reduction selected by [op]. */
@Serializable
@SerialName("VFold")
data class VFold(
    /** Reduction operation applied to the vector. */
    val op: VFoldOp,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = when (op) {
        VFoldOp.Sum -> {
            var s = 0.0
            for (e in v) s += e
            s
        }
        VFoldOp.Product -> {
            var p = 1.0
            for (e in v) p *= e
            p
        }
        VFoldOp.Mean -> {
            require(v.isNotEmpty()) { "VFold.Mean on empty vector" }
            var s = 0.0
            for (e in v) s += e
            s / v.size
        }
        VFoldOp.Min -> {
            require(v.isNotEmpty()) { "VFold.Min on empty vector" }
            var m = v[0]
            for (i in 1 until v.size) if (v[i] < m) m = v[i]
            m
        }
        VFoldOp.Max -> {
            require(v.isNotEmpty()) { "VFold.Max on empty vector" }
            var m = v[0]
            for (i in 1 until v.size) if (v[i] > m) m = v[i]
            m
        }
        VFoldOp.Norm2 -> {
            var s = 0.0
            for (e in v) s += e * e
            sqrt(s)
        }
    }
}

/**
 * Weighted dot product `Sum weights[i] * v[i]`. Length must match the incoming
 * vector at eval time. Wire form is a primitive list - encodes cleanly.
 */
@Serializable
@SerialName("VDot")
data class VDot(
    /** Coefficient vector applied element-wise; must match input length at eval. */
    val weights: List<Double>,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double {
        require(v.size == weights.size) {
            "VDot length mismatch: weights=${weights.size}, v=${v.size}"
        }
        var s = 0.0
        for (i in 0 until v.size) s += weights[i] * v[i]
        return s
    }
}

/**
 * Wire-serializable AST for boolean expressions over the same input env as
 * [ScalarExpr]. Used by filter-config wrappers and as the condition of [IfExpr].
 */
@Serializable
sealed interface BoolExpr {
    /** Evaluate this predicate against the per-update inputs. */
    fun eval(x: Double, y: Double = 0.0, v: DoubleArray = EMPTY_VECTOR): Boolean
}

/** Wire spec for `l > r`. */
@Serializable
@SerialName("Gt")
data class Gt(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = l.eval(x, y, v) > r.eval(x, y, v)
}

/** Wire spec for `l >= r`. */
@Serializable
@SerialName("Ge")
data class Ge(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = l.eval(x, y, v) >= r.eval(x, y, v)
}

/** Wire spec for `l < r`. */
@Serializable
@SerialName("Lt")
data class Lt(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = l.eval(x, y, v) < r.eval(x, y, v)
}

/** Wire spec for `l <= r`. */
@Serializable
@SerialName("Le")
data class Le(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = l.eval(x, y, v) <= r.eval(x, y, v)
}

/** Wire spec for `l == r`. Exact floating-point equality; usually you want a tolerance instead. */
@Serializable
@SerialName("Eq")
data class Eq(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = l.eval(x, y, v) == r.eval(x, y, v)
}

/** Wire spec for `l && r` (short-circuiting). */
@Serializable
@SerialName("And")
data class And(
    /** Left operand. */
    val l: BoolExpr,
    /** Right operand; evaluated only when [l] is true. */
    val r: BoolExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = l.eval(x, y, v) && r.eval(x, y, v)
}

/** Wire spec for `l || r` (short-circuiting). */
@Serializable
@SerialName("Or")
data class Or(
    /** Left operand. */
    val l: BoolExpr,
    /** Right operand; evaluated only when [l] is false. */
    val r: BoolExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = l.eval(x, y, v) || r.eval(x, y, v)
}

/** Wire spec for `!a`. */
@Serializable
@SerialName("Not")
data class Not(
    /** Operand to negate. */
    val a: BoolExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = !a.eval(x, y, v)
}

/** `min <= a <= max` (inclusive). Wire-compact form of `And(Ge(a, min), Le(a, max))`. */
@Serializable
@SerialName("InRange")
data class InRange(
    /** Value tested against the range. */
    val a: ScalarExpr,
    /** Inclusive lower bound. */
    val min: Double,
    /** Inclusive upper bound. */
    val max: Double,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean {
        val value = a.eval(x, y, v)
        return value in min..max
    }
}

/**
 * Wire-serializable AST for vector-valued expressions over the same input env
 * as [ScalarExpr] / [BoolExpr]. The output length need not match the input -
 * use this for permutations, dimensionality changes, pooling, feature
 * augmentation, etc. For same-length per-element transforms, the simpler
 * `transformElement(ScalarExpr)` config is more direct.
 */
@Serializable
sealed interface VectorExpr {
    /** Evaluate this expression to produce a fresh vector from the per-update inputs. */
    fun eval(x: Double = 0.0, y: Double = 0.0, v: DoubleArray = EMPTY_VECTOR): DoubleArray
}

/**
 * Build an output vector by evaluating each [ScalarExpr] in order. Output
 * length = `exprs.size`. The exprs can reference any input element via
 * [V]`(i)` - sufficient for permutations (`VElements(listOf(V(2), V(0), V(1)))`),
 * pooling (`VElements(listOf((V(0)+V(1))/2.0, (V(2)+V(3))/2.0))`),
 * dimensionality reduction, and feature augmentation.
 */
@Serializable
@SerialName("VElements")
data class VElements(
    /** Per-output-coordinate expression; output length = `exprs.size`. */
    val exprs: List<ScalarExpr>,
) : VectorExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): DoubleArray =
        DoubleArray(exprs.size) { i -> exprs[i].eval(x, y, v) }
}

// ========== DSL: arithmetic operators ==========

/** Build [Add] of two expressions. */
operator fun ScalarExpr.plus(rhs: ScalarExpr): ScalarExpr = Add(this, rhs)

/** Add a literal [rhs] to this expression. */
operator fun ScalarExpr.plus(rhs: Double): ScalarExpr = Add(this, Const(rhs))

/** Add an expression to a literal receiver. */
operator fun Double.plus(rhs: ScalarExpr): ScalarExpr = Add(Const(this), rhs)

/** Build [Sub] of two expressions. */
operator fun ScalarExpr.minus(rhs: ScalarExpr): ScalarExpr = Sub(this, rhs)

/** Subtract a literal [rhs] from this expression. */
operator fun ScalarExpr.minus(rhs: Double): ScalarExpr = Sub(this, Const(rhs))

/** Subtract an expression from a literal receiver. */
operator fun Double.minus(rhs: ScalarExpr): ScalarExpr = Sub(Const(this), rhs)

/** Build [Mul] of two expressions. */
operator fun ScalarExpr.times(rhs: ScalarExpr): ScalarExpr = Mul(this, rhs)

/** Multiply this expression by a literal [rhs]. */
operator fun ScalarExpr.times(rhs: Double): ScalarExpr = Mul(this, Const(rhs))

/** Multiply a literal receiver by an expression. */
operator fun Double.times(rhs: ScalarExpr): ScalarExpr = Mul(Const(this), rhs)

/** Build [Div] of two expressions. */
operator fun ScalarExpr.div(rhs: ScalarExpr): ScalarExpr = Div(this, rhs)

/** Divide this expression by a literal [rhs]. */
operator fun ScalarExpr.div(rhs: Double): ScalarExpr = Div(this, Const(rhs))

/** Divide a literal receiver by an expression. */
operator fun Double.div(rhs: ScalarExpr): ScalarExpr = Div(Const(this), rhs)

/** Unary minus: wraps in [Neg]. */
operator fun ScalarExpr.unaryMinus(): ScalarExpr = Neg(this)

// ========== DSL: comparison and boolean ==========

/** Strictly-greater-than comparison. */
infix fun ScalarExpr.gt(rhs: ScalarExpr): BoolExpr = Gt(this, rhs)

/** Strictly-greater-than against a literal. */
infix fun ScalarExpr.gt(rhs: Double): BoolExpr = Gt(this, Const(rhs))

/** Greater-or-equal comparison. */
infix fun ScalarExpr.ge(rhs: ScalarExpr): BoolExpr = Ge(this, rhs)

/** Greater-or-equal against a literal. */
infix fun ScalarExpr.ge(rhs: Double): BoolExpr = Ge(this, Const(rhs))

/** Strictly-less-than comparison. */
infix fun ScalarExpr.lt(rhs: ScalarExpr): BoolExpr = Lt(this, rhs)

/** Strictly-less-than against a literal. */
infix fun ScalarExpr.lt(rhs: Double): BoolExpr = Lt(this, Const(rhs))

/** Less-or-equal comparison. */
infix fun ScalarExpr.le(rhs: ScalarExpr): BoolExpr = Le(this, rhs)

/** Less-or-equal against a literal. */
infix fun ScalarExpr.le(rhs: Double): BoolExpr = Le(this, Const(rhs))

/** Exact equality (no tolerance). */
infix fun ScalarExpr.eq(rhs: ScalarExpr): BoolExpr = Eq(this, rhs)

/** Exact equality against a literal (no tolerance). */
infix fun ScalarExpr.eq(rhs: Double): BoolExpr = Eq(this, Const(rhs))

/** Short-circuiting conjunction. */
infix fun BoolExpr.and(rhs: BoolExpr): BoolExpr = And(this, rhs)

/** Short-circuiting disjunction. */
infix fun BoolExpr.or(rhs: BoolExpr): BoolExpr = Or(this, rhs)

/** Logical negation. */
operator fun BoolExpr.not(): BoolExpr = Not(this)
