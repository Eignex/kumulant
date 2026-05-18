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
    fun eval(x: Double, y: Double = 0.0, v: DoubleArray = EMPTY_VECTOR): Double
}

@Serializable
@SerialName("X")
data object X : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = x
}

@Serializable
@SerialName("Y")
data object Y : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = y
}

/** `v[index]` - out-of-bounds throws at eval time. */
@Serializable
@SerialName("V")
data class V(val index: Int) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = v[index]
}

@Serializable
@SerialName("Const")
data class Const(val v: Double) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = this.v
}

@Serializable
@SerialName("Add")
data class Add(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = l.eval(x, y, v) + r.eval(x, y, v)
}

@Serializable
@SerialName("Sub")
data class Sub(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = l.eval(x, y, v) - r.eval(x, y, v)
}

@Serializable
@SerialName("Mul")
data class Mul(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = l.eval(x, y, v) * r.eval(x, y, v)
}

@Serializable
@SerialName("Div")
data class Div(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = l.eval(x, y, v) / r.eval(x, y, v)
}

@Serializable
@SerialName("Neg")
data class Neg(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = -a.eval(x, y, v)
}

@Serializable
@SerialName("Abs")
data class Abs(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = abs(a.eval(x, y, v))
}

@Serializable
@SerialName("Log")
data class Log(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = ln(a.eval(x, y, v))
}

@Serializable
@SerialName("Exp")
data class Exp(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = exp(a.eval(x, y, v))
}

@Serializable
@SerialName("Sqrt")
data class Sqrt(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = sqrt(a.eval(x, y, v))
}

@Serializable
@SerialName("Pow")
data class Pow(val a: ScalarExpr, val b: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double = a.eval(x, y, v).pow(b.eval(x, y, v))
}

@Serializable
@SerialName("MinExpr")
data class MinExpr(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double =
        kotlin.math.min(l.eval(x, y, v), r.eval(x, y, v))
}

@Serializable
@SerialName("MaxExpr")
data class MaxExpr(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double =
        kotlin.math.max(l.eval(x, y, v), r.eval(x, y, v))
}

@Serializable
@SerialName("IfExpr")
data class IfExpr(val cond: BoolExpr, val then: ScalarExpr, val otherwise: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Double =
        if (cond.eval(x, y, v)) then.eval(x, y, v) else otherwise.eval(x, y, v)
}

/**
 * Reduction over the entire vector input. Distinct from element-level
 * arithmetic ([Add], [Mul]) - this collapses a `DoubleArray` of arbitrary
 * length to a single scalar via the chosen operation.
 */
@Serializable
enum class VFoldOp { Sum, Product, Mean, Min, Max, Norm2 }

@Serializable
@SerialName("VFold")
data class VFold(val op: VFoldOp) : ScalarExpr {
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
data class VDot(val weights: List<Double>) : ScalarExpr {
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
    fun eval(x: Double, y: Double = 0.0, v: DoubleArray = EMPTY_VECTOR): Boolean
}

@Serializable
@SerialName("Gt")
data class Gt(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = l.eval(x, y, v) > r.eval(x, y, v)
}

@Serializable
@SerialName("Ge")
data class Ge(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = l.eval(x, y, v) >= r.eval(x, y, v)
}

@Serializable
@SerialName("Lt")
data class Lt(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = l.eval(x, y, v) < r.eval(x, y, v)
}

@Serializable
@SerialName("Le")
data class Le(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = l.eval(x, y, v) <= r.eval(x, y, v)
}

@Serializable
@SerialName("Eq")
data class Eq(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = l.eval(x, y, v) == r.eval(x, y, v)
}

@Serializable
@SerialName("And")
data class And(val l: BoolExpr, val r: BoolExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = l.eval(x, y, v) && r.eval(x, y, v)
}

@Serializable
@SerialName("Or")
data class Or(val l: BoolExpr, val r: BoolExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = l.eval(x, y, v) || r.eval(x, y, v)
}

@Serializable
@SerialName("Not")
data class Not(val a: BoolExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): Boolean = !a.eval(x, y, v)
}

/** `min <= a <= max` (inclusive). Wire-compact form of `And(Ge(a, min), Le(a, max))`. */
@Serializable
@SerialName("InRange")
data class InRange(val a: ScalarExpr, val min: Double, val max: Double) : BoolExpr {
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
data class VElements(val exprs: List<ScalarExpr>) : VectorExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray): DoubleArray =
        DoubleArray(exprs.size) { i -> exprs[i].eval(x, y, v) }
}

// ========== DSL: arithmetic operators ==========

operator fun ScalarExpr.plus(rhs: ScalarExpr): ScalarExpr = Add(this, rhs)
operator fun ScalarExpr.plus(rhs: Double): ScalarExpr = Add(this, Const(rhs))
operator fun Double.plus(rhs: ScalarExpr): ScalarExpr = Add(Const(this), rhs)

operator fun ScalarExpr.minus(rhs: ScalarExpr): ScalarExpr = Sub(this, rhs)
operator fun ScalarExpr.minus(rhs: Double): ScalarExpr = Sub(this, Const(rhs))
operator fun Double.minus(rhs: ScalarExpr): ScalarExpr = Sub(Const(this), rhs)

operator fun ScalarExpr.times(rhs: ScalarExpr): ScalarExpr = Mul(this, rhs)
operator fun ScalarExpr.times(rhs: Double): ScalarExpr = Mul(this, Const(rhs))
operator fun Double.times(rhs: ScalarExpr): ScalarExpr = Mul(Const(this), rhs)

operator fun ScalarExpr.div(rhs: ScalarExpr): ScalarExpr = Div(this, rhs)
operator fun ScalarExpr.div(rhs: Double): ScalarExpr = Div(this, Const(rhs))
operator fun Double.div(rhs: ScalarExpr): ScalarExpr = Div(Const(this), rhs)

operator fun ScalarExpr.unaryMinus(): ScalarExpr = Neg(this)

// ========== DSL: comparison and boolean ==========

infix fun ScalarExpr.gt(rhs: ScalarExpr): BoolExpr = Gt(this, rhs)
infix fun ScalarExpr.gt(rhs: Double): BoolExpr = Gt(this, Const(rhs))
infix fun ScalarExpr.ge(rhs: ScalarExpr): BoolExpr = Ge(this, rhs)
infix fun ScalarExpr.ge(rhs: Double): BoolExpr = Ge(this, Const(rhs))
infix fun ScalarExpr.lt(rhs: ScalarExpr): BoolExpr = Lt(this, rhs)
infix fun ScalarExpr.lt(rhs: Double): BoolExpr = Lt(this, Const(rhs))
infix fun ScalarExpr.le(rhs: ScalarExpr): BoolExpr = Le(this, rhs)
infix fun ScalarExpr.le(rhs: Double): BoolExpr = Le(this, Const(rhs))
infix fun ScalarExpr.eq(rhs: ScalarExpr): BoolExpr = Eq(this, rhs)
infix fun ScalarExpr.eq(rhs: Double): BoolExpr = Eq(this, Const(rhs))

infix fun BoolExpr.and(rhs: BoolExpr): BoolExpr = And(this, rhs)
infix fun BoolExpr.or(rhs: BoolExpr): BoolExpr = Or(this, rhs)
operator fun BoolExpr.not(): BoolExpr = Not(this)
