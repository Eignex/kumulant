package com.eignex.kumulant.schema

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * Wire-serializable AST for single-variable scalar expressions over the
 * incoming `Double` value. Used by the transform-config wrappers in
 * [OperationConfigs.kt] to encode value-mapping behavior that would otherwise
 * require a Kotlin lambda. Nodes are pure data; evaluation is straightforward
 * recursive interpretation.
 *
 * Polymorphic via skema's `$type` discriminator. Authoring form is direct AST
 * construction: `Add(Mul(Const(2.0), X), Const(1.0))` for `2*x + 1`.
 */
@Serializable
sealed interface ScalarExpr {
    fun eval(x: Double): Double
}

@Serializable @SerialName("X")
data object X : ScalarExpr {
    override fun eval(x: Double): Double = x
}

@Serializable @SerialName("Const")
data class Const(val v: Double) : ScalarExpr {
    override fun eval(x: Double): Double = v
}

@Serializable @SerialName("Add")
data class Add(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double): Double = l.eval(x) + r.eval(x)
}

@Serializable @SerialName("Sub")
data class Sub(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double): Double = l.eval(x) - r.eval(x)
}

@Serializable @SerialName("Mul")
data class Mul(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double): Double = l.eval(x) * r.eval(x)
}

@Serializable @SerialName("Div")
data class Div(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double): Double = l.eval(x) / r.eval(x)
}

@Serializable @SerialName("Neg")
data class Neg(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double): Double = -a.eval(x)
}

@Serializable @SerialName("Abs")
data class Abs(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double): Double = abs(a.eval(x))
}

@Serializable @SerialName("Log")
data class Log(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double): Double = ln(a.eval(x))
}

@Serializable @SerialName("Exp")
data class Exp(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double): Double = exp(a.eval(x))
}

@Serializable @SerialName("Sqrt")
data class Sqrt(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double): Double = sqrt(a.eval(x))
}

@Serializable @SerialName("Pow")
data class Pow(val a: ScalarExpr, val b: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double): Double = a.eval(x).pow(b.eval(x))
}

@Serializable @SerialName("MinExpr")
data class MinExpr(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double): Double = kotlin.math.min(l.eval(x), r.eval(x))
}

@Serializable @SerialName("MaxExpr")
data class MaxExpr(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double): Double = kotlin.math.max(l.eval(x), r.eval(x))
}

@Serializable @SerialName("IfExpr")
data class IfExpr(val cond: BoolExpr, val then: ScalarExpr, val otherwise: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double): Double = if (cond.eval(x)) then.eval(x) else otherwise.eval(x)
}

/**
 * Wire-serializable AST for single-variable boolean expressions over the
 * incoming `Double` value. Used by filter-config wrappers and as the
 * condition of [IfExpr].
 */
@Serializable
sealed interface BoolExpr {
    fun eval(x: Double): Boolean
}

@Serializable @SerialName("Gt")
data class Gt(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double): Boolean = l.eval(x) > r.eval(x)
}

@Serializable @SerialName("Ge")
data class Ge(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double): Boolean = l.eval(x) >= r.eval(x)
}

@Serializable @SerialName("Lt")
data class Lt(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double): Boolean = l.eval(x) < r.eval(x)
}

@Serializable @SerialName("Le")
data class Le(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double): Boolean = l.eval(x) <= r.eval(x)
}

@Serializable @SerialName("Eq")
data class Eq(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double): Boolean = l.eval(x) == r.eval(x)
}

@Serializable @SerialName("And")
data class And(val l: BoolExpr, val r: BoolExpr) : BoolExpr {
    override fun eval(x: Double): Boolean = l.eval(x) && r.eval(x)
}

@Serializable @SerialName("Or")
data class Or(val l: BoolExpr, val r: BoolExpr) : BoolExpr {
    override fun eval(x: Double): Boolean = l.eval(x) || r.eval(x)
}

@Serializable @SerialName("Not")
data class Not(val a: BoolExpr) : BoolExpr {
    override fun eval(x: Double): Boolean = !a.eval(x)
}

/**
 * `min <= a <= max` (inclusive). Wire-compact form of `And(Ge(a, min), Le(a, max))`.
 */
@Serializable @SerialName("InRange")
data class InRange(val a: ScalarExpr, val min: Double, val max: Double) : BoolExpr {
    override fun eval(x: Double): Boolean {
        val v = a.eval(x)
        return v in min..max
    }
}
