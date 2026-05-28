package com.eignex.kumulant.schema

import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.HasMinMax
import com.eignex.kumulant.core.IndexedResult
import com.eignex.kumulant.core.Result
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.math.sqrt

private val EMPTY_VECTOR = DoubleArray(0)

/**
 * Wire-serialisable AST for scalar expressions over the per-update input
 * environment. The library uses these wherever a stat needs to apply a
 * caller-supplied projection / weight / threshold expression that has to
 * round-trip on the wire; `weightBy`, `transform`, the per-bin scaler
 * projections, the `WithFeedback` op, the loss / pinball / quantile
 * configurations.
 *
 * ## The input environment
 *
 * Every evaluation receives:
 *
 * - `x: Double`: the primary scalar input. For series stats it's the
 *   observation value; for paired stats it's the x-axis; for regression
 *   stats it's unused (use [V] to access feature vector coordinates).
 * - `y: Double`: the secondary scalar input. Used by paired stats (the
 *   y-axis) and regression stats (the response).
 * - `v: DoubleArray`: the full input vector. Used by vector / regression
 *   stats; empty otherwise.
 * - `primary: Result?`: the primary stat's snapshot at evaluation time
 *   for feedback operators. [Center], [Scale], [Low], [High] read directly
 *   from this; per-coordinate ops receive an [com.eignex.kumulant.core.IndexedResult]
 *   to thread the coordinate index through.
 *
 * Stats that don't need a particular field pass the default (0.0, empty
 * array, null).
 *
 * ## Construction
 *
 * Compose with the DSL operators in this file rather than constructing AST
 * data classes directly: `X * 2.0`, `(X + Const(1.0)) gt 0.0`,
 * `IfExpr(X gt 0.0, X, -X)`. The operators return the public sealed
 * interface and hide the concrete (internal) node types.
 *
 * ## Wire format
 *
 * Polymorphic via skema's `$type` discriminator. The `@SerialName` on each
 * concrete node is the wire-format tag; pick stable, unambiguous names if
 * you add new ones.
 */
@Serializable
sealed interface ScalarExpr {
    /**
     * Evaluate this expression against the per-update inputs. See the class
     * KDoc for the input-environment convention.
     */
    fun eval(x: Double, y: Double = 0.0, v: DoubleArray = EMPTY_VECTOR, primary: Result? = null): Double
}

/** Refers to the primary scalar input `x`. */
@Serializable
@SerialName("X")
data object X : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = x
}

/** Refers to the secondary scalar input `y` (paired stats only). */
@Serializable
@SerialName("Y")
data object Y : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = y
}

/**
 * Reads the `center` field of the feedback primary's snapshot. Requires the primary's
 * result to implement [HasCenterScale]; raises [IllegalStateException] when evaluated
 * without a primary, or when the primary's result does not expose a center.
 */
@Serializable
@SerialName("Center")
data object Center : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double {
        val unwrapped = if (primary is IndexedResult) primary.inner else primary
        check(unwrapped is HasCenterScale) {
            "Center requires a HasCenterScale feedback primary; got ${unwrapped?.let { it::class.simpleName }}"
        }
        return unwrapped.center
    }
}

/**
 * Reads the `scale` field of the feedback primary's snapshot. Requires the primary's
 * result to implement [HasCenterScale]; raises [IllegalStateException] when evaluated
 * without a primary, or when the primary's result does not expose a scale.
 */
@Serializable
@SerialName("Scale")
data object Scale : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double {
        val unwrapped = if (primary is IndexedResult) primary.inner else primary
        check(unwrapped is HasCenterScale) {
            "Scale requires a HasCenterScale feedback primary; got ${unwrapped?.let { it::class.simpleName }}"
        }
        return unwrapped.scale
    }
}

/**
 * Reads the `min` field of the feedback primary's snapshot. Requires the primary's
 * result to implement [HasMinMax]; raises [IllegalStateException] when evaluated
 * without such a primary.
 */
@Serializable
@SerialName("Low")
data object Low : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double {
        val unwrapped = if (primary is IndexedResult) primary.inner else primary
        check(unwrapped is HasMinMax) {
            "Low requires a HasMinMax feedback primary; got ${unwrapped?.let { it::class.simpleName }}"
        }
        return unwrapped.min
    }
}

/**
 * Reads the `max` field of the feedback primary's snapshot. Requires the primary's
 * result to implement [HasMinMax]; raises [IllegalStateException] when evaluated
 * without such a primary.
 */
@Serializable
@SerialName("High")
data object High : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double {
        val unwrapped = if (primary is IndexedResult) primary.inner else primary
        check(unwrapped is HasMinMax) {
            "High requires a HasMinMax feedback primary; got ${unwrapped?.let { it::class.simpleName }}"
        }
        return unwrapped.max
    }
}

/**
 * In element-wise feedback contexts (vector / regression / paired), returns the
 * coordinate index of the currently evaluating element as a Double. Outside such
 * contexts (primary is not an [IndexedResult]), raises [IllegalStateException].
 *
 * Branch on this with [IfExpr]/[Eq] to apply different sub-expressions per coordinate:
 * `IfExpr(VIndex eq 0.0, (X - Center) / Scale, X)`.
 */
@Serializable
@SerialName("VIndex")
data object VIndex : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double {
        check(primary is IndexedResult) {
            "VIndex requires an element-wise feedback context; got ${primary?.let { it::class.simpleName }}"
        }
        return primary.index.toDouble()
    }
}

/** `v[index]` - out-of-bounds throws at eval time. */
@Serializable
@SerialName("V")
data class V(
    /** Vector coordinate to read. */
    val index: Int,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = v[index]
}

/** Wire spec for a constant scalar. */
@Serializable
@SerialName("Const")
data class Const(
    /** Literal value returned by [eval]. */
    val v: Double,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = this.v
}

/** Wire spec for `l + r`. */
@Serializable
@SerialName("Add")
internal data class Add(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = l.eval(
        x,
        y,
        v,
        primary,
    ) + r.eval(x, y, v, primary)
}

/** Wire spec for `l - r`. */
@Serializable
@SerialName("Sub")
internal data class Sub(
    /** Left operand (minuend). */
    val l: ScalarExpr,
    /** Right operand (subtrahend). */
    val r: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = l.eval(
        x,
        y,
        v,
        primary,
    ) - r.eval(x, y, v, primary)
}

/** Wire spec for `l * r`. */
@Serializable
@SerialName("Mul")
internal data class Mul(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = l.eval(
        x,
        y,
        v,
        primary,
    ) * r.eval(x, y, v, primary)
}

/** Wire spec for `l / r`. */
@Serializable
@SerialName("Div")
internal data class Div(
    /** Dividend. */
    val l: ScalarExpr,
    /** Divisor. */
    val r: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = l.eval(
        x,
        y,
        v,
        primary,
    ) / r.eval(x, y, v, primary)
}

/** Wire spec for `-a`. */
@Serializable
@SerialName("Neg")
internal data class Neg(
    /** Operand to negate. */
    val a: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = -a.eval(x, y, v, primary)
}

/** Wire spec for `|a|`. */
@Serializable
@SerialName("Abs")
internal data class Abs(
    /** Operand whose absolute value is returned. */
    val a: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = abs(a.eval(x, y, v, primary))
}

/** Wire spec for the natural logarithm `ln(a)`. */
@Serializable
@SerialName("Log")
internal data class Log(
    /** Operand passed to `ln`. */
    val a: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = ln(a.eval(x, y, v, primary))
}

/** Wire spec for `exp(a)`. */
@Serializable
@SerialName("Exp")
internal data class Exp(
    /** Operand passed to `exp`. */
    val a: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = exp(a.eval(x, y, v, primary))
}

/** Wire spec for `sqrt(a)`. */
@Serializable
@SerialName("Sqrt")
internal data class Sqrt(
    /** Operand passed to `sqrt`. */
    val a: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = sqrt(a.eval(x, y, v, primary))
}

/** Wire spec for `a ^ b`. */
@Serializable
@SerialName("Pow")
internal data class Pow(
    /** Base. */
    val a: ScalarExpr,
    /** Exponent. */
    val b: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = a.eval(
        x,
        y,
        v,
        primary,
    ).pow(b.eval(x, y, v, primary))
}

/** Wire spec for `min(l, r)`. */
@Serializable
@SerialName("MinExpr")
internal data class MinExpr(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = min(
        l.eval(x, y, v, primary),
        r.eval(x, y, v, primary),
    )
}

/** Wire spec for `max(l, r)`. */
@Serializable
@SerialName("MaxExpr")
internal data class MaxExpr(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = max(
        l.eval(x, y, v, primary),
        r.eval(x, y, v, primary),
    )
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
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        if (cond.eval(x, y, v, primary)) then.eval(x, y, v, primary) else otherwise.eval(x, y, v, primary)
}

/**
 * One case of a [Switch] expression: when `on` evaluates to [value] (exact equality),
 * the result is [then]. Exact double equality is fine for integer-valued keys like
 * [VIndex]; not recommended for general continuous comparisons.
 */
@Serializable
@SerialName("SwitchCase")
data class SwitchCase(
    /** Value of the switch key that selects this branch. */
    val value: Double,
    /** Expression evaluated when this case is selected. */
    val then: ScalarExpr,
)

/**
 * Multi-way branch on a scalar key. Replaces nested [IfExpr] cascades. The first case
 * whose [SwitchCase.value] equals `on.eval(...)` exactly wins; if none match, [otherwise]
 * is returned.
 */
@Serializable
@SerialName("Switch")
data class Switch(
    /** Scalar expression whose value selects the case. Typically [VIndex]. */
    val on: ScalarExpr,
    /** Cases checked in order. */
    val cases: List<SwitchCase>,
    /** Returned when no case matches. */
    val otherwise: ScalarExpr,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double {
        val key = on.eval(x, y, v, primary)
        for (c in cases) if (c.value == key) return c.then.eval(x, y, v, primary)
        return otherwise.eval(x, y, v, primary)
    }
}

/**
 * Membership test: `of in values` (exact equality). Use it to flatten chains of
 * `Eq` predicates joined by `Or`; e.g. `VIndex In listOf(0.0, 3.0)`.
 */
@Serializable
@SerialName("In")
data class In(
    /** Scalar expression being tested. */
    val of: ScalarExpr,
    /** Allowed values; exact equality. */
    val values: List<Double>,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        of.eval(x, y, v, primary) in values
}

/**
 * Z-score projection: `(X - Center) / Scale`, emitting `0` when [Scale] is still zero.
 * Reusable AST sugar for the standard-scaler pattern; requires a [HasCenterScale] primary.
 */
@Serializable
@SerialName("Standardize")
data object Standardize : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double {
        val unwrapped = if (primary is IndexedResult) primary.inner else primary
        check(unwrapped is HasCenterScale) {
            "Standardize requires a HasCenterScale primary; got ${unwrapped?.let { it::class.simpleName }}"
        }
        val scale = unwrapped.scale
        return if (scale > 0.0) (x - unwrapped.center) / scale else 0.0
    }
}

/**
 * Min-max projection mapping `X` from `[primary.min, primary.max]` to `[targetLow, targetHigh]`,
 * emitting [targetLow] while the running range is still degenerate. Reusable AST sugar for the
 * min-max-scaler pattern; requires a [HasMinMax] primary.
 */
@Serializable
@SerialName("MinMax")
data class MinMax(
    /** Lower bound of the output range. */
    val targetLow: Double = 0.0,
    /** Upper bound of the output range. */
    val targetHigh: Double = 1.0,
) : ScalarExpr {
    init {
        require(targetHigh > targetLow) { "MinMax targetHigh ($targetHigh) must be > targetLow ($targetLow)" }
    }

    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double {
        val unwrapped = if (primary is IndexedResult) primary.inner else primary
        check(unwrapped is HasMinMax) {
            "MinMax requires a HasMinMax primary; got ${unwrapped?.let { it::class.simpleName }}"
        }
        val lo = unwrapped.min
        val hi = unwrapped.max
        val span = hi - lo
        return if (span > 0.0) targetLow + (x - lo) / span * (targetHigh - targetLow) else targetLow
    }
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
internal data class VFold(
    /** Reduction operation applied to the vector. */
    val op: VFoldOp,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = when (op) {
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
internal data class VDot(
    /** Coefficient vector applied element-wise; must match input length at eval. */
    val weights: List<Double>,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double {
        require(v.size == weights.size) {
            "VDot length mismatch: weights=${weights.size}, v=${v.size}"
        }
        var s = 0.0
        for (i in 0 until v.size) s += weights[i] * v[i]
        return s
    }
}

/**
 * Wire-serialisable AST for boolean predicates over the same input
 * environment as [ScalarExpr]. Consumed by every spec-side `filter` operator
 * and by [IfExpr]'s condition slot.
 *
 * Build via the comparison operators (`gt` / `ge` / `lt` / `le` / `eq`) on
 * [ScalarExpr] and the boolean combinators (`and` / `or` / `not`). The
 * resulting expression round-trips on the wire alongside the rest of the
 * spec.
 *
 * Examples:
 *
 * ```kotlin
 * X gt 0.0                          // positive values only
 * (X gt 0.0) and (X lt 100.0)       // half-open interval
 * In(VIndex, listOf(0.0, 2.0, 5.0)) // coordinate index in {0, 2, 5}
 * ```
 *
 * See [ScalarExpr] for the input-environment convention.
 */
@Serializable
sealed interface BoolExpr {
    /**
     * Evaluate this predicate against the per-update inputs. Returns `true`
     * to keep an observation under `filter` or to take the `then` branch
     * under `IfExpr`.
     */
    fun eval(x: Double, y: Double = 0.0, v: DoubleArray = EMPTY_VECTOR, primary: Result? = null): Boolean
}

/** Wire spec for `l > r`. */
@Serializable
@SerialName("Gt")
internal data class Gt(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean = l.eval(
        x,
        y,
        v,
        primary,
    ) > r.eval(x, y, v, primary)
}

/** Wire spec for `l >= r`. */
@Serializable
@SerialName("Ge")
internal data class Ge(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean = l.eval(
        x,
        y,
        v,
        primary,
    ) >= r.eval(x, y, v, primary)
}

/** Wire spec for `l < r`. */
@Serializable
@SerialName("Lt")
internal data class Lt(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean = l.eval(
        x,
        y,
        v,
        primary,
    ) < r.eval(x, y, v, primary)
}

/** Wire spec for `l <= r`. */
@Serializable
@SerialName("Le")
internal data class Le(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean = l.eval(
        x,
        y,
        v,
        primary,
    ) <= r.eval(x, y, v, primary)
}

/** Wire spec for `l == r`. Exact floating-point equality; usually you want a tolerance instead. */
@Serializable
@SerialName("Eq")
internal data class Eq(
    /** Left operand. */
    val l: ScalarExpr,
    /** Right operand. */
    val r: ScalarExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean = l.eval(
        x,
        y,
        v,
        primary,
    ) == r.eval(x, y, v, primary)
}

/** Wire spec for `l && r` (short-circuiting). */
@Serializable
@SerialName("And")
internal data class And(
    /** Left operand. */
    val l: BoolExpr,
    /** Right operand; evaluated only when [l] is true. */
    val r: BoolExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean = l.eval(
        x,
        y,
        v,
        primary,
    ) && r.eval(x, y, v, primary)
}

/** Wire spec for `l || r` (short-circuiting). */
@Serializable
@SerialName("Or")
internal data class Or(
    /** Left operand. */
    val l: BoolExpr,
    /** Right operand; evaluated only when [l] is false. */
    val r: BoolExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean = l.eval(
        x,
        y,
        v,
        primary,
    ) || r.eval(x, y, v, primary)
}

/** Wire spec for `!a`. */
@Serializable
@SerialName("Not")
internal data class Not(
    /** Operand to negate. */
    val a: BoolExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean = !a.eval(x, y, v, primary)
}

/** `min <= a <= max` (inclusive). Wire-compact form of `And(Ge(a, min), Le(a, max))`. */
@Serializable
@SerialName("InRange")
internal data class InRange(
    /** Value tested against the range. */
    val a: ScalarExpr,
    /** Inclusive lower bound. */
    val min: Double,
    /** Inclusive upper bound. */
    val max: Double,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean {
        val value = a.eval(x, y, v, primary)
        return value in min..max
    }
}

/**
 * Wire-serialisable AST for vector-valued expressions over the same input
 * environment as [ScalarExpr] / [BoolExpr]. Used by the spec-side
 * `transformVector(VectorExpr)` operator when the output is a fresh vector
 * rather than a per-element transform.
 *
 * Output length need not match input length; use [VectorExpr] for
 * permutations, dimensionality changes, pooling, feature augmentation. For
 * same-length per-element transforms the simpler
 * `transformElement(ScalarExpr)` is more direct and pays no extra
 * allocation.
 *
 * See [ScalarExpr] for the input-environment convention.
 */
@Serializable
sealed interface VectorExpr {
    /**
     * Evaluate this expression to produce a fresh `DoubleArray` from the
     * per-update inputs. Concrete implementations choose the output length;
     * downstream `transformVector(VectorExpr)` propagates it as the new
     * vector dimensionality.
     */
    fun eval(x: Double = 0.0, y: Double = 0.0, v: DoubleArray = EMPTY_VECTOR, primary: Result? = null): DoubleArray
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
internal data class VElements(
    /** Per-output-coordinate expression; output length = `exprs.size`. */
    val exprs: List<ScalarExpr>,
) : VectorExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): DoubleArray =
        DoubleArray(exprs.size) { i -> exprs[i].eval(x, y, v, primary) }
}

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
