package com.eignex.kumulant.schema.expr

import com.eignex.koblas.core.F64SparseVector
import com.eignex.koblas.core.F64VectorLike
import com.eignex.koblas.forEachStored
import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.HasMinMax
import com.eignex.kumulant.core.IndexedResult
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stream.DEFAULT_TARGET_HIGH
import com.eignex.kumulant.stream.DEFAULT_TARGET_LOW
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
 * Narrow a feedback primary to the trait a node needs, unwrapping the per-element indirection first.
 *
 * The [IndexedResult] unwrap is load-bearing: a vectorised feedback stat hands each coordinate its own
 * primary boxed with the coordinate's index, so a node that checked the box rather than its contents
 * would reject every element-wise pipeline.
 */
private inline fun <reified T> Result?.feedbackPrimary(node: String): T {
    val unwrapped = if (this is IndexedResult) inner else this
    check(unwrapped is T) {
        "$node requires a ${T::class.simpleName} feedback primary; got ${unwrapped?.let { it::class.simpleName }}"
    }
    return unwrapped
}

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
        val unwrapped = primary.feedbackPrimary<HasCenterScale>("Center")
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
        val unwrapped = primary.feedbackPrimary<HasCenterScale>("Scale")
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
        val unwrapped = primary.feedbackPrimary<HasMinMax>("Low")
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
        val unwrapped = primary.feedbackPrimary<HasMinMax>("High")
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
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean {
        // `in values` is List<Double>.contains, which boxes and uses Double.equals: total-order
        // semantics where NaN == NaN and 0.0 != -0.0. This node exists to flatten Eq chains, so it
        // has to compare primitives with IEEE == the way Eq and Switch do.
        val value = of.eval(x, y, v, primary)
        for (candidate in values) if (value == candidate) return true
        return false
    }
}

/**
 * Z-score projection: `(X - Center) / Scale`, emitting `0` when [Scale] is still zero.
 * Reusable AST sugar for the standard-scaler pattern; requires a [HasCenterScale] primary.
 */
@Serializable
@SerialName("Standardize")
data object Standardize : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double {
        val unwrapped = primary.feedbackPrimary<HasCenterScale>("Standardize")
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
    val targetLow: Double = DEFAULT_TARGET_LOW,
    /** Upper bound of the output range. */
    val targetHigh: Double = DEFAULT_TARGET_HIGH,
) : ScalarExpr {
    init {
        require(targetHigh > targetLow) { "MinMax targetHigh ($targetHigh) must be > targetLow ($targetLow)" }
    }

    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double {
        val unwrapped = primary.feedbackPrimary<HasMinMax>("MinMax")
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

/**
 * Wire spec for `a is NaN`.
 *
 * The only way to express this: `Gt`, `Ge`, `Lt`, `Le` and `Eq` all compare with IEEE semantics, so
 * every one of them evaluates `false` against a `NaN` and none can single it out. Its purpose is
 * `filter(!IsNaN(X))`, which is how a caller opts into dropping `NaN` observations -
 * [Stat][com.eignex.kumulant.core.Stat] leaves them to propagate by default, so a stream that wants
 * them gone says so here.
 */
@Serializable
@SerialName("IsNaN")
internal data class IsNaN(
    /** Value tested for NaN. */
    val of: ScalarExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        of.eval(x, y, v, primary).isNaN()
}

/**
 * Wire spec for `a is finite`, meaning neither `NaN` nor an infinity.
 *
 * Strictly stronger than `!IsNaN(a)`, and the difference matters: an infinity compares normally under
 * `Gt` and `Lt`, so a range check can be written that an infinity passes, and `1e400` parses to
 * `Infinity` without complaint. A caller that wants only real numbers wants this, not the `NaN` test.
 */
@Serializable
@SerialName("IsFinite")
internal data class IsFinite(
    /** Value tested for finiteness. */
    val of: ScalarExpr,
) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        of.eval(x, y, v, primary).isFinite()
}

/**
 * Wire spec for "every input to this update is finite", whatever the modality.
 *
 * One node covers all five because of how the filter wrappers bind `eval`: a series stat passes its
 * value as `x`, a paired stat passes `x` and `y`, a vector stat passes the coordinates in `v`, a
 * regression stat passes the target as `y` and the features in `v`, and a discrete stat passes a `Long`
 * widened to `x`, which cannot be non-finite. Unused slots default to `0.0`, which is finite, so
 * checking all of them is correct everywhere rather than merely harmless.
 *
 * This exists because a per-coordinate test cannot express it. `V(0).isFinite()` checks one coordinate,
 * and a feature vector needs all of them; there is no way to write a fold over `v` in the AST.
 *
 * [Stat][com.eignex.kumulant.core.Stat] guarantees only that a non-finite value will not throw. What a
 * stat then does with it is the stat's business, and for an accumulator that means propagating it for
 * good. This is the supported way to opt out of that.
 */
@Serializable
@SerialName("AllFinite")
internal data object AllFinite : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean {
        if (!x.isFinite() || !y.isFinite()) return false
        for (c in v) if (!c.isFinite()) return false
        return true
    }
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
 * Evaluate this scalar expression against a borrowed KoBLAS vector without materialising it.
 *
 * This overload is additive: [ScalarExpr.eval] with a [DoubleArray] remains the compatibility
 * surface for callers whose input is already dense.
 */
fun ScalarExpr.eval(x: Double = 0.0, y: Double = 0.0, v: F64VectorLike, primary: Result? = null): Double {
    if (v is F64SparseVector) {
        when (this) {
            is VFold -> when (op) {
                VFoldOp.Sum -> return sparseSum(v)
                VFoldOp.Mean -> return sparseMean(v)
                VFoldOp.Min -> return sparseMin(v)
                VFoldOp.Max -> return sparseMax(v)
                VFoldOp.Norm2 -> return sparseNorm2(v)
                VFoldOp.Product -> Unit
            }

            is VDot -> if (weights.all { it.isFinite() }) return sparseDot(v, weights)

            else -> Unit
        }
    }
    return evalVectorGeneric(x, y, v, primary)
}

private fun ScalarExpr.evalVectorGeneric(x: Double, y: Double, v: F64VectorLike, primary: Result?): Double = when (this) {
    X -> x

    Y -> y

    Center -> primary.feedbackPrimary<HasCenterScale>("Center").center

    Scale -> primary.feedbackPrimary<HasCenterScale>("Scale").scale

    Low -> primary.feedbackPrimary<HasMinMax>("Low").min

    High -> primary.feedbackPrimary<HasMinMax>("High").max

    VIndex -> {
        check(
            primary is IndexedResult,
        ) { "VIndex requires an element-wise feedback context; got ${primary?.let { it::class.simpleName }}" }
        primary.index.toDouble()
    }

    is V -> v[index]

    is Const -> this.v

    is Add -> l.eval(x, y, v, primary) + r.eval(x, y, v, primary)

    is Sub -> l.eval(x, y, v, primary) - r.eval(x, y, v, primary)

    is Mul -> l.eval(x, y, v, primary) * r.eval(x, y, v, primary)

    is Div -> l.eval(x, y, v, primary) / r.eval(x, y, v, primary)

    is Neg -> -a.eval(x, y, v, primary)

    is Abs -> abs(a.eval(x, y, v, primary))

    is Log -> ln(a.eval(x, y, v, primary))

    is Exp -> exp(a.eval(x, y, v, primary))

    is Sqrt -> sqrt(a.eval(x, y, v, primary))

    is Pow -> a.eval(x, y, v, primary).pow(b.eval(x, y, v, primary))

    is MinExpr -> min(l.eval(x, y, v, primary), r.eval(x, y, v, primary))

    is MaxExpr -> max(l.eval(x, y, v, primary), r.eval(x, y, v, primary))

    is IfExpr -> if (cond.eval(x, y, v, primary)) then.eval(x, y, v, primary) else otherwise.eval(x, y, v, primary)

    is Switch -> {
        val key = on.eval(x, y, v, primary)
        cases.firstOrNull { it.value == key }?.then?.eval(x, y, v, primary) ?: otherwise.eval(x, y, v, primary)
    }

    Standardize -> {
        val result = primary.feedbackPrimary<HasCenterScale>("Standardize")
        if (result.scale > 0.0) (x - result.center) / result.scale else 0.0
    }

    is MinMax -> {
        val result = primary.feedbackPrimary<HasMinMax>("MinMax")
        val span = result.max - result.min
        if (span > 0.0) targetLow + (x - result.min) / span * (targetHigh - targetLow) else targetLow
    }

    is VFold -> when (op) {
        VFoldOp.Sum -> (0 until v.size).sumOf { v[it] }

        VFoldOp.Product -> (0 until v.size).fold(1.0) { product, i -> product * v[i] }

        VFoldOp.Mean -> {
            require(
                v.size != 0,
            ) { "VFold.Mean on empty vector" }
            (0 until v.size).sumOf { v[it] } / v.size
        }

        VFoldOp.Min -> {
            require(
                v.size != 0,
            ) { "VFold.Min on empty vector" }
            var minimum = v[0]
            for (i in 1 until v.size) if (v[i] < minimum) minimum = v[i]
            minimum
        }

        VFoldOp.Max -> {
            require(
                v.size != 0,
            ) { "VFold.Max on empty vector" }
            var maximum = v[0]
            for (i in 1 until v.size) if (v[i] > maximum) maximum = v[i]
            maximum
        }

        VFoldOp.Norm2 -> sqrt((0 until v.size).sumOf { v[it] * v[it] })
    }

    is VDot -> {
        require(v.size == weights.size) { "VDot length mismatch: weights=${weights.size}, v=${v.size}" }
        (0 until v.size).sumOf { weights[it] * v[it] }
    }
}

private fun sparseSum(v: F64SparseVector): Double {
    var sum = 0.0
    v.forEachStored { _, value -> sum += value }
    return sum
}

private fun sparseMean(v: F64SparseVector): Double {
    require(v.size != 0) { "VFold.Mean on empty vector" }
    return sparseSum(v) / v.size
}

private fun sparseNorm2(v: F64SparseVector): Double {
    var sum = 0.0
    v.forEachStored { _, value -> sum += value * value }
    return sqrt(sum)
}

private fun sparseDot(v: F64SparseVector, weights: List<Double>): Double {
    require(v.size == weights.size) { "VDot length mismatch: weights=${weights.size}, v=${v.size}" }
    var sum = 0.0
    v.forEachStored { index, value -> sum += weights[index] * value }
    return sum
}

private fun sparseMin(v: F64SparseVector): Double {
    require(v.size != 0) { "VFold.Min on empty vector" }
    var minimum = 0.0
    var first = true
    var next = 0
    v.forEachStored { index, value ->
        if (first) {
            minimum = if (index == 0) value else 0.0
            first = false
        }
        if (index > next && 0.0 < minimum) minimum = 0.0
        if (index != 0 && value < minimum) minimum = value
        next = index + 1
    }
    if (first) return 0.0
    if (next < v.size && 0.0 < minimum) minimum = 0.0
    return minimum
}

private fun sparseMax(v: F64SparseVector): Double {
    require(v.size != 0) { "VFold.Max on empty vector" }
    var maximum = 0.0
    var first = true
    var next = 0
    v.forEachStored { index, value ->
        if (first) {
            maximum = if (index == 0) value else 0.0
            first = false
        }
        if (index > next && 0.0 > maximum) maximum = 0.0
        if (index != 0 && value > maximum) maximum = value
        next = index + 1
    }
    if (first) return 0.0
    if (next < v.size && 0.0 > maximum) maximum = 0.0
    return maximum
}

/** Evaluate this boolean expression against a borrowed KoBLAS vector without materialising it. */
fun BoolExpr.eval(x: Double = 0.0, y: Double = 0.0, v: F64VectorLike, primary: Result? = null): Boolean = when (this) {
    is Gt -> l.eval(x, y, v, primary) > r.eval(x, y, v, primary)

    is Ge -> l.eval(x, y, v, primary) >= r.eval(x, y, v, primary)

    is Lt -> l.eval(x, y, v, primary) < r.eval(x, y, v, primary)

    is Le -> l.eval(x, y, v, primary) <= r.eval(x, y, v, primary)

    is Eq -> l.eval(x, y, v, primary) == r.eval(x, y, v, primary)

    is And -> l.eval(x, y, v, primary) && r.eval(x, y, v, primary)

    is Or -> l.eval(x, y, v, primary) || r.eval(x, y, v, primary)

    is Not -> !a.eval(x, y, v, primary)

    is In -> {
        val value = of.eval(x, y, v, primary)
        var matches = false
        for (candidate in values) {
            if (value == candidate) {
                matches = true
                break
            }
        }
        matches
    }

    is IsNaN -> of.eval(x, y, v, primary).isNaN()

    is IsFinite -> of.eval(x, y, v, primary).isFinite()

    AllFinite -> {
        if (!x.isFinite() || !y.isFinite()) {
            false
        } else {
            var finite = true
            v.forEachStored { _, value ->
                if (!value.isFinite()) finite = false
            }
            finite
        }
    }

    is InRange -> a.eval(x, y, v, primary) in min..max
}

/** Evaluate this vector expression against a borrowed KoBLAS vector, allocating only its owned output. */
fun VectorExpr.eval(x: Double = 0.0, y: Double = 0.0, v: F64VectorLike, primary: Result? = null): DoubleArray =
    when (this) {
        is VElements -> DoubleArray(exprs.size) { i -> exprs[i].eval(x, y, v, primary) }
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

/**
 * True when this expression evaluates to `NaN`.
 *
 * A `NaN` observation propagates through every stat by default; see
 * [Stat][com.eignex.kumulant.core.Stat]. Pair this with
 * [not] to drop them instead, which is the supported way to ask for that:
 *
 * ```
 * Mean.filter(!X.isNaN())
 * ```
 */
fun ScalarExpr.isNaN(): BoolExpr = IsNaN(this)

/**
 * True when this expression evaluates to a finite number: not `NaN`, and not an infinity.
 *
 * Stronger than `!isNaN()`. Prefer it whenever the intent is "a real number", since an infinity passes
 * every ordering comparison the AST can express.
 *
 * ```
 * Mean.filter(X.isFinite())
 * ```
 */
fun ScalarExpr.isFinite(): BoolExpr = IsFinite(this)

/**
 * True when every input to the update is finite, whichever modality the stat is.
 *
 * Use it through the `filterFinite()` shorthand on each spec type rather than by hand. Unlike
 * [isFinite], this covers a whole feature vector, which no per-coordinate expression can.
 */
fun allFinite(): BoolExpr = AllFinite

/**
 * Absolute value of this expression.
 *
 * The unary and binary maths nodes follow the same rule as the arithmetic operators above: the node
 * itself stays `internal` so only its `@SerialName` is a contract, and this factory is the Kotlin way
 * to build one. Every node in this file must be reachable both ways - from Kotlin and from the wire -
 * and a pipeline that can only be authored as JSON is not usable from Kotlin at all.
 */
fun ScalarExpr.abs(): ScalarExpr = Abs(this)

/** Natural logarithm of this expression. `ln(0)` is `-Infinity` and a negative input gives `NaN`. */
fun ScalarExpr.ln(): ScalarExpr = Log(this)

/** `e` raised to this expression. Overflows to `+Infinity` well before `Double` runs out of range. */
fun ScalarExpr.exp(): ScalarExpr = Exp(this)

/** Square root of this expression. A negative input gives `NaN`. */
fun ScalarExpr.sqrt(): ScalarExpr = Sqrt(this)

/** This expression raised to [exponent]. */
fun ScalarExpr.pow(exponent: ScalarExpr): ScalarExpr = Pow(this, exponent)

/** This expression raised to a constant [exponent]. */
fun ScalarExpr.pow(exponent: Double): ScalarExpr = Pow(this, Const(exponent))

/** The smaller of this expression and [other]. */
fun ScalarExpr.min(other: ScalarExpr): ScalarExpr = MinExpr(this, other)

/** The smaller of this expression and a constant [other]. */
fun ScalarExpr.min(other: Double): ScalarExpr = MinExpr(this, Const(other))

/** The larger of this expression and [other]. */
fun ScalarExpr.max(other: ScalarExpr): ScalarExpr = MaxExpr(this, other)

/** The larger of this expression and a constant [other]. */
fun ScalarExpr.max(other: Double): ScalarExpr = MaxExpr(this, Const(other))

/**
 * True when this expression falls within `[min, max]`, both ends inclusive.
 *
 * Distinct from `x ge min and (x le max)`, which builds three nodes and evaluates this expression
 * twice; a filter runs on every update, so the single-node form is the one to reach for.
 */
fun ScalarExpr.inRange(min: Double, max: Double): BoolExpr = InRange(this, min, max)

/**
 * Reduce the whole feature vector to one scalar with [op].
 *
 * The vector-consuming half of the AST, and the only Kotlin-side consumer of [VFoldOp].
 */
fun vFold(op: VFoldOp): ScalarExpr = VFold(op)

/** Weighted sum of the feature vector. [weights] must match the vector length at evaluation time. */
fun vDot(weights: List<Double>): ScalarExpr = VDot(weights)

/** Weighted sum of the feature vector, varargs form. */
fun vDot(vararg weights: Double): ScalarExpr = VDot(weights.toList())

/**
 * Build an output vector by evaluating each expression in order; output length is `exprs.size`.
 *
 * The only [VectorExpr] constructor, and so the only way to reach `transformVector` / `transformX` from
 * Kotlin. Reference input coordinates with [V]: `vectorOf(V(2), V(0), V(1))` permutes,
 * `vectorOf((V(0) + V(1)) / 2.0)` pools, and a shorter list than the input reduces dimensionality.
 */
fun vectorOf(exprs: List<ScalarExpr>): VectorExpr = VElements(exprs)

/** Build an output vector from expressions in order, varargs form. */
fun vectorOf(vararg exprs: ScalarExpr): VectorExpr = VElements(exprs.toList())
