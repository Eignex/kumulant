package com.eignex.kumulant.schema.expr

import com.eignex.koblas.SparseVector
import com.eignex.koblas.Vector
import com.eignex.koblas.forEachStored
import com.eignex.koblas.koblas
import com.eignex.koblas.sum
import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.HasMinMax
import com.eignex.kumulant.core.IndexedResult
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.schema.serialization.BoolExprSerializer
import com.eignex.kumulant.schema.serialization.ScalarExprSerializer
import com.eignex.kumulant.schema.serialization.VectorExprSerializer
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

// Per-coordinate feedback boxes its primary in IndexedResult; trait checks apply to the inner result.
private inline fun <reified T> Result?.feedbackPrimary(node: String): T {
    val unwrapped = if (this is IndexedResult) inner else this
    check(unwrapped is T) {
        "$node requires a ${T::class.simpleName} feedback primary; got ${unwrapped?.let { it::class.simpleName }}"
    }
    return unwrapped
}

/**
 * Wire-serialisable scalar expression over per-update inputs:
 *
 * - `x`: series value or paired x-axis; unused for regression.
 * - `y`: paired y-axis or regression response.
 * - `v`: vector input or regression features.
 * - `primary`: feedback snapshot; [Center], [Scale], [Low], and [High] read its traits.
 *   Per-coordinate feedback uses [IndexedResult] to carry the coordinate index.
 *
 * Unused inputs default to `0.0`, an empty array, or `null`.
 * Compose with DSL operators such as `X * 2.0`, `X gt 0.0`, and [ifThenElse].
 *
 * JSON uses operator keys, numeric literals, and named references:
 * `{"add":[{"ref":"value"},1]}`. Resolve [Ref] nodes with [bind] and serialize through
 * [ScalarExpr], [BoolExpr], or [VectorExpr]. Other formats use sealed-class tags.
 */
@Serializable(with = ScalarExprSerializer::class)
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
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

/** Refers to the secondary scalar input `y` (paired stats only). */
@Serializable
@SerialName("Y")
data object Y : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

/**
 * Reads the `center` field of the feedback primary's snapshot. Requires the primary's
 * result to implement [HasCenterScale]; raises [IllegalStateException] when evaluated
 * without a primary, or when the primary's result does not expose a center.
 */
@Serializable
@SerialName("Center")
data object Center : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

/**
 * Reads the `scale` field of the feedback primary's snapshot. Requires the primary's
 * result to implement [HasCenterScale]; raises [IllegalStateException] when evaluated
 * without a primary, or when the primary's result does not expose a scale.
 */
@Serializable
@SerialName("Scale")
data object Scale : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

/**
 * Reads the `min` field of the feedback primary's snapshot. Requires the primary's
 * result to implement [HasMinMax]; raises [IllegalStateException] when evaluated
 * without such a primary.
 */
@Serializable
@SerialName("Low")
data object Low : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

/**
 * Reads the `max` field of the feedback primary's snapshot. Requires the primary's
 * result to implement [HasMinMax]; raises [IllegalStateException] when evaluated
 * without such a primary.
 */
@Serializable
@SerialName("High")
data object High : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

/**
 * In element-wise feedback contexts (vector / regression / paired), returns the
 * coordinate index of the currently evaluating element as a Double. Outside such
 * contexts (primary is not an [IndexedResult]), raises [IllegalStateException].
 *
 * Branch on this with [ifThenElse]/[Eq] to apply different sub-expressions per coordinate:
 * `ifThenElse(VIndex eq 0.0, (X - Center) / Scale, X)`.
 */
@Serializable
@SerialName("VIndex")
data object VIndex : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

/** `v[index]` - out-of-bounds throws at eval time. */
@Serializable
@SerialName("V")
data class V(
    /** Vector coordinate to read. */
    val index: Int,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

/** Wire spec for a constant scalar. */
@Serializable
@SerialName("Const")
data class Const(
    /** Literal value returned by [eval]. */
    val v: Double,
) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Add")
internal data class Add(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Sub")
internal data class Sub(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Mul")
internal data class Mul(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Div")
internal data class Div(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Neg")
internal data class Neg(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Abs")
internal data class Abs(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Log")
internal data class Log(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Exp")
internal data class Exp(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Sqrt")
internal data class Sqrt(val a: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Pow")
internal data class Pow(val a: ScalarExpr, val b: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("MinExpr")
internal data class MinExpr(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("MaxExpr")
internal data class MaxExpr(val l: ScalarExpr, val r: ScalarExpr) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
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
        evaluate(x, y, v, primary, ArrayInput)
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
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
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
        evaluate(x, y, v, primary, ArrayInput)
}

/**
 * Z-score projection: `(X - Center) / Scale`, emitting `0` when [Scale] is still zero.
 * Reusable AST sugar for the standard-scaler pattern; requires a [HasCenterScale] primary.
 */
@Serializable
@SerialName("Standardize")
data object Standardize : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
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

    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
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

@Serializable
@SerialName("VFold")
internal data class VFold(val op: VFoldOp) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("VDot")
internal data class VDot(val weights: List<Double>) : ScalarExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        evaluate(x, y, v, primary, ArrayInput)
}

/**
 * Wire-serialisable AST for boolean predicates over the same input
 * environment as [ScalarExpr]. Consumed by every spec-side `filter` operator
 * and by [IfExpr]'s condition slot.
 *
 * Build via the comparison operators (`gt` / `ge` / `lt` / `le` / `eq` / `ne`) on
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
@Serializable(with = BoolExprSerializer::class)
sealed interface BoolExpr {
    /**
     * Evaluate this predicate against the per-update inputs. Returns `true`
     * to keep an observation under `filter` or to take the `then` branch
     * under `IfExpr`.
     */
    fun eval(x: Double, y: Double = 0.0, v: DoubleArray = EMPTY_VECTOR, primary: Result? = null): Boolean
}

@Serializable
@SerialName("Gt")
internal data class Gt(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Ge")
internal data class Ge(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Lt")
internal data class Lt(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Le")
internal data class Le(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Eq")
internal data class Eq(val l: ScalarExpr, val r: ScalarExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("And")
internal data class And(val l: BoolExpr, val r: BoolExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Or")
internal data class Or(val l: BoolExpr, val r: BoolExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("Not")
internal data class Not(val a: BoolExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("IsNaN")
internal data class IsNaN(val of: ScalarExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("IsFinite")
internal data class IsFinite(val of: ScalarExpr) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        evaluate(x, y, v, primary, ArrayInput)
}

// Unused input slots default to finite values, allowing one finiteness check across all modalities.
@Serializable
@SerialName("AllFinite")
internal data object AllFinite : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        evaluate(x, y, v, primary, ArrayInput)
}

@Serializable
@SerialName("InRange")
internal data class InRange(val a: ScalarExpr, val min: Double, val max: Double) : BoolExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean =
        evaluate(x, y, v, primary, ArrayInput)
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
@Serializable(with = VectorExprSerializer::class)
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
 *
 * [VFoldOp.Sum] and [VFoldOp.Mean] reduce through koblas, which pairs terms across SIMD lanes for a
 * dense operand and accumulates in order for every other storage. Those associate differently, so
 * the same coordinates reduced from a dense vector and from a sparse one can disagree in the last
 * bits, and a snapshot carrying such a value is only equal to another that was fed the same storage.
 * Every other fold is order-independent and agrees exactly whatever the storage.
 */
fun ScalarExpr.eval(x: Double = 0.0, y: Double = 0.0, v: Vector, primary: Result? = null): Double =
    evaluate(x, y, v, primary, VectorInput)

/** Evaluate this boolean expression against a borrowed KoBLAS vector without materialising it. */
fun BoolExpr.eval(x: Double = 0.0, y: Double = 0.0, v: Vector, primary: Result? = null): Boolean =
    evaluate(x, y, v, primary, VectorInput)

/**
 * Evaluate this vector expression against a borrowed KoBLAS vector, allocating only its owned
 * output.
 */
fun VectorExpr.eval(x: Double = 0.0, y: Double = 0.0, v: Vector, primary: Result? = null): DoubleArray =
    evaluate(x, y, v, primary, VectorInput)

private fun <T> ScalarExpr.evaluate(x: Double, y: Double, v: T, primary: Result?, input: ExprInput<T>): Double =
    when (this) {
        is Ref -> error("Unresolved reference '$name'; supply explicit bindings")

        is BoundScalarExpr -> resolved.evaluate(x, y, v, primary, input)

        X -> x

        Y -> y

        Center -> primary.feedbackPrimary<HasCenterScale>("Center").center

        Scale -> primary.feedbackPrimary<HasCenterScale>("Scale").scale

        Low -> primary.feedbackPrimary<HasMinMax>("Low").min

        High -> primary.feedbackPrimary<HasMinMax>("High").max

        VIndex -> {
            check(primary is IndexedResult) {
                "VIndex requires an element-wise feedback context; got ${primary?.let { it::class.simpleName }}"
            }
            primary.index.toDouble()
        }

        is V -> input.get(v, index)

        is Const -> this.v

        is Add -> l.evaluate(x, y, v, primary, input) + r.evaluate(x, y, v, primary, input)

        is Sub -> l.evaluate(x, y, v, primary, input) - r.evaluate(x, y, v, primary, input)

        is Mul -> l.evaluate(x, y, v, primary, input) * r.evaluate(x, y, v, primary, input)

        is Div -> l.evaluate(x, y, v, primary, input) / r.evaluate(x, y, v, primary, input)

        is Neg -> -a.evaluate(x, y, v, primary, input)

        is Abs -> abs(a.evaluate(x, y, v, primary, input))

        is Log -> ln(a.evaluate(x, y, v, primary, input))

        is Exp -> exp(a.evaluate(x, y, v, primary, input))

        is Sqrt -> sqrt(a.evaluate(x, y, v, primary, input))

        is Pow -> a.evaluate(x, y, v, primary, input).pow(b.evaluate(x, y, v, primary, input))

        is MinExpr -> min(l.evaluate(x, y, v, primary, input), r.evaluate(x, y, v, primary, input))

        is MaxExpr -> max(l.evaluate(x, y, v, primary, input), r.evaluate(x, y, v, primary, input))

        is IfExpr ->
            if (
                cond.evaluate(
                    x,
                    y,
                    v,
                    primary,
                    input,
                )
            ) {
                then.evaluate(x, y, v, primary, input)
            } else {
                otherwise.evaluate(x, y, v, primary, input)
            }

        is Switch -> {
            val key = on.evaluate(x, y, v, primary, input)
            cases
                .firstOrNull { it.value == key }
                ?.then
                ?.evaluate(
                    x,
                    y,
                    v,
                    primary,
                    input,
                ) ?: otherwise.evaluate(x, y, v, primary, input)
        }

        Standardize -> {
            val result = primary.feedbackPrimary<HasCenterScale>("Standardize")
            if (result.scale > 0.0) (x - result.center) / result.scale else 0.0
        }

        is MinMax -> {
            val result = primary.feedbackPrimary<HasMinMax>("MinMax")
            val span = result.max - result.min
            if (span > 0.0) {
                targetLow + (x - result.min) / span * (targetHigh - targetLow)
            } else {
                targetLow
            }
        }

        is VFold -> input.fold(v, op)

        is VDot -> input.dot(v, weights)
    }

private fun <T> BoolExpr.evaluate(x: Double, y: Double, v: T, primary: Result?, input: ExprInput<T>): Boolean =
    when (this) {
        is BoundBoolExpr -> resolved.evaluate(x, y, v, primary, input)

        is Gt -> l.evaluate(x, y, v, primary, input) > r.evaluate(x, y, v, primary, input)

        is Ge -> l.evaluate(x, y, v, primary, input) >= r.evaluate(x, y, v, primary, input)

        is Lt -> l.evaluate(x, y, v, primary, input) < r.evaluate(x, y, v, primary, input)

        is Le -> l.evaluate(x, y, v, primary, input) <= r.evaluate(x, y, v, primary, input)

        is Eq -> l.evaluate(x, y, v, primary, input) == r.evaluate(x, y, v, primary, input)

        is And -> l.evaluate(x, y, v, primary, input) && r.evaluate(x, y, v, primary, input)

        is Or -> l.evaluate(x, y, v, primary, input) || r.evaluate(x, y, v, primary, input)

        is Not -> !a.evaluate(x, y, v, primary, input)

        // Primitive equality preserves the same NaN and signed-zero semantics as Eq and Switch.
        is In -> {
            val value = of.evaluate(x, y, v, primary, input)
            var matches = false
            for (candidate in values) {
                if (value == candidate) {
                    matches = true
                    break
                }
            }
            matches
        }

        is IsNaN -> of.evaluate(x, y, v, primary, input).isNaN()

        is IsFinite -> of.evaluate(x, y, v, primary, input).isFinite()

        AllFinite -> x.isFinite() && y.isFinite() && input.allFinite(v)

        is InRange -> a.evaluate(x, y, v, primary, input) in min..max
    }

private fun <T> VectorExpr.evaluate(x: Double, y: Double, v: T, primary: Result?, input: ExprInput<T>): DoubleArray =
    when (this) {
        is BoundVectorExpr -> resolved.evaluate(x, y, v, primary, input)
        is VElements -> DoubleArray(exprs.size) { i -> exprs[i].evaluate(x, y, v, primary, input) }
    }

private interface ExprInput<T> {
    fun get(v: T, index: Int): Double

    fun fold(v: T, op: VFoldOp): Double

    fun dot(v: T, weights: List<Double>): Double

    fun allFinite(v: T): Boolean
}

private object ArrayInput : ExprInput<DoubleArray> {
    override fun get(v: DoubleArray, index: Int): Double = v[index]

    override fun fold(v: DoubleArray, op: VFoldOp): Double = when (op) {
        VFoldOp.Sum -> koblas.vectorKernels.sum(v, 0, v.size)

        VFoldOp.Product -> {
            var p = 1.0
            for (e in v) p *= e
            p
        }

        VFoldOp.Mean -> {
            require(v.isNotEmpty()) { "VFold.Mean on empty vector" }
            koblas.vectorKernels.sum(v, 0, v.size) / v.size
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

    override fun dot(v: DoubleArray, weights: List<Double>): Double {
        require(v.size == weights.size) {
            "VDot length mismatch: weights=${weights.size}, v=${v.size}"
        }
        var s = 0.0
        for (i in 0 until v.size) s += weights[i] * v[i]
        return s
    }

    override fun allFinite(v: DoubleArray): Boolean = v.all { it.isFinite() }
}

private object VectorInput : ExprInput<Vector> {
    override fun get(v: Vector, index: Int): Double = v[index]

    override fun fold(v: Vector, op: VFoldOp): Double {
        if (v is SparseVector) {
            when (op) {
                VFoldOp.Min -> return sparseMin(v)

                VFoldOp.Max -> return sparseMax(v)

                VFoldOp.Norm2 -> return sparseNorm2(v)

                VFoldOp.Sum,
                VFoldOp.Mean,
                VFoldOp.Product,
                -> Unit
            }
        }
        return when (op) {
            VFoldOp.Sum -> v.sum()

            VFoldOp.Product -> (0 until v.size).fold(1.0) { product, i -> product * v[i] }

            VFoldOp.Mean -> {
                require(v.size != 0) { "VFold.Mean on empty vector" }
                v.sum() / v.size
            }

            VFoldOp.Min -> {
                require(v.size != 0) { "VFold.Min on empty vector" }
                var minimum = v[0]
                for (i in 1 until v.size) if (v[i] < minimum) minimum = v[i]
                minimum
            }

            VFoldOp.Max -> {
                require(v.size != 0) { "VFold.Max on empty vector" }
                var maximum = v[0]
                for (i in 1 until v.size) if (v[i] > maximum) maximum = v[i]
                maximum
            }

            VFoldOp.Norm2 -> sqrt((0 until v.size).sumOf { v[it] * v[it] })
        }
    }

    override fun dot(v: Vector, weights: List<Double>): Double {
        require(v.size == weights.size) {
            "VDot length mismatch: weights=${weights.size}, v=${v.size}"
        }
        // Skipping implicit zeros is valid only when multiplying them cannot produce NaN.
        if (v is SparseVector && weights.all { it.isFinite() }) return sparseDot(v, weights)
        return (0 until v.size).sumOf { weights[it] * v[it] }
    }

    override fun allFinite(v: Vector): Boolean {
        var finite = true
        v.forEachStored { _, value -> if (!value.isFinite()) finite = false }
        return finite
    }
}

private fun sparseNorm2(v: SparseVector): Double {
    var sum = 0.0
    v.forEachStored { _, value -> sum += value * value }
    return sqrt(sum)
}

private fun sparseDot(v: SparseVector, weights: List<Double>): Double {
    var sum = 0.0
    v.forEachStored { index, value -> sum += weights[index] * value }
    return sum
}

private fun sparseMin(v: SparseVector): Double {
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

private fun sparseMax(v: SparseVector): Double {
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

@Serializable
@SerialName("VElements")
internal data class VElements(val exprs: List<ScalarExpr>) : VectorExpr {
    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): DoubleArray =
        evaluate(x, y, v, primary, ArrayInput)
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

/**
 * Exact inequality, the negation of [eq] under IEEE 754: true when either side is `NaN`, and false
 * for `0.0` against `-0.0`. It builds `!(this eq rhs)`, which encodes with the portable `not` and `eq`.
 */
infix fun ScalarExpr.ne(rhs: ScalarExpr): BoolExpr = Not(Eq(this, rhs))

/** Exact inequality against a literal; see [ne]. */
infix fun ScalarExpr.ne(rhs: Double): BoolExpr = Not(Eq(this, Const(rhs)))

/** Strictly-greater-than with a literal left operand. */
infix fun Double.gt(rhs: ScalarExpr): BoolExpr = Gt(Const(this), rhs)

/** Greater-or-equal with a literal left operand. */
infix fun Double.ge(rhs: ScalarExpr): BoolExpr = Ge(Const(this), rhs)

/** Strictly-less-than with a literal left operand. */
infix fun Double.lt(rhs: ScalarExpr): BoolExpr = Lt(Const(this), rhs)

/** Less-or-equal with a literal left operand. */
infix fun Double.le(rhs: ScalarExpr): BoolExpr = Le(Const(this), rhs)

/** Exact equality with a literal left operand (no tolerance). */
infix fun Double.eq(rhs: ScalarExpr): BoolExpr = Eq(Const(this), rhs)

/** Exact inequality with a literal left operand; see [ne]. */
infix fun Double.ne(rhs: ScalarExpr): BoolExpr = Not(Eq(Const(this), rhs))

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

/** This expression raised to an integer [exponent], which binary64 represents exactly. */
fun ScalarExpr.pow(exponent: Int): ScalarExpr = Pow(this, Const(exponent.toDouble()))

/** The smaller of this expression and [other]. */
fun ScalarExpr.min(other: ScalarExpr): ScalarExpr = MinExpr(this, other)

/** The smaller of this expression and a constant [other]. */
fun ScalarExpr.min(other: Double): ScalarExpr = MinExpr(this, Const(other))

/** The larger of this expression and [other]. */
fun ScalarExpr.max(other: ScalarExpr): ScalarExpr = MaxExpr(this, other)

/** The larger of this expression and a constant [other]. */
fun ScalarExpr.max(other: Double): ScalarExpr = MaxExpr(this, Const(other))

/** [then] when [cond] holds, else [otherwise]; only the selected branch is evaluated. Builds an [IfExpr]. */
fun ifThenElse(cond: BoolExpr, then: ScalarExpr, otherwise: ScalarExpr): ScalarExpr = IfExpr(cond, then, otherwise)

/** [then] when [cond] holds, else [otherwise], with a literal [then] branch. */
fun ifThenElse(cond: BoolExpr, then: Double, otherwise: ScalarExpr): ScalarExpr = IfExpr(cond, Const(then), otherwise)

/** [then] when [cond] holds, else [otherwise], with a literal [otherwise] branch. */
fun ifThenElse(cond: BoolExpr, then: ScalarExpr, otherwise: Double): ScalarExpr = IfExpr(cond, then, Const(otherwise))

/** [then] when [cond] holds, else [otherwise], with literal branches. */
fun ifThenElse(cond: BoolExpr, then: Double, otherwise: Double): ScalarExpr =
    IfExpr(cond, Const(then), Const(otherwise))

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
