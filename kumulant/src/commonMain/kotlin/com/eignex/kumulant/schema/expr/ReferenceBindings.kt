package com.eignex.kumulant.schema.expr

import com.eignex.kumulant.core.Result
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient

/** Named scalar reference. Bind it explicitly before evaluating or materializing a statistical recipe. */
@Serializable
@SerialName("Ref")
data class Ref(
    /** Name looked up in the enclosing binding scope. */
    val name: String,
) : ScalarExpr {
    init {
        require(name.isNotEmpty()) { "A reference name must not be empty" }
    }

    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double =
        error("Unresolved reference '$name'; supply explicit bindings")
}

@Serializable
@SerialName("BoundScalarExpr")
internal data class BoundScalarExpr(val expr: ScalarExpr, val bindings: Map<String, ScalarExpr>) : ScalarExpr {
    @Transient
    internal val resolved: ScalarExpr = ReferenceResolver(bindings).scalar(expr)

    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Double = resolved.eval(x, y, v, primary)
}

@Serializable
@SerialName("BoundBoolExpr")
internal data class BoundBoolExpr(val expr: BoolExpr, val bindings: Map<String, ScalarExpr>) : BoolExpr {
    @Transient
    internal val resolved: BoolExpr = ReferenceResolver(bindings).boolean(expr)

    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): Boolean = resolved.eval(x, y, v, primary)
}

@Serializable
@SerialName("BoundVectorExpr")
internal data class BoundVectorExpr(val expr: VectorExpr, val bindings: Map<String, ScalarExpr>) : VectorExpr {
    @Transient
    internal val resolved: VectorExpr = ReferenceResolver(bindings).vector(expr)

    override fun eval(x: Double, y: Double, v: DoubleArray, primary: Result?): DoubleArray =
        resolved.eval(x, y, v, primary)
}

/** Bind every reference eagerly, preserving the tree's grouping and retaining the scope on the wire. */
fun ScalarExpr.bind(bindings: Map<String, ScalarExpr>): ScalarExpr = BoundScalarExpr(this, bindings.toMap())

/** Bind every scalar reference, including references in branches that may not execute. */
fun BoolExpr.bind(bindings: Map<String, ScalarExpr>): BoolExpr = BoundBoolExpr(this, bindings.toMap())

/** Bind every scalar reference in a vector projection. Nested binding scopes resolve independently. */
fun VectorExpr.bind(bindings: Map<String, ScalarExpr>): VectorExpr = BoundVectorExpr(this, bindings.toMap())

internal fun ScalarExpr.requireBound(path: String) = requireBoundAt(path) { ReferenceResolver(emptyMap()).scalar(this) }
internal fun BoolExpr.requireBound(path: String) = requireBoundAt(path) { ReferenceResolver(emptyMap()).boolean(this) }
internal fun VectorExpr.requireBound(path: String) = requireBoundAt(path) { ReferenceResolver(emptyMap()).vector(this) }

private inline fun requireBoundAt(path: String, resolve: () -> Any) {
    try {
        resolve()
    } catch (error: IllegalArgumentException) {
        throw IllegalArgumentException("$path: ${error.message}", error)
    }
}

private class ReferenceResolver(private val bindings: Map<String, ScalarExpr>) {
    private val active = mutableSetOf<String>()
    private val resolved = mutableMapOf<String, ScalarExpr>()

    private fun reference(name: String): ScalarExpr {
        resolved[name]?.let { return it }
        val source = requireNotNull(bindings[name]) { "Unresolved reference '$name'" }
        require(active.add(name)) { "Cyclic reference '$name'" }
        return try {
            scalar(source).also { resolved[name] = it }
        } finally {
            active.remove(name)
        }
    }

    fun scalar(expr: ScalarExpr): ScalarExpr = when (expr) {
        is Ref -> reference(expr.name)

        is Add -> expr.copy(l = scalar(expr.l), r = scalar(expr.r))

        is Sub -> expr.copy(l = scalar(expr.l), r = scalar(expr.r))

        is Mul -> expr.copy(l = scalar(expr.l), r = scalar(expr.r))

        is Div -> expr.copy(l = scalar(expr.l), r = scalar(expr.r))

        is Neg -> expr.copy(a = scalar(expr.a))

        is Abs -> expr.copy(a = scalar(expr.a))

        is Log -> expr.copy(a = scalar(expr.a))

        is Exp -> expr.copy(a = scalar(expr.a))

        is Sqrt -> expr.copy(a = scalar(expr.a))

        is Pow -> expr.copy(a = scalar(expr.a), b = scalar(expr.b))

        is MinExpr -> expr.copy(l = scalar(expr.l), r = scalar(expr.r))

        is MaxExpr -> expr.copy(l = scalar(expr.l), r = scalar(expr.r))

        is IfExpr -> expr.copy(cond = boolean(expr.cond), then = scalar(expr.then), otherwise = scalar(expr.otherwise))

        is Switch -> expr.copy(
            on = scalar(expr.on),
            cases = expr.cases.map { it.copy(then = scalar(it.then)) },
            otherwise = scalar(expr.otherwise),
        )

        X, Y, Center, Scale, Low, High, VIndex, Standardize,
        is V, is Const, is MinMax, is VFold, is VDot, is BoundScalarExpr,
        -> expr
    }

    fun boolean(expr: BoolExpr): BoolExpr = when (expr) {
        is Gt -> expr.copy(l = scalar(expr.l), r = scalar(expr.r))
        is Ge -> expr.copy(l = scalar(expr.l), r = scalar(expr.r))
        is Lt -> expr.copy(l = scalar(expr.l), r = scalar(expr.r))
        is Le -> expr.copy(l = scalar(expr.l), r = scalar(expr.r))
        is Eq -> expr.copy(l = scalar(expr.l), r = scalar(expr.r))
        is And -> expr.copy(l = boolean(expr.l), r = boolean(expr.r))
        is Or -> expr.copy(l = boolean(expr.l), r = boolean(expr.r))
        is Not -> expr.copy(a = boolean(expr.a))
        is In -> expr.copy(of = scalar(expr.of))
        is IsNaN -> expr.copy(of = scalar(expr.of))
        is IsFinite -> expr.copy(of = scalar(expr.of))
        is InRange -> expr.copy(a = scalar(expr.a))
        AllFinite, is BoundBoolExpr -> expr
    }

    fun vector(expr: VectorExpr): VectorExpr = when (expr) {
        is VElements -> expr.copy(exprs = expr.exprs.map(::scalar))
        is BoundVectorExpr -> expr
    }
}
