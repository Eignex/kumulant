package com.eignex.kumulant.schema.expr

import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.HasMinMax
import com.eignex.kumulant.core.Result

// Inputs an expression reads beyond x and y, each named by the first node that reads it.
internal class ExprInputs {
    var vectorNode: String? = null
        private set
    var indexNode: String? = null
        private set
    var centerScaleNode: String? = null
        private set
    var minMaxNode: String? = null
        private set

    val readsFeedback: Boolean get() = centerScaleNode != null || minMaxNode != null

    enum class Scope(val vector: Boolean, val feedback: Boolean, val indexed: Boolean) {
        Scalar(vector = false, feedback = false, indexed = false),
        Vector(vector = true, feedback = false, indexed = false),
        Feedback(vector = false, feedback = true, indexed = false),
        IndexedFeedback(vector = false, feedback = true, indexed = true),
    }

    fun requireScope(path: String, scope: Scope) {
        vectorNode?.let {
            require(scope.vector) { "$path: $it reads a vector input that this context does not provide" }
        }
        indexNode?.let { require(scope.indexed) { "$path: $it requires an element-wise feedback context" } }
        val feedbackNode = centerScaleNode ?: minMaxNode
        feedbackNode?.let { require(scope.feedback) { "$path: $it requires a feedback primary" } }
    }

    fun requirePrimary(path: String, primary: Result) {
        centerScaleNode?.let {
            require(primary is HasCenterScale) {
                "$path: $it requires a HasCenterScale feedback primary; got ${primary::class.simpleName}"
            }
        }
        minMaxNode?.let {
            require(primary is HasMinMax) {
                "$path: $it requires a HasMinMax feedback primary; got ${primary::class.simpleName}"
            }
        }
    }

    fun scalar(expr: ScalarExpr) {
        when (expr) {
            is BoundScalarExpr -> scalar(expr.resolved)

            is V -> vectorNode = vectorNode ?: "V"

            is VFold -> vectorNode = vectorNode ?: "VFold"

            is VDot -> vectorNode = vectorNode ?: "VDot"

            VIndex -> indexNode = indexNode ?: "VIndex"

            Center -> centerScaleNode = centerScaleNode ?: "Center"

            Scale -> centerScaleNode = centerScaleNode ?: "Scale"

            Standardize -> centerScaleNode = centerScaleNode ?: "Standardize"

            Low -> minMaxNode = minMaxNode ?: "Low"

            High -> minMaxNode = minMaxNode ?: "High"

            is MinMax -> minMaxNode = minMaxNode ?: "MinMax"

            is Add -> scalar(expr.l, expr.r)

            is Sub -> scalar(expr.l, expr.r)

            is Mul -> scalar(expr.l, expr.r)

            is Div -> scalar(expr.l, expr.r)

            is MinExpr -> scalar(expr.l, expr.r)

            is MaxExpr -> scalar(expr.l, expr.r)

            is Pow -> scalar(expr.a, expr.b)

            is Neg -> scalar(expr.a)

            is Abs -> scalar(expr.a)

            is Log -> scalar(expr.a)

            is Exp -> scalar(expr.a)

            is Sqrt -> scalar(expr.a)

            is IfExpr -> {
                boolean(expr.cond)
                scalar(expr.then, expr.otherwise)
            }

            is Switch -> {
                scalar(expr.on, expr.otherwise)
                expr.cases.forEach { scalar(it.then) }
            }

            X, Y, is Const, is Ref -> Unit
        }
    }

    private fun scalar(left: ScalarExpr, right: ScalarExpr) {
        scalar(left)
        scalar(right)
    }

    fun boolean(expr: BoolExpr) {
        when (expr) {
            is BoundBoolExpr -> boolean(expr.resolved)

            is Gt -> scalar(expr.l, expr.r)

            is Ge -> scalar(expr.l, expr.r)

            is Lt -> scalar(expr.l, expr.r)

            is Le -> scalar(expr.l, expr.r)

            is Eq -> scalar(expr.l, expr.r)

            is And -> {
                boolean(expr.l)
                boolean(expr.r)
            }

            is Or -> {
                boolean(expr.l)
                boolean(expr.r)
            }

            is Not -> boolean(expr.a)

            is In -> scalar(expr.of)

            is IsNaN -> scalar(expr.of)

            is IsFinite -> scalar(expr.of)

            is InRange -> scalar(expr.a)

            AllFinite -> Unit
        }
    }

    fun vector(expr: VectorExpr) {
        when (expr) {
            is BoundVectorExpr -> vector(expr.resolved)
            is VElements -> expr.exprs.forEach(::scalar)
        }
    }
}

internal fun ScalarExpr.inputs(): ExprInputs = ExprInputs().also { it.scalar(this) }

internal fun BoolExpr.inputs(): ExprInputs = ExprInputs().also { it.boolean(this) }

internal fun VectorExpr.inputs(): ExprInputs = ExprInputs().also { it.vector(this) }
