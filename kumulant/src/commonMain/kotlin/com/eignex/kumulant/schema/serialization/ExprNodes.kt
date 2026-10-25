@file:Suppress("UNCHECKED_CAST")

package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.stat.regression.glm.*

internal val scalarExprNodes: List<WireNode> by lazy {
    listOf(
        WireNode(X::class, "input", WireShape.Reference, listOf("value")) { X.serializer() },
        WireNode(Y::class, "input", WireShape.Reference, listOf("response")) { Y.serializer() },
        WireNode(Center::class, "feedback", WireShape.Reference, listOf("center")) { Center.serializer() },
        WireNode(Scale::class, "feedback", WireShape.Reference, listOf("scale")) { Scale.serializer() },
        WireNode(Low::class, "feedback", WireShape.Reference, listOf("min")) { Low.serializer() },
        WireNode(High::class, "feedback", WireShape.Reference, listOf("max")) { High.serializer() },
        WireNode(VIndex::class, "input", WireShape.Reference, listOf("index")) { VIndex.serializer() },
        WireNode(V::class, "element", WireShape.Operand, listOf("index")) { V.serializer() },
        WireNode(Const::class, "const", WireShape.Literal, listOf("v")) { Const.serializer() },
        WireNode(Add::class, "add", WireShape.Operands, listOf("l", "r")) { Add.serializer() },
        WireNode(Sub::class, "sub", WireShape.Operands, listOf("l", "r")) { Sub.serializer() },
        WireNode(Mul::class, "mul", WireShape.Operands, listOf("l", "r")) { Mul.serializer() },
        WireNode(Div::class, "div", WireShape.Operands, listOf("l", "r")) { Div.serializer() },
        WireNode(Neg::class, "neg", WireShape.Operand, listOf("a")) { Neg.serializer() },
        WireNode(Abs::class, "abs", WireShape.Operand, listOf("a")) { Abs.serializer() },
        WireNode(Log::class, "log", WireShape.Operand, listOf("a")) { Log.serializer() },
        WireNode(Exp::class, "exp", WireShape.Operand, listOf("a")) { Exp.serializer() },
        WireNode(Sqrt::class, "sqrt", WireShape.Operand, listOf("a")) { Sqrt.serializer() },
        WireNode(Pow::class, "pow", WireShape.Operands, listOf("a", "b")) { Pow.serializer() },
        WireNode(MinExpr::class, "min", WireShape.Operands, listOf("l", "r")) { MinExpr.serializer() },
        WireNode(MaxExpr::class, "max", WireShape.Operands, listOf("l", "r")) { MaxExpr.serializer() },
        WireNode(IfExpr::class, "if", WireShape.Fields) { IfExpr.serializer() },
        WireNode(Switch::class, "switch", WireShape.Fields) { Switch.serializer() },
        WireNode(Standardize::class, "standardize", WireShape.Fields) { Standardize.serializer() },
        WireNode(MinMax::class, "minMax", WireShape.Fields) { MinMax.serializer() },
        WireNode(VFold::class, "fold", WireShape.Fields) { VFold.serializer() },
        WireNode(VDot::class, "dot", WireShape.Operand, listOf("weights")) { VDot.serializer() },
    )
}

internal object ScalarExprSerializer : CanonicalSerializer<ScalarExpr>(ScalarExpr::class, { scalarExprNodes })

internal val boolExprNodes: List<WireNode> by lazy {
    listOf(
        WireNode(In::class, "in", WireShape.Fields) { In.serializer() },
        WireNode(Gt::class, "gt", WireShape.Operands, listOf("l", "r")) { Gt.serializer() },
        WireNode(Ge::class, "ge", WireShape.Operands, listOf("l", "r")) { Ge.serializer() },
        WireNode(Lt::class, "lt", WireShape.Operands, listOf("l", "r")) { Lt.serializer() },
        WireNode(Le::class, "le", WireShape.Operands, listOf("l", "r")) { Le.serializer() },
        WireNode(Eq::class, "eq", WireShape.Operands, listOf("l", "r")) { Eq.serializer() },
        WireNode(And::class, "and", WireShape.Operands, listOf("l", "r")) { And.serializer() },
        WireNode(Or::class, "or", WireShape.Operands, listOf("l", "r")) { Or.serializer() },
        WireNode(Not::class, "not", WireShape.Operand, listOf("a")) { Not.serializer() },
        WireNode(IsNaN::class, "isNaN", WireShape.Operand, listOf("of")) { IsNaN.serializer() },
        WireNode(IsFinite::class, "isFinite", WireShape.Operand, listOf("of")) { IsFinite.serializer() },
        WireNode(AllFinite::class, "allFinite", WireShape.Fields) { AllFinite.serializer() },
        WireNode(InRange::class, "inRange", WireShape.Fields) { InRange.serializer() },
    )
}

internal object BoolExprSerializer : CanonicalSerializer<BoolExpr>(BoolExpr::class, { boolExprNodes })

internal val vectorExprNodes: List<WireNode> by lazy {
    listOf(
        WireNode(VElements::class, "elements", WireShape.Operand, listOf("exprs")) { VElements.serializer() },
    )
}

internal object VectorExprSerializer : CanonicalSerializer<VectorExpr>(VectorExpr::class, { vectorExprNodes })
