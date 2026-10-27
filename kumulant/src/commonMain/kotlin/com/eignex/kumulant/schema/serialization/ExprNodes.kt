package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.schema.expr.*
import com.eignex.skema.serialization.JsonNodeShape
import com.eignex.skema.serialization.jsonNode

internal object ScalarExprSerializer : KumulantSerializer<ScalarExpr>(ScalarExpr::class, {
    listOf(
        jsonNode<Ref>("ref", JsonNodeShape.Operand, "name"),
        jsonNode<BoundScalarExpr>("bind"),
        jsonNode<X>("input", mapping = InputMapping("x")),
        jsonNode<Y>("input", mapping = InputMapping("y")),
        jsonNode<Center>("feedback", mapping = InputMapping("center")),
        jsonNode<Scale>("feedback", mapping = InputMapping("scale")),
        jsonNode<Low>("feedback", mapping = InputMapping("low")),
        jsonNode<High>("feedback", mapping = InputMapping("high")),
        jsonNode<VIndex>("feedback", mapping = InputMapping("index")),
        jsonNode<V>("element", JsonNodeShape.Operand, "index"),
        jsonNode<Const>("const", JsonNodeShape.Literal, "v", mapping = NumberMapping),
        jsonNode<Add>("add", JsonNodeShape.Operands, "l", "r"),
        jsonNode<Sub>("sub", JsonNodeShape.Operands, "l", "r"),
        jsonNode<Mul>("mul", JsonNodeShape.Operands, "l", "r"),
        jsonNode<Div>("div", JsonNodeShape.Operands, "l", "r"),
        jsonNode<Neg>("neg", JsonNodeShape.Operand, "a"),
        jsonNode<Abs>("abs", JsonNodeShape.Operand, "a"),
        jsonNode<Log>("log", JsonNodeShape.Operand, "a"),
        jsonNode<Exp>("exp", JsonNodeShape.Operand, "a"),
        jsonNode<Sqrt>("sqrt", JsonNodeShape.Operand, "a"),
        jsonNode<Pow>("pow", JsonNodeShape.Operands, "a", "b"),
        jsonNode<MinExpr>("min", JsonNodeShape.Operands, "l", "r"),
        jsonNode<MaxExpr>("max", JsonNodeShape.Operands, "l", "r"),
        jsonNode<IfExpr>("if", JsonNodeShape.Operands, "cond", "then", "otherwise"),
        jsonNode<Switch>("switch"),
        jsonNode<Standardize>("standardize"),
        jsonNode<MinMax>("minMax"),
        jsonNode<VFold>("fold"),
        jsonNode<VDot>("dot", JsonNodeShape.Operand, "weights"),
    )
})

internal object BoolExprSerializer : KumulantSerializer<BoolExpr>(BoolExpr::class, {
    listOf(
        jsonNode<BoundBoolExpr>("bind"),
        jsonNode<In>("in"),
        jsonNode<Gt>("gt", JsonNodeShape.Operands, "l", "r"),
        jsonNode<Ge>("ge", JsonNodeShape.Operands, "l", "r"),
        jsonNode<Lt>("lt", JsonNodeShape.Operands, "l", "r"),
        jsonNode<Le>("le", JsonNodeShape.Operands, "l", "r"),
        jsonNode<Eq>("eq", JsonNodeShape.Operands, "l", "r"),
        jsonNode<And>("and", JsonNodeShape.Operands, "l", "r"),
        jsonNode<Or>("or", JsonNodeShape.Operands, "l", "r"),
        jsonNode<Not>("not", JsonNodeShape.Operand, "a"),
        jsonNode<IsNaN>("isNaN", JsonNodeShape.Operand, "of"),
        jsonNode<IsFinite>("isFinite", JsonNodeShape.Operand, "of"),
        jsonNode<AllFinite>("allFinite"),
        jsonNode<InRange>("inRange"),
    )
})

internal object VectorExprSerializer : KumulantSerializer<VectorExpr>(VectorExpr::class, {
    listOf(
        jsonNode<BoundVectorExpr>("bind"),
        jsonNode<VElements>("elements", JsonNodeShape.Operand, "exprs"),
    )
})
