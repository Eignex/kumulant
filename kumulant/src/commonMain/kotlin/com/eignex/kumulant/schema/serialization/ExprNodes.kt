package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.schema.expr.*

internal object ScalarExprSerializer : CanonicalSerializer<ScalarExpr>(
    ScalarExpr::class,
    node<X>("input", WireShape.Reference, "value"),
    node<Y>("input", WireShape.Reference, "response"),
    node<Center>("feedback", WireShape.Reference, "center"),
    node<Scale>("feedback", WireShape.Reference, "scale"),
    node<Low>("feedback", WireShape.Reference, "min"),
    node<High>("feedback", WireShape.Reference, "max"),
    node<VIndex>("input", WireShape.Reference, "index"),
    node<V>("element", WireShape.Operand, "index"),
    node<Const>("const", WireShape.Literal, "v"),
    node<Add>("add", WireShape.Operands, "l", "r"),
    node<Sub>("sub", WireShape.Operands, "l", "r"),
    node<Mul>("mul", WireShape.Operands, "l", "r"),
    node<Div>("div", WireShape.Operands, "l", "r"),
    node<Neg>("neg", WireShape.Operand, "a"),
    node<Abs>("abs", WireShape.Operand, "a"),
    node<Log>("log", WireShape.Operand, "a"),
    node<Exp>("exp", WireShape.Operand, "a"),
    node<Sqrt>("sqrt", WireShape.Operand, "a"),
    node<Pow>("pow", WireShape.Operands, "a", "b"),
    node<MinExpr>("min", WireShape.Operands, "l", "r"),
    node<MaxExpr>("max", WireShape.Operands, "l", "r"),
    node<IfExpr>("if"),
    node<Switch>("switch"),
    node<Standardize>("standardize"),
    node<MinMax>("minMax"),
    node<VFold>("fold"),
    node<VDot>("dot", WireShape.Operand, "weights"),
)

internal object BoolExprSerializer : CanonicalSerializer<BoolExpr>(
    BoolExpr::class,
    node<In>("in"),
    node<Gt>("gt", WireShape.Operands, "l", "r"),
    node<Ge>("ge", WireShape.Operands, "l", "r"),
    node<Lt>("lt", WireShape.Operands, "l", "r"),
    node<Le>("le", WireShape.Operands, "l", "r"),
    node<Eq>("eq", WireShape.Operands, "l", "r"),
    node<And>("and", WireShape.Operands, "l", "r"),
    node<Or>("or", WireShape.Operands, "l", "r"),
    node<Not>("not", WireShape.Operand, "a"),
    node<IsNaN>("isNaN", WireShape.Operand, "of"),
    node<IsFinite>("isFinite", WireShape.Operand, "of"),
    node<AllFinite>("allFinite"),
    node<InRange>("inRange"),
)

internal object VectorExprSerializer : CanonicalSerializer<VectorExpr>(
    VectorExpr::class,
    node<VElements>("elements", WireShape.Operand, "exprs"),
)
