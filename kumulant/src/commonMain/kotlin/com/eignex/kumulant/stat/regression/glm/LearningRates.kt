package com.eignex.kumulant.stat.regression.glm

import com.eignex.kumulant.schema.expr.Const
import com.eignex.kumulant.schema.expr.Exp
import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.schema.expr.X
import com.eignex.kumulant.schema.expr.div
import com.eignex.kumulant.schema.expr.plus
import com.eignex.kumulant.schema.expr.times
import com.eignex.kumulant.schema.expr.unaryMinus

// Learning-rate schedules evaluate ScalarExpr with the update step bound to X.

/** Constant rate `eta`. */
@Suppress("FunctionNaming") // PascalCase to read like a constructor at call sites
fun ConstantRate(eta: Double = 1e-3): ScalarExpr = Const(eta)

/** `eta / (1 + k * step)`. */
@Suppress("FunctionNaming")
fun StepDecay(eta: Double = 1e-2, k: Double = 1e-3): ScalarExpr = Const(eta) / (Const(1.0) + Const(k) * X)

/** `eta * exp(-k * step)`. */
@Suppress("FunctionNaming")
fun ExponentialDecay(eta: Double = 1e-2, k: Double = 1e-5): ScalarExpr = Const(eta) * Exp(-Const(k) * X)
