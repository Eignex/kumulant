package com.eignex.kumulant.stat.regression.glm

import com.eignex.kumulant.schema.expr.Const
import com.eignex.kumulant.schema.expr.Exp
import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.schema.expr.X
import com.eignex.kumulant.schema.expr.div
import com.eignex.kumulant.schema.expr.plus
import com.eignex.kumulant.schema.expr.times
import com.eignex.kumulant.schema.expr.unaryMinus

// Convenience constructors for the common learning-rate schedules, expressed as
// [ScalarExpr] over the step counter `X`. `RegressionStat` implementations evaluate
// the expression at `step.toDouble()` per update, so any wire-portable scalar
// function of the step works - these factories are just terse names for the three
// recipes that occur in practice.
//
// A regression stat takes its schedule as a plain [ScalarExpr], so callers can pass
// either a factory result or hand-build something arbitrary:
//
// ```kotlin
// StochasticRegressionStat(featureSize = 16, optimizer = Sgd(ConstantRate(0.05)))
// StochasticRegressionStat(featureSize = 16, learningRate = StepDecay(0.01, 1e-3))
// StochasticRegressionStat(featureSize = 16, learningRate = 0.01 * Exp(-1e-5 * X))
// ```

/** Constant rate `eta`. */
@Suppress("FunctionNaming") // PascalCase to read like a constructor at call sites
fun ConstantRate(eta: Double = 1e-3): ScalarExpr = Const(eta)

/** `eta / (1 + k * step)`. */
@Suppress("FunctionNaming")
fun StepDecay(eta: Double = 1e-2, k: Double = 1e-3): ScalarExpr = Const(eta) / (Const(1.0) + Const(k) * X)

/** `eta * exp(-k * step)`. */
@Suppress("FunctionNaming")
fun ExponentialDecay(eta: Double = 1e-2, k: Double = 1e-5): ScalarExpr = Const(eta) * Exp(-Const(k) * X)
