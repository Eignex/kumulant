package com.eignex.kumulant.bandit

/**
 * Floor applied to the played probability before it divides an importance-weighted reward.
 *
 * EXP3 and EXP4 both estimate an unbiased gain as `reward / p(played arm)`, so an arm the policy had
 * all but written off produces an enormous gain estimate, and one it had written off entirely produces
 * a non-finite one. The floor bounds that blow-up. It is deliberately far below any probability the
 * exploration term `gamma / K` can produce, so it never perturbs a live distribution.
 */
internal const val MIN_PLAY_PROB: Double = 1e-12

/** Above this maximum weight, rescale before `exp` overflows to infinity. */
internal const val RENORM_THRESHOLD: Double = 1e100

/** Below this maximum weight, rescale before the whole array underflows to zero. */
internal const val RENORM_FLOOR: Double = 1e-100

/**
 * Keep an array of exponential weights inside the range where `exp` still carries information.
 *
 * The distribution these weights induce depends only on their *ratios*, so dividing the array through
 * by its maximum is free: it changes no arm's or expert's relative standing. That is what makes both
 * ends of the range recoverable rather than just the top one.
 *
 * Both ends need guarding, and only the overflow end used to be. A run of large negative rewards drives
 * every `exp(eta * gain)` toward zero, and once the array is *entirely* zero no later reward can lift
 * it: every subsequent update multiplies zero by something. Rescaling by the surviving maximum keeps
 * the array alive; the fully collapsed case has no ratios left to preserve, so it resets to uniform.
 *
 * EXP3 and EXP4 carried separate copies of this, spelled with different branch structure - EXP3 tested
 * the ceiling and the floor in two `if`s, EXP4 in one disjunction - but they compute the same thing.
 */
internal fun DoubleArray.renormaliseExponentialWeights() {
    var maxW = 0.0
    for (w in this) if (w > maxW) maxW = w
    when {
        maxW > RENORM_THRESHOLD || (maxW > 0.0 && maxW < RENORM_FLOOR) ->
            for (i in indices) this[i] /= maxW

        maxW == 0.0 -> for (i in indices) this[i] = 1.0
    }
}
