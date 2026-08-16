package com.eignex.kumulant.stat.regression.tree

import kotlin.math.ceil
import kotlin.math.sqrt

/**
 * Breiman's default random-subspace size: `ceil(sqrt(p))` candidates drawn per audit leaf.
 *
 * Both forests default `mtry` to this when the caller leaves it null, and both used to carry their own
 * private copy of the expression. The clamp matters at the small end - `ceil(sqrt(1))` is already 1, but
 * `coerceAtLeast(1)` keeps an mtry of zero from disabling growth silently if the arithmetic is ever
 * changed - and `p <= 0` returns 0 rather than 1 because an empty candidate pool means no growth at all
 * rather than growth on one candidate that does not exist.
 *
 * @param p size of the tree's full candidate-split pool.
 */
internal fun defaultMtry(p: Int): Int = if (p <= 0) 0 else ceil(sqrt(p.toDouble())).toInt().coerceAtLeast(1)
