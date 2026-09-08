package com.eignex.kumulant

import com.eignex.koblas.DenseVector

/**
 * A feature vector, for the tests that need one.
 *
 * One call site for `DenseVector.of` across the suite: koblas moved its dense storage to column-major
 * after `v0.1.0`, a change no compiler catches, so the next one should be a single edit.
 */
internal fun feat(vararg xs: Double): DenseVector = DenseVector.of(xs)
