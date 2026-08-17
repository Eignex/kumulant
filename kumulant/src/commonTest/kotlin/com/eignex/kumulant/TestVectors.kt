package com.eignex.kumulant

import com.eignex.koblas.DenseVector

/**
 * A feature vector, for the tests that need one.
 *
 * Fourteen test files declared this same one-liner, seven of them as a class member and seven as a
 * file-private function. It is here for the same reason [DELTA] is: not to save the lines, but so that
 * `DenseVector.of` has one call site in the test suite. Koblas moved its dense storage to column-major
 * after `v0.1.0`, a change no compiler catches, and a single point of construction is what makes the next
 * such change one edit rather than fourteen.
 */
internal fun feat(vararg xs: Double): DenseVector = DenseVector.of(xs)
