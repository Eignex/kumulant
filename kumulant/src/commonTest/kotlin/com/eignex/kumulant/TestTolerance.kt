package com.eignex.kumulant

/**
 * The comparison tolerance for floating-point assertions across the test suite.
 *
 * Eighty test files each declared their own `private const val DELTA`, at three different values: 45 at
 * `1e-12`, 34 at `1e-9`, and one at `1e-6`. The spread encoded nothing. Every one of those files passes
 * at `1e-12` on all thirteen targets, including the native and wasm ones, whose libm differs from the
 * JVM's - so the looser values were not headroom anybody had measured a need for, they were just the
 * number the file's author happened to write.
 *
 * One value, so that a file which genuinely needs slack has to say so with a local override and a reason.
 * That is the point of consolidating: not saving seventy-nine lines, but making a loosened tolerance
 * visible as a decision instead of indistinguishable from the default.
 *
 * `1e-12` rather than something tighter because these are `Double` comparisons after accumulation over a
 * stream, not single operations. Roughly three decimal digits of slack against the `2.2e-16` epsilon
 * absorbs the reassociation a Welford or Chan recurrence performs without absorbing a real error.
 *
 * A test asserting something that should be *bit*-exact - a downdate restoring a prior state, a merge
 * matching a direct accumulation - should assert equality rather than reach for a smaller delta here.
 */
internal const val DELTA: Double = 1e-12
