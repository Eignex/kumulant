package com.eignex.kumulant

/**
 * The comparison tolerance for floating-point assertions across the test suite.
 *
 * One value, so that a file which genuinely needs slack has to say so with a local override and a reason,
 * making a loosened tolerance visible as a decision instead of indistinguishable from the default.
 *
 * `1e-12` rather than something tighter because these are `Double` comparisons after accumulation over a
 * stream, not single operations. Roughly three decimal digits of slack against the `2.2e-16` epsilon
 * absorbs the reassociation a Welford or Chan recurrence performs without absorbing a real error.
 *
 * A test asserting something that should be *bit*-exact - a downdate restoring a prior state, a merge
 * matching a direct accumulation - should assert equality rather than reach for a smaller delta here.
 */
internal const val DELTA: Double = 1e-12
