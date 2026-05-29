package com.eignex.kumulant.stat

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat

/**
 * A [Stat] over an unspecified [Result] type. The star-projected base shared by
 * every concrete accumulator in this package, useful when handling a
 * heterogeneous bag of stats whose result types differ; a registry, a fan-out
 * over a mixed schema, or generic plumbing that only calls
 * [read][Stat.read] / [merge][Stat.merge] / [reset][Stat.reset].
 *
 * For a stat with a known result type, refer to [Stat] (or a modality subtype
 * such as [com.eignex.kumulant.core.SeriesStat]) with its concrete `R`.
 */
typealias AnyStat = Stat<*>
