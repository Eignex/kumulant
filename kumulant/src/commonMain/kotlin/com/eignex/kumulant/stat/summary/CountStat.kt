package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.operation.withValue
import com.eignex.kumulant.operation.withWeight

/**
 * Observation count: each update contributes 1 regardless of supplied value and weight.
 *
 * **Use cases:** event rate denominators, sample sizes, "how many".
 *
 * **Memory:** O(1).
 *
 * **Update:** O(1).
 *
 * **Concurrency:** Inherits [SumStat]'s concurrency model.
 */
class CountStat(concurrency: Concurrency = Concurrency.None) :
    SeriesStat<SumResult> by SumStat(concurrency).withWeight(1.0).withValue(1.0)
