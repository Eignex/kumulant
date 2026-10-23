package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.stat.summary.WeightedVarianceResult

/**
 * Pool leaf or subtree summaries with Chan's parallel variance merge.
 *
 * Forest members are correlated views of the same observations. By default their
 * combined weight is divided by the number of contributors, retaining uncertainty
 * as the ensemble grows. Pass [independent] for disjoint evidence whose weights add.
 * Empty or non-positive-weight summaries contribute no moments but still count as
 * ensemble members. An empty iterable returns an empty summary.
 */
fun mergeForestEvidence(
    summaries: Iterable<WeightedVarianceResult>,
    independent: Boolean = false,
): WeightedVarianceResult {
    var combined = WeightedVarianceResult(0.0, 0.0, 0.0)
    var members = 0
    for (summary in summaries) {
        members++
        if (summary.totalWeights > 0.0) combined = mergeWVR(combined, summary)
    }
    return if (independent || members == 0) combined else combined.copy(totalWeights = combined.totalWeights / members)
}
