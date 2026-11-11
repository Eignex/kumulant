package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.Mad
import com.eignex.kumulant.stat.StatDefaults
import com.eignex.kumulant.stat.quantile.TDigestStat

/**
 * Streaming median and unscaled median absolute deviation from one raw-value [TDigestStat].
 *
 * Updates accumulate observations without computing deviations. Reading computes the
 * weighted median of centroid locations, then the weighted median of their absolute
 * distances from that center. The sorted locations permit a linear outward scan.
 * Compression can lose within-centroid spread; this estimates the historical MAD,
 * without retaining deviations measured against earlier centers.
 *
 * Merge combines the complete digest retained by [MadResult.digest], then reads
 * recalculate both estimates. A summary-only result contributes unit total weight
 * split between `median - mad` and `median + mad`, approximating its missing evidence.
 *
 * **Use cases:** robust center and scale for heavy-tailed streams. Use [FrugalMadStat]
 * for constant-memory adaptive tracking with an equal-weight approximate merge.
 *
 * **Memory:** O([compression]); one digest and its update buffer.
 *
 * **Update:** Inherits [TDigestStat]'s buffered update cost; no snapshot per update.
 * Reading adds an O(k) scan with O(1) working memory for k centroids.
 *
 * **Concurrency:** Inherits [TDigestStat]'s concurrency model. Both estimates are
 * calculated from the same detached snapshot.
 */
@Suppress("FunctionNaming")
fun MadStat(
    compression: Double = StatDefaults.TDIGEST_COMPRESSION,
    concurrency: Concurrency = Concurrency.None,
): SeriesStat<MadResult> = Mad(compression).materialize(concurrency)
