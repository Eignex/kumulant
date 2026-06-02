package com.eignex.kumulant.schema.spec

import kotlinx.serialization.Serializable

/**
 * Per-bucket reduction used by the `ResampleByTimeSeries` spec when aligning an
 * input series onto fixed wall-clock buckets. Configured on a spec; the
 * materializer threads it through to the runtime bucket aggregator.
 */
@Serializable
enum class ResampleAggregator {
    /** Forward the arithmetic mean of in-bucket values (unweighted). */
    Mean,

    /** Forward the sum of in-bucket values. */
    Sum,

    /** Forward the most recent in-bucket value. */
    Last,

    /** Forward the minimum in-bucket value. */
    Min,

    /** Forward the maximum in-bucket value. */
    Max,
}
