package com.eignex.kumulant.schema.spec

import kotlinx.serialization.Serializable

/**
 * Per-bucket reduction used by the `ResampleByTimeSeries` spec when aligning an
 * input series onto fixed wall-clock buckets. Configured on a spec; the
 * materializer threads it through to the runtime bucket aggregator.
 */
@Serializable
enum class ResampleAggregator {
    /** Forward the weighted mean of in-bucket values, `Sum(value * weight) / Sum(weight)`. */
    Mean,

    /** Forward the weighted sum of in-bucket values, `Sum(value * weight)`. */
    Sum,

    /**
     * Forward the most recent in-bucket value.
     *
     * Selects a value rather than accumulating one, so it reads no weight at all - see [Min] for what
     * that means for a negative one.
     */
    Last,

    /**
     * Forward the minimum in-bucket value.
     *
     * Selects rather than accumulates, so weight does not enter: a minimum has no multiplicity to
     * scale. The consequence is worth stating, because it is the one place resampling does not do what
     * a negative weight asks. A downdate small enough to leave the bucket's weight positive is accepted
     * and reduces that weight, but the value it meant to retract still competes for the extremum, since
     * there is no arithmetic that removes a value from a minimum. Reach for [Mean] or [Sum] where
     * retraction has to be exact.
     */
    Min,

    /** Forward the maximum in-bucket value. Weight-agnostic on the same terms as [Min]. */
    Max,
}
