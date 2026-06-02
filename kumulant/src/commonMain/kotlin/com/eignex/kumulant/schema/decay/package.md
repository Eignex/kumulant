# Package com.eignex.kumulant.schema.decay

The wire-portable weighting strategies for the time-decay and smoothing
stats. A [DecayWeightingSpec] is the serializable counterpart of the
runtime weighting in [com.eignex.kumulant.stat.decay]: it says how fast
old observations should fade, and inflates to the live form when the stat
is materialized.

There are two strategies. [HalfLife] decays by wall-clock time, halving an
observation's weight over a configured duration, and suits signals where
recency is measured in elapsed time. [Alpha] decays per observation, with
each new sample carrying a fixed smoothing factor against the running
estimate, and suits signals where recency is measured in sample count.

The strategies are split by type rather than folded into one discriminated
union so each decay-stat spec can statically require the right one. Durations
travel as millisecond longs rather than `kotlin.time.Duration` to keep the
wire compact and avoid the experimental duration serializer. The decay and
smoothing stat specs themselves stay in [com.eignex.kumulant.schema]
alongside the other specs; only the weighting configuration lives here.
