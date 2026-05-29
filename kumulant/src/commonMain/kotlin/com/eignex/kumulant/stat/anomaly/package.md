# Package com.eignex.kumulant.stat.anomaly

Online anomaly detectors. All three primitives produce a `score(x)`
method on their result so the same downstream pipeline can consume
"how anomalous is this observation?" regardless of which detector
generated it.

## Picking a detector

| Stat | Input shape | When to reach for it |
|------|-------------|----------------------|
| [GaussianScorerStat] | Scalar | Streams that are roughly bell-shaped. The z-score `|x - mean| / stdDev` answers "how far from normal." Cheap, O(1) memory. |
| [QuantileFilterStat] | Scalar | Non-Gaussian, possibly skewed or heavy-tailed streams. Threshold is the running q-quantile of the input; `score(x) = 1.0` flags anything in the tail. Adapts as the distribution drifts. |
| [HalfSpaceTreesStat] | Vector | Multivariate signals where correlations between features carry the anomaly signal. Ensemble of pre-built random half-space trees; low score → anomaly. Cheap per update, parallel across trees. |

## How they relate

[GaussianScorerStat] is the simplest possible parametric detector and
the natural baseline. It assumes the stream is well-summarised by mean
and variance; if it isn't, the score saturates uselessly. The
single-line implementation wrapping [com.eignex.kumulant.stat.summary.VarianceStat]
makes this clear.

[QuantileFilterStat] is the non-parametric upgrade: instead of
assuming a distribution, it tracks the actual q-quantile via
[com.eignex.kumulant.stat.quantile.DDSketchStat]. The threshold
adapts with the stream, so concept drift in the *body* of the
distribution shifts the anomaly bar automatically.

[HalfSpaceTreesStat] is the multivariate generalisation. Each tree
projects the input onto random axis-aligned half-spaces; leaves track
mass over a sliding *reference* window vs the *latest* window. An
input whose leaf has tiny reference-window mass falls into a region
the recent stream rarely visited; anomaly. The reference window
rotates every `windowSize` observations so the detector tracks slow
concept drift.

## Score semantics

The three detectors don't agree on which direction is "more
anomalous":

- [GaussianScorerStat]: higher score = more anomalous.
  Threshold against a fixed multiple of standard deviations.
- [QuantileFilterStat]: binary 0/1: `1.0` means "above the running
  quantile."
- [HalfSpaceTreesStat]: **lower** score = more anomalous. The
  reported number is leaf mass times depth, which is large for inputs
  in dense regions of the reference window. Invert it if you want
  "higher = more anomalous" semantics in a downstream pipeline.

This asymmetry tracks the literature; documenting it here so callers
don't unify the directions accidentally.

## Merge

[GaussianScorerStat] inherits Chan-style parallel merge from
[com.eignex.kumulant.stat.summary.VarianceStat]; exact across
parallel workers. [QuantileFilterStat] and [HalfSpaceTreesStat] do
**not** support merge directly: the quantile-filter result only
carries the scalar threshold (the bin layout would need to travel
too), and a half-space-trees result only merges when the tree
structures match (same `randomSeed`). For distributed anomaly
detection, ship the underlying [com.eignex.kumulant.stat.quantile.DDSketchStat]
/ [HalfSpaceTreesStat] snapshots and merge those.

## Concurrency

[GaussianScorerStat] inherits [com.eignex.kumulant.stat.summary.VarianceStat]'s
Welford-coupled cells: locked under [com.eignex.kumulant.core.Concurrency.Strict]
/ [com.eignex.kumulant.core.Concurrency.HighWrite], racing with bounded drift
under [com.eignex.kumulant.core.Concurrency.Relaxed] but never throwing.
[QuantileFilterStat] inherits [com.eignex.kumulant.stat.quantile.DDSketchStat]'s
striped histogram counters; lock-free and exact under every level.
[HalfSpaceTreesStat] applies independent atomic mass updates per leaf and
serialises only the periodic reference-window rotation under
[com.eignex.kumulant.core.Concurrency.Strict] /
[com.eignex.kumulant.core.Concurrency.HighWrite].
