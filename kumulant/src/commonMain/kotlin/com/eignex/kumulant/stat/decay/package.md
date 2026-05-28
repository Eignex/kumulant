# Package com.eignex.kumulant.stat.decay

Time-weighted moments. Each observation enters the accumulator with a
weight that shrinks toward zero with age, so older observations
contribute less. Two parallel sub-families: timestamp-based decay (the
`Decaying*` stats) and step-based EWMA (the `Ewma*` stats).

## The two flavours

### Timestamp-based: Decaying*

[DecayingSumStat], [DecayingMeanStat], [DecayingVarianceStat] are the
time-weighted counterparts of [com.eignex.kumulant.stat.summary.SumStat]
/ [com.eignex.kumulant.stat.summary.MeanStat] /
[com.eignex.kumulant.stat.summary.VarianceStat]. The decay schedule is
configured via [DecayWeighting]:

- `HalfLife(durationMillis)`; exponential decay specified by the
  half-life of an observation's weight.
- `TimeConstant(tau)`; exponential decay specified by the time
  constant (e-fold) of the weighting function.
- `Custom(weighting)`; caller-supplied decay function from `(elapsedNanos)`
  to a multiplicative weight.

Because weight is a function of the timestamp delta, these handle
irregular update intervals correctly. A stream that fires once a second
and then once an hour applies decay over the actual elapsed time, not
over the update count.

### Step-based: Ewma*

[EwmaMeanStat] and [EwmaVarianceStat] are the classical
exponentially-weighted moving variants with a step-based decay:
`alpha * new + (1 - alpha) * old`. Smoothing factor lives in
[DecayWeighting.Alpha].

These are cheaper and more familiar but assume roughly fixed intervals
between observations. Mixed-interval streams should reach for the
timestamp-based variants instead.

## Picking by need

- **Smooth tracking of a recent signal, regular updates** → [EwmaMeanStat]
  / [EwmaVarianceStat].
- **Smooth tracking of a recent signal, irregular timestamps** →
  [DecayingMeanStat] / [DecayingVarianceStat] with a `HalfLife` or
  `TimeConstant`.
- **Volatility tracking with a long-run baseline**; see
  [com.eignex.kumulant.stat.forecast.RecursiveVarianceStat], which adds
  a GARCH-style baseline term to [EwmaVarianceStat]'s shape.

## Result shapes

[DecayingMeanStat] / [EwmaMeanStat] expose
[WeightedMeanResult][com.eignex.kumulant.stat.summary.WeightedMeanResult]
; same shape as the non-decayed [MeanStat][com.eignex.kumulant.stat.summary.MeanStat].
[DecayingVarianceStat] / [EwmaVarianceStat] expose
[WeightedVarianceResult][com.eignex.kumulant.stat.summary.WeightedVarianceResult].
This is intentional: downstream traits like
[com.eignex.kumulant.core.HasCenterScale] and
[com.eignex.kumulant.core.HasSampleVariance] work identically on
decayed and undecayed results, so a band / standardize / scoring step
doesn't care which provenance the moment came from.

## Merge

Decayed stats with the same schedule merge cleanly: the weight
formulation makes the Chan-style parallel merge work the same way it
does for the undecayed Welford family. Stats with different schedules
(different half-lives, different time constants) cannot be merged
meaningfully; the merge contract enforces matching configuration.

## Concurrency

All variants are Welford-coupled (mean + variance + total weight cells
that have to stay consistent). Locked under
[com.eignex.kumulant.core.Concurrency.Strict] /
[com.eignex.kumulant.core.Concurrency.HighWrite]. Under
[com.eignex.kumulant.core.Concurrency.Relaxed] the cells race; the
drift is the same small ULP-level magnitude as the undecayed Welford
family.
