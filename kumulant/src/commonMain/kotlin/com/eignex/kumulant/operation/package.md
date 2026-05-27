# Package com.eignex.kumulant.operation

Internal implementation details of the operation wrappers. The
**user-facing operation surface lives in [com.eignex.kumulant.schema]**
as extension functions on [StatSpec][com.eignex.kumulant.schema.StatSpec]
that return wire-portable spec wrappers — windowing, filtering,
weighting, sampling, transforms, folds, scalers, hysteresis, bands,
resampling, vectorisation, feedback. Pick the spec that fits and
`materialize(concurrency)` when you need a live stat.

What's public here:

- [BandResult] — result type for the `BandSeries` spec; emitted by the
  band wrapper around any [HasCenterScale][com.eignex.kumulant.core.HasCenterScale]
  series stat.
- [ResampleAggregator] — enum config for the `ResampleByTimeSeries`
  spec (Mean / Sum / Last / Min / Max per bucket).

Everything else (`filter`, `transform`, `windowed`, `band`, `weightBy`,
`vectorized`, `lag`, `diff`, `derivative`, `hysteresis`, `band`, the
`ListStats` family, etc.) is `internal`: still constructed by the
spec materializer, but not part of the published Kotlin API. A user
who needs a Kotlin closure that can't be expressed as a
[ScalarExpr][com.eignex.kumulant.schema.ScalarExpr] /
[BoolExpr][com.eignex.kumulant.schema.BoolExpr] can wrap their own
`Stat<R>` decorator; the library does not ship one.
