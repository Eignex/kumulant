# Package com.eignex.kumulant.stat.quantile

Bounded-memory quantile estimators and histograms. Every entry trades a
different precision-versus-cost knob: relative error guarantees,
fixed-precision over a known range, reservoir sampling for raw values
back, or constant memory at the cost of accuracy.

## Picking a quantile estimator

| Stat | Memory | Precision | Reach for it when |
|------|--------|-----------|-------------------|
| [DDSketchStat] | O(1 / relativeError) | Relative error guarantee | Latencies, payload sizes, any value spanning orders of magnitude. Merge across replicas is exact. The default percentile sketch. |
| [TDigestStat] | O(compression) | Tighter tail-quantile error than DDSketch at the same memory budget | You specifically care about the 99th / 99.9th percentile (tails) and not the body. |
| [HdrHistogramStat] | O(precision · log(range)) | Strictest precision in a bounded range | The value range is known up front (e.g. latencies between 1 µs and 1 hr) and you want guaranteed precision in that range. |
| [LinearHistogramStat] | O(binCount) | Equal-width bin precision | Meaningful breakpoints are known up front; you want bins that match them directly with no rebucketing on read. |
| [ReservoirHistogramStat] | O(capacity) | Raw values back | Downstream needs the actual observations (to feed another stat or compute quantities the sketches don't expose). |
| [FrugalQuantileStat] | O(1); two variables | Coarse, single-quantile | You can fit only a few bytes per stat and only care about one percentile. |
| [ThresholdBucketStat] | O(thresholds) | Caller-supplied edges | You know the meaningful value buckets ahead of time and want per-bucket counts, not a quantile estimate. |

## Result shapes

| Result | Shape |
|--------|-------|
| [SketchResult] | DDSketch snapshot: log-spaced bin map + precomputed quantiles at the configured probabilities |
| [QuantileResult] | FrugalQuantileStat single-quantile scalar |
| [TDigestResult] | t-digest centroids + precomputed quantiles |
| [SparseHistogramResult] | Parallel `[lowerBounds, upperBounds)` arrays with weights; produced by [HdrHistogramStat], [LinearHistogramStat], and [SketchResult.toSparseHistogram] |
| [ReservoirResult] | Bounded reservoir sample of raw values + the sampling weight |
| [ThresholdBucketResult] | Per-bucket weighted counts over caller-supplied edges |

[SketchResult] / [TDigestResult] / [ReservoirResult] all expose
quantiles at the configured probabilities, so the result type a
downstream consumer sees depends on which sketch was picked. For a
uniform downstream interface, project to [SparseHistogramResult] (the
shared histogram shape).

## Merge story

- **DDSketch, HDR, t-digest** merge exactly across replicas via
  cell-wise bin addition / centroid combination.
- **LinearHistogram, ThresholdBucket** merge exactly via cell-wise bin
  addition (same bin layout required).
- **ReservoirHistogram** merges sample-weighted via reservoir union; the
  result is statistically equivalent to one large reservoir.
- **FrugalQuantile** does not have a clean merge; it averages the two
  point estimates. Use it for single-stream tracking, not distributed
  aggregation.

## Concurrency

Histogram-shaped stats ([DDSketchStat], [HdrHistogramStat],
[LinearHistogramStat], [ThresholdBucketStat]) decompose updates into a
single striped atomic increment on the destination bin; exact under
every [com.eignex.kumulant.core.Concurrency] level. [ReservoirHistogramStat]
and [FrugalQuantileStat] keep coupled state and self-serialise under
concurrent access. [TDigestStat] self-serialises through its own lock.

## PIT-style equiprobable histogram

The `pitHistogram(numBins)` factory in
[com.eignex.kumulant.stat.score] is built from this family: a stream of
PIT values (which are uniform under correct distributional forecasts)
fed into an equiprobable [LinearHistogramStat] over `[0, 1]` exposes
the deviation from uniformity that the corresponding PIT test consumes.
