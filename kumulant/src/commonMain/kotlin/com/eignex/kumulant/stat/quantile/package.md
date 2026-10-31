# Package com.eignex.kumulant.stat.quantile

Bounded-memory quantile estimators and histograms. Every entry trades a
different precision-versus-cost knob: relative error guarantees,
fixed-precision over a known range, reservoir sampling for raw values
back, or constant memory at the cost of accuracy.

## Picking a quantile estimator

| Stat | Memory | Precision | Reach for it when |
|------|--------|-----------|-------------------|
| [DDSketchStat] | O(log(maxIndexableValue / minIndexableValue) / relativeError) | Relative value error within the indexable range; out-of-range values fold into edge bins | Values spanning orders of magnitude with a configured range and error target. |
| [TDigestStat] | O(compression) | Adaptive resolution, finer near the tails; no relative-error guarantee | You care about extreme percentiles without choosing a relative-error parameter. |
| [HdrHistogramStat] | O(precision · log(range)) | Strictest precision in a bounded range | The value range is known up front (e.g. latencies between 1 µs and 1 hr) and you want guaranteed precision in that range. |
| [BucketHistogramStat] | O(edges) | Caller-supplied edges | You know the meaningful value buckets ahead of time (SLO latency buckets, a probability range split evenly with [BucketHistogramStat.linear]) and want per-bucket counts. |
| [ReservoirHistogramStat] | O(capacity) | Raw values back | Downstream needs the actual observations (to feed another stat or compute quantities the sketches don't expose). |
| [FrugalQuantileStat] | O(1); one estimate | Coarse, single-quantile | You can fit only a few bytes per stat and only care about one percentile. |

## Result shapes

| Result | Shape |
|--------|-------|
| [SketchResult] | DDSketch snapshot: log-spaced bin map + precomputed quantiles at the configured probabilities |
| [QuantileResult] | FrugalQuantileStat single-quantile scalar |
| [TDigestResult] | t-digest centroids + precomputed quantiles |
| [SparseHistogramResult] | Parallel lower/upper bound arrays with weights; produced by [HdrHistogramStat], [BucketHistogramStat], and [SketchResult.toSparseHistogram] |
| [ReservoirResult] | Bounded reservoir sample of raw values + the sampling weight |

[SketchResult] and [TDigestResult] include quantiles at configured probabilities.
[ReservoirResult.quantile] computes a quantile from retained values on demand.
Histogram conversion helpers project these results to [SparseHistogramResult]
when downstream consumers need a shared histogram shape.

## PIT-style equiprobable histogram

The `pitHistogram(numBins)` factory in
[com.eignex.kumulant.stat.score] is built from this family: a stream of
PIT values (which are uniform under correct distributional forecasts)
fed into an equiprobable [BucketHistogramStat] over `[0, 1]` exposes
the deviation from uniformity that the corresponding PIT test consumes.

## Merge

- **DDSketch** adds matching logarithmic bins. Incoming indices outside the
  receiver's indexable range fold into its edge bins.
- **HDR** assigns each incoming bin's weight using its lower bound; this
  preserves matching layouts and approximates other non-negative layouts.
- **BucketHistogram** adds each incoming interval to the bucket holding its
  midpoint; matching edges merge exactly and other layouts are approximated.
- **t-digest** combines centroids and recompresses; the result remains approximate
  and depends on merge order.
- **ReservoirHistogram** retains the highest admission keys from both samples.
  Equivalence to a single reservoir requires independent admission streams and
  source samples large enough for the receiver's capacity.
- **FrugalQuantile** averages two point estimates. This is a coarse,
  non-associative approximation with no accuracy guarantee.

## Concurrency

Histogram-shaped stats ([DDSketchStat], [HdrHistogramStat],
[BucketHistogramStat]) use independent additive cells.
Concurrent modes preserve per-cell updates, with JVM striped adders under
[com.eignex.kumulant.core.Concurrency.HighWrite]; multi-bin reads are not atomic.
[ReservoirHistogramStat] locks updates, merges, and reads under Strict and
HighWrite. Relaxed admission uses CAS with a separate RNG lock and can expose
mismatched keys and values during a read. [FrugalQuantileStat] locks steps under
Strict and HighWrite and uses atomic updates under Relaxed; estimates depend
on observation order. [TDigestStat] locks its body under every concurrent mode.
[com.eignex.kumulant.core.Concurrency.None] provides no synchronisation.
