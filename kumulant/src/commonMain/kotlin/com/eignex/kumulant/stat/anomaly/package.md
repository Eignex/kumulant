# Package com.eignex.kumulant.stat.anomaly

Stateless anomaly scores derived from summary and quantile snapshots. Accumulate
observations with the underlying stat, then score a query against its result.
Both scores increase toward anomalous values; neither helper changes the snapshot.

## Picking a score

| Helper | Snapshot | Reach for it when ... |
|--------|----------|-----------------------|
| [absoluteZScore] | [com.eignex.kumulant.core.HasCenterScale] | Distance from the center in scale units is meaningful. [com.eignex.kumulant.stat.summary.VarianceStat] supplies the mean and standard deviation. |
| [exceedanceScore] | [com.eignex.kumulant.stat.quantile.QuantileResult], [com.eignex.kumulant.stat.quantile.SketchResult], or [com.eignex.kumulant.stat.quantile.TDigestResult] | A binary upper-tail flag is enough, including skewed or heavy-tailed streams. |

## Compose patterns

```kotlin
val summary = VarianceStat()
summary.update(value)
val distance = summary.read().absoluteZScore(query)

val sketch = DDSketchStat(probabilities = doubleArrayOf(0.99))
sketch.update(value)
val inTail = sketch.read().exceedanceScore(query, probability = 0.99)

val frugal = FrugalQuantileStat(q = 0.99)
frugal.update(value)
val lightweightTail = frugal.read().exceedanceScore(query)
```

[absoluteZScore] returns zero when the scale is at most `1e-12` or NaN.
[exceedanceScore] returns one strictly above the threshold and zero otherwise,
including a NaN threshold from an empty sketch. For sketches and digests, the
requested probability must be configured on the accumulator; the helper selects
its precomputed quantile without interpolation.

Read before updating to score against earlier observations, or after updating
to include the observation being scored.

## Merge

Merge the complete underlying summary, sketch, or digest snapshots, then score
the merged result. Scoring does not discard bins, centroids, or observation weights.
[com.eignex.kumulant.stat.quantile.FrugalQuantileStat] retains its approximate,
order-dependent merge semantics.

## Concurrency

The helpers hold no mutable state. Accumulation and snapshot consistency follow
the underlying stat's concurrency model; see its KDoc for details.
