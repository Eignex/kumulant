# Package com.eignex.kumulant.stat.cardinality

Approximate distinct-count estimators. Both entries take a stream of
opaque `Long` keys ([com.eignex.kumulant.core.DiscreteStat]) and answer
"how many distinct keys have I seen?" in bounded memory. The right
pre-step is to hash domain-specific keys through
[com.eignex.kumulant.math.hash64] / [com.eignex.kumulant.math.Hasher64]
so the input has uniform 64-bit entropy; `value.hashCode().toLong()`
only provides 32 bits and skews the estimators on low-cardinality
domains.

## Picking an estimator

| Stat | Memory | Accuracy regime | Reach for it when |
|------|--------|-----------------|-------------------|
| [HyperLogLogStat] | O(2^precision) bytes (precision 14 ≈ 16 KB, ~1% relative error) | All cardinalities | The default. Merge across replicas is exact via cell-wise max, which makes HLL the natural choice when many workers each track a slice of the same stream. |
| [LinearCountingStat] | O(bits / 8) bytes | Tight when true cardinality is much smaller than the bitmap; degrades sharply once the bitmap saturates | Cardinality is bounded and known to stay well below the bitmap size. |

In practice [HyperLogLogStat] at precision 12-14 covers most uses
cheaply; [LinearCountingStat] earns its place only when the true
cardinality is comfortably below the bitmap size and you want the
tightest possible estimate at very low counts.

## Merge

Both estimators merge exactly:

- [HyperLogLogStat] takes the cell-wise max of two register vectors of
  the same precision. Two replicas observing overlapping streams stitch
  together correctly without double-counting.
- [LinearCountingStat] takes the bitwise OR of two bitsets of the same
  size. Same property.

That makes both families a clean fit for distributed cardinality
estimation: workers compute slices in parallel, ship snapshots, the
coordinator merges into a single estimator.

## Concurrency

Both stats decompose updates into independent striped cell operations:
HLL's register update is a CAS-max on one register; LinearCounting's
update is an atomic OR on one bit. Lock-free and exact under every
[com.eignex.kumulant.core.Concurrency] level.
