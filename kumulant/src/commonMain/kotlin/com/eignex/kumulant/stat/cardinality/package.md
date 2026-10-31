# Package com.eignex.kumulant.stat.cardinality

Approximate distinct-count estimation. [HyperLogLogStat] takes a stream of
opaque `Long` keys ([com.eignex.kumulant.core.DiscreteStat]) and answers
"how many distinct keys have I seen?" in bounded memory. The right
pre-step is to hash domain-specific keys through
[com.eignex.kumulant.math.hash64] / [com.eignex.kumulant.math.Hasher64]
so the input has uniform 64-bit entropy; `value.hashCode().toLong()`
only provides 32 bits and skews the estimator on low-cardinality
domains.

## Accuracy

Memory is O(2^precision) bytes; precision 14 is about 16 KB at ~1% relative
error. At small cardinalities the estimator switches to linear counting over
its registers, so low counts stay tight.

## Merge

[HyperLogLogStat] merges exactly by taking the cell-wise max of two register
vectors of the same precision. Two replicas observing overlapping streams
stitch together correctly without double-counting, which makes it a clean fit
for distributed cardinality estimation: workers compute slices in parallel,
ship snapshots, and the coordinator merges them into a single estimator.

## Concurrency

Each update is a CAS-max on one register. Lock-free and exact under every
[com.eignex.kumulant.core.Concurrency] level.
