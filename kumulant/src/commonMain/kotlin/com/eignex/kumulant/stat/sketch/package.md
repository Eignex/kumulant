# Package com.eignex.kumulant.stat.sketch

Structural queries on a stream that aren't shaped like a cardinality or
a quantile. Four members; each answers a different question.

## What's in the family

| Stat | Result | Question |
|------|--------|----------|
| [BloomFilterStat] | [BloomFilterResult] | "Have I seen this key before?" Membership only, one-sided false positives, no per-key counts. |
| [CountMinSketchStat] | [CountMinSketchResult] | "How many times has this key appeared?" Approximate per-key counts; biased upward by collisions, exact for heavy items. |
| [SpaceSavingStat] | [HeavyHittersResult] | "What are the top K heaviest keys?" Long-tail problem: out of millions of distinct items, a handful might dominate volume. |
| [MinHashStat] | [MinHashResult] | "How similar are two sets?" Jaccard similarity sketch; estimates overlap without comparing the sets directly. |

All four take a stream of opaque `Long` keys through
[com.eignex.kumulant.core.DiscreteStat]. As with [com.eignex.kumulant.stat.cardinality],
hash domain-specific keys through [com.eignex.kumulant.math.hash64]
first so the input carries uniform 64-bit entropy.

## When to pick what

- **BloomFilter** when you need yes/no membership at the absolute
  cheapest memory per key. A "yes" may be a false positive (rate
  controlled by configured bits + hashes); a "no" is always exact.
  Standard tool for deduplication of seen-already keys.
- **CountMinSketch** when you need per-key counts but an exact map
  would blow memory. Counts are biased upward by collisions but exact
  for heavy items, which is fine for top-K-like questions and rate
  monitoring.
- **SpaceSaving** when you only need the K heaviest keys; viral URLs,
  hot database rows, top users. Cheaper than a full map: keeps K
  counters and guarantees that any key whose true frequency exceeds
  the K-th largest is in the retained set.
- **MinHash** for near-duplicate detection over streams. Standard
  Jaccard-similarity sketch; two sets' MinHash signatures collide on
  hash `j` with probability equal to their Jaccard index.

## Merge

- [BloomFilterStat] merges via bitwise OR. Exact for same-sized
  bitsets.
- [CountMinSketchStat] merges via cell-wise sum. Same-shape sketches
  required.
- [SpaceSavingStat] merges by re-priority over the union of counters.
- [MinHashStat] merges via cell-wise min over signature arrays. Exact
  Jaccard reproduction.

All four families work cleanly in distributed pipelines: workers track
slices, ship snapshots, the coordinator merges.

## Concurrency

Each family decomposes updates into independent atomic operations on
single cells (BloomFilter / CountMin / MinHash) or a small bounded set
of cells (SpaceSaving). Lock-free and exact under every
[com.eignex.kumulant.core.Concurrency] level.
