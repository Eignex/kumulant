# Kumulant style guide

## Stat KDoc template

Every stat class (anything implementing `SeriesStat`, `PairedStat`, `DiscreteStat`,
or `RegressionStat`) should have a KDoc that follows this template.

```kotlin
/**
 * <One-sentence summary: what this stat computes and what its snapshot returns.>
 *
 * <Algorithm paragraph when non-trivial; name the recurrence (Welford, Chan's
 * parallel, Hoeffding bound, Sherman-Morrison, ...), cite the paper if relevant,
 * call out numerical-stability caveats.>
 *
 * <Configuration paragraph when the constructor has non-obvious knobs; what
 * each tunable does, defaults, accuracy/memory tradeoffs.>
 *
 * **Use cases:** <when to reach for this stat over its peers; common applications.>
 *
 * **Memory:** <O(...) in constructor params, or "O(1)" when fixed-size.>
 *
 * **Update:** <O(...) per update, plus any rare amortized work.>
 *
 * **Concurrency:** <mechanism, per-Concurrency behaviour, caveats.>
 */
class FooStat(...) : SeriesStat<FooResult> { ... }
```

### Required and optional sections

Always present, in this order:

1. **Summary**; one sentence, no header. What it computes, what the snapshot
   returns.
2. **`**Use cases:**`**; one to three sentences. Where this stat fits, what
   its peers are.
3. **`**Memory:**`**; O-bound in constructor parameters. Examples:
   `O(1)`, `O(precision)`, `O(depth · width)`, `O(featureSize^2)`.
4. **`**Update:**`**; O-bound per `update()` call. Examples:
   `O(1)`, `O(featureSize)`, `O(featureSize^2)` (Sherman-Morrison),
   `O(log n)` (bin lookup), `O(numHashes)`.
5. **`**Concurrency:**`**; mechanism first, then per-`Concurrency` behaviour,
   then caveats. See *Concurrency clauses* below.

Optional, between the summary and the bold sections:

- **Algorithm paragraph**: the recurrence, the paper, the math. Skip when the
  stat is a thin wrapper or its formula is in the summary.
- **Configuration paragraph**: non-obvious constructor knobs and their
  tradeoffs. Skip when the stat has only the standard `concurrency` parameter.

### Section style

- Use `**Label:**` inline bold, **not** `#` markdown headings. Heading-style
  blocks render too heavy inside per-class KDocs.
- Each `**Section:**` body should be one to three sentences. If you need more,
  the algorithm/configuration paragraph above is the place.
- Declarative, not narrative: *"Locked under any concurrent level"* beats
  *"We lock under any concurrent level because…"*.
- Result data classes keep their existing per-field `/** ... */` docs
  unchanged; the template applies only to the stat class itself.

### Concurrency clauses

State the update mechanism in 3–6 words, then the per-`Concurrency` behaviour.
Reuse these standard phrasings so the matrix is grep-able:

| Mechanism                                       | Per-level behaviour                                                                                                                |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| *Single atomic add per update*                  | Exact under every level. `HighWrite` switches the cell to a striped adder.                                                          |
| *Single-cell CAS-min/max loop*                  | Exact under every level. The CAS retry naturally serialises racing writers.                                                         |
| *Independent striped cells with deterministic bucket assignment* | Exact under every level. Increments commute and racing writers on the same value share the cell.                |
| *Welford-coupled cells (no lock)*               | `None`/`Strict`/`HighWrite` exact; `Relaxed` drifts ~1e-5 relative on coupled state but never throws.                               |
| *Body locked under any concurrent level*        | Exact under every level. Throughput bound by lock contention; shard and merge for higher write rates.                              |
| *Lock-free order-dependent recurrence*          | Even `Strict` does not reproduce a serial reference because the lock serialises arrival, not order. Drift ~X% under contention.     |

Cite measured drift numbers from `:kumulant-bench:analyzeConcurrencyDrift` when
known; drop them if not.

When a stat is a thin wrapper over another, each section can collapse to one
line: `**Concurrency:** Inherits [SumStat]'s concurrency model.`

### Worked example

```kotlin
/**
 * Weighted mean and variance via Welford with Chan-style parallel merge.
 *
 * Population variance `sst / totalWeights`; use [HasSampleVariance.sampleVariance]
 * on the result for the unbiased estimator.
 *
 * **Use cases:** dispersion of any scalar quantity; pairs with [MomentsStat]
 * when skewness/kurtosis are also needed.
 *
 * **Memory:** O(1); three doubles plus a lock.
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Welford-coupled cells. `Strict` and `HighWrite` lock the
 * body so each update is atomic; exact up to floating-point reorder ULPs.
 * `Relaxed` drops the lock and the three cells race independently; the
 * variance drifts ~1e-4 relative under contention but never throws.
 */
class VarianceStat(...) : SeriesStat<WeightedVarianceResult>
```
