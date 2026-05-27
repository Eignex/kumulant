# Package com.eignex.kumulant.math

Vector / matrix primitives, distribution sampling, and stream-hash
functions consumed by the rest of the library. The public surface
splits into three groups; the rest of the package is `internal` SIMD,
Cholesky, and BLAS-style helpers used by regression and Bayesian stats.

## Vectors and matrices

| Type | Role |
|------|------|
| [VectorView] | Sealed read interface — `size`, `operator get(i)`, `forEachStored`. Accepted by [VectorStat][com.eignex.kumulant.core.VectorStat] / [RegressionStat][com.eignex.kumulant.core.RegressionStat] update, by every result's `predict(x: VectorView)` method, and by every spec that consumes a feature vector. |
| [DenseVector] | Backed by a flat `DoubleArray`. Constructed via `DenseVector.of(doubleArrayOf(...))`. The default carrier when the caller has a dense array on hand. |
| [SparseVector] | Backed by parallel index/value arrays. Constructed via `SparseVector.of(indices, values, size)`. Forwarded by every regressor's sparse-aware update path; `forEachStored` walks only the nonzero entries. |
| [MatrixView] | Sealed read interface — `rows`, `cols`, `operator get(i, j)`. Carried by covariance / Cholesky results. |
| [DenseMatrix] | Row-major flat `DoubleArray` backing. Carried by [com.eignex.kumulant.stat.regression.glm.CovarianceRegressionResult] for posterior covariance and Cholesky factors. |

Both vector types accept both dense and sparse on the same API path —
`forEachStored { i, v -> ... }` is the universal entry point that lets a
consumer iterate only the populated entries, regardless of the backing.
That's what gives [com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat]
its O(nnz(x)) update cost.

## Distribution sampling

Convenience extensions on `kotlin.random.Random`:

- `nextNormal(mean, std)` — Gaussian. Used by Thompson-sampling
  posteriors throughout the bandit and regression layers.
- `nextLogNormal(mean, variance)` — log-normal. Used by composite arms
  modelling multiplicative reward.
- `nextGamma(alpha)` — gamma. Building block for Beta / Dirichlet.
- `nextBeta(alpha, beta)` — beta. Used by Beta-Bernoulli posteriors.
- `nextPoissonOne()` — Poisson(1). Used by Oza & Russell online
  bagging in [RandomForestRegressionStat][com.eignex.kumulant.stat.regression.tree.RandomForestRegressionStat].

These are mostly internal to the library but exposed in case downstream
code wants the same well-tested implementations.

## Hash functions

The streaming sketches and cardinality estimators
([com.eignex.kumulant.stat.cardinality], [com.eignex.kumulant.stat.sketch])
need their input to carry uniform 64-bit entropy. The JVM's
`Object.hashCode()` only provides 32 bits and tends to be biased for
low-cardinality domains, so the hash pre-step is the right way to feed
opaque keys into those sketches.

| Function / type | Role |
|-----------------|------|
| [hash64] | Default 64-bit hash of a `ByteArray` (or `String` via UTF-8). Currently delegates to [SplitMixChunkHasher]. The unqualified entry; downstream code that doesn't care which algorithm should use this. |
| [Hasher64] | Pluggable 64-bit byte-hash interface. Implementations must be deterministic and pure. Implement a custom one to pin a specific hash variant. |
| [SplitMixChunkHasher] | The current default implementation: SplitMix64 over 8-byte chunks. Pin to this directly when stability across library versions matters. |
| [splitmix64] | Bit-mixing 64-bit integer transform. Used internally and exposed for completeness. |

Note that these are *non-cryptographic* — passes BigCrush but is not
collision-resistant. Use a cryptographic hash function for adversarial
input.
