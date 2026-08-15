# Package com.eignex.kumulant.math

Distribution sampling, stream-hash functions, and the few numeric
routines that sit outside koblas, consumed by the rest of the library.
The vector / matrix primitives and BLAS-style linear algebra live in the
separate [koblas](https://github.com/Eignex/koblas) library
(`com.eignex.koblas`), which covers a defined BLAS/LAPACK subset; what
falls outside that subset and only kumulant needs lives here.

## Linear-algebra helpers

- `choleskyDowndateInPlace`: rank-1 downdate of a lower-triangular
  Cholesky factor, so
  [BayesianRegressionStat][com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat]
  can track the posterior covariance factor across observations instead
  of refactorizing per update.
- `zeroUpperTriangle`: clears the strict upper triangle of a factor,
  which koblas leaves unspecified, before it is stored in a snapshot.

## Distribution sampling

Convenience extensions on `kotlin.random.Random`:

- `nextNormal(mean, std)`: Gaussian. Used by Thompson-sampling
  posteriors throughout the bandit and regression layers.
- `nextLogNormal(mean, variance)`: log-normal. Used by composite arms
  modelling multiplicative reward.
- `nextGamma(alpha)`: gamma. Building block for Beta / Dirichlet.
- `nextBeta(alpha, beta)`: beta. Used by Beta-Bernoulli posteriors.
- `nextPoissonOne()`: Poisson(1). Used by Oza & Russell online
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

Note that these are *non-cryptographic*; passes BigCrush but is not
collision-resistant. Use a cryptographic hash function for adversarial
input.
