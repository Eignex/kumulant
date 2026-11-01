# Package com.eignex.kumulant.math

Distribution sampling, stream-hash functions, and Cholesky factorization used by
statistical models. Vector and matrix storage, arithmetic, and triangular solves
live in the separate [koblas](https://github.com/Eignex/koblas) BLAS library
(`com.eignex.koblas`).

## Cholesky factorization

[factorCholesky] factors the finite lower triangle of a symmetric positive-definite
matrix into an owned [CholeskyFactor]. The factor solves vector and matrix systems,
provides a log determinant, and returns an independent lower-triangular copy.
Allocating operations preserve inputs. [CholeskyFactor.solveInto] and
[CholeskyFactor.solveLowerInto] write to supplied destinations, which may alias
the right-hand side. Separate calls can reuse a workspace serially; concurrent
calls need independent workspaces and destinations. The factor remains unchanged.
[CholeskyFactor.rankUpdated] adds an outer product and
[CholeskyFactor.extended] appends a block, each returning an owned factor.
Factorization adds no jitter or regularization;
[NotPositiveDefinite] identifies a failed pivot for callers choosing a retry policy.

## Distribution sampling

Convenience extensions on `kotlin.random.Random`:

- `nextNormal(mean, std)`: Gaussian. Used by Thompson-sampling
  posteriors throughout the bandit and regression layers.
- `nextGamma(alpha)`: gamma. Building block for Beta / Dirichlet.
- `nextBeta(alpha, beta)`: beta. Used by Beta-Bernoulli posteriors.
- `nextPoissonOne()`: Poisson(1). Used by Oza & Russell online
  bagging in [RandomForestRegressionStat][com.eignex.kumulant.stat.regression.tree.RandomForestRegressionStat].

These are mostly internal to the library but exposed in case downstream
code wants the same well-tested implementations.

## Quasi-Monte Carlo integration

[SobolSequence] generates uniform or standard-normal integration points. A seed applies
left linear matrix scrambling and a digital shift; null selects the unscrambled sequence.
Use powers-of-two sample blocks from the beginning and independent seeds for error estimates.
These points are dependent integration samples, not independent observations for a statistical model.

Coordinate and sample prefixes remain fixed when dimensions grow. `dimensionOffset` selects a
contiguous projection, so a consumer can construct additional normal coordinates while retaining
an existing common-sample cache. The engine supports 1,024 global coordinates and 2^52 points;
normal samples use 52-bit cell midpoints and an inverse CDF to keep endpoints finite.

```kotlin
val sequence = SobolSequence(dimensions = 4, scrambleSeed = 17L)
val point = DoubleArray(4)
repeat(1024) {
    sequence.nextNormal(point)
    // Evaluate an integrand at point before reusing this storage.
}
```

Direction numbers are the first 1,024 coordinates of the
[Joe-Kuo D(6) table](https://web.maths.unsw.edu.au/~fkuo/sobol/).
Normal quantiles use Wichura's AS 241 coefficients from
[John Burkardt's ASA241 distribution](https://people.sc.fsu.edu/~jburkardt/f_src/asa241/asa241.html).
Their BSD and MIT notices are included under `META-INF/LICENSES` in the JVM artifact and source distribution.

## Hash functions

The streaming sketches and cardinality estimators
([com.eignex.kumulant.stat.cardinality], [com.eignex.kumulant.stat.sketch])
need their input to carry uniform 64-bit entropy. The JVM's
`Object.hashCode()` only provides 32 bits and tends to be biased for
low-cardinality domains, so the hash pre-step is the right way to feed
opaque keys into those sketches.

| Function / type | Role |
|-----------------|------|
| [hash64] | 64-bit hash of a `ByteArray` (or `String` via UTF-8): SplitMix64 over 8-byte chunks, stable across platforms. |
| [splitmix64] | Bit-mixing 64-bit integer transform. Used internally and exposed for completeness. |

Note that these are *non-cryptographic*; passes BigCrush but is not
collision-resistant. Use a cryptographic hash function for adversarial
input.
