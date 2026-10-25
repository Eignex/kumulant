# Preliminary Cholesky baseline, 2026-10-25

This records the allocating public API at base revision
`f8fb995659d6cb3bd2d5dee400e9927f288edd88` for the first benchmark increment of
[issue #139](https://github.com/Eignex/kumulant/issues/139).
The numerical implementation is unchanged. The measured harness is identified
by its JAR and source SHA-256 hashes in [environment.json](environment.json).

These are exploratory measurements on a busy shared workstation. CPU affinity
reduces migration on the hybrid CPU, but other workloads, frequency scaling,
and thermal effects remain uncontrolled. Rerun baseline and candidate on an idle
machine before accepting an optimization; these timings establish no speedup.
The full parameter envelope and Native performance baseline remain to be run.

## Environment and method

- Intel Core i9-12900H, Linux x86_64; affinity pinned to logical CPU 4 (a performance core).
- Temurin 25.0.1+8-LTS, Kotlin 2.4.10, kotlinx.benchmark 0.4.17, JMH 1.37.
- koblas root `0.1.1-20261013.182449-263`, JVM `0.1.1-20261013.182449-266`;
  Linux x64 validation uses `0.1.1-20261013.182449-262`.
- One benchmark thread, two JVM forks, 3 warmup and 5 measurement iterations per fork,
  1 second per iteration, average time, 512 MiB initial/maximum heap, GC profiler.
- Scalar ran first, SIMD second, sequentially. Backend names were verified by
  the accuracy reports. SIMD used the incubator Vector API with four double lanes.
- Seed 139; right-hand-side seed 140. Covariances use noise variance `1e-6` plus
  jitter `1e-10`. The [suite documentation](../../../README.md) defines all fixtures.
- One-minute system load averages before/after scalar: 22.29/8.40;
  before/after SIMD: 8.40/6.35.

Exact commands, source hashes, run timestamps, CPU/OS details, and dependency
versions are in [environment.json](environment.json). Raw JMH JSON retains
per-fork samples, confidence intervals, allocated bytes/op, allocation rate,
GC counts, and GC times: [scalar](scalar.json), [SIMD](simd.json).

## Timings and allocations

Times are mean microseconds/op ± JMH's 99.9% confidence interval half-width,
from ten measured iterations. Allocations are mean bytes/op from `gc.alloc.rate.norm`.
Public API ownership copies, factor panel/trailing-block copies, and backend
packing/workspace are included together. Fixture generation is excluded.

| Operation | Fixture / shape | Scalar us/op | SIMD us/op | Scalar B/op | SIMD B/op |
| --- | --- | ---: | ---: | ---: | ---: |
| Factor | `matrixCase=precision:8` | 0.282 ± 0.124 | 0.226 ± 0.054 | 568.0 | 568.0 |
| Factor | `matrixCase=precision:128` | 180.106 ± 35.330 | 127.806 ± 13.310 | 410,425.1 | 410,808.7 |
| Factor | `matrixCase=rbf:128` | 181.352 ± 43.144 | 128.828 ± 12.768 | 410,425.1 | 410,808.8 |
| Factor | `matrixCase=rbf:512` | 11626.898 ± 6220.330 | 4302.316 ± 906.626 | 8,783,751.7 | 8,784,593.2 |
| Lower solve | `shape=128x16` | 137.954 ± 67.616 | 28.920 ± 2.963 | 57,416.8 | 65,624.2 |
| Matrix solve | `shape=128x16` | 173.665 ± 53.752 | 63.846 ± 9.977 | 98,409.0 | 114,824.4 |
| Log determinant | `historySize=128` | 1.026 ± 0.288 | 1.055 ± 0.452 | 0.0 | 0.0 |
| Vector solve | `historySize=128` | 10.180 ± 1.467 | 8.471 ± 1.348 | 1,040.1 | 1,040.0 |
| Factor + solve + log determinant | `historySize=128` | 260.980 ± 124.805 | 196.874 ± 99.837 | 411,449.5 | 411,833.2 |

## Accuracy

The sweep covers precision, RBF, and Matérn-5/2 matrices at sizes 63/64/65 and
127/128/129 and scales `1e-100`, `1`, `1e100`. The solve uses 16 right-hand sides.
Reconstruction is maximum absolute entry error divided by maximum input entry
magnitude. Backward error is the maximum per-column value of
`||A x - b||∞ / (||A||∞ ||x||∞ + ||b||∞)`.

These tables show maxima over all 18 size/scale combinations in each family.
Raw reports include every case and the selected backend:
[scalar](accuracy-scalar.csv), [SIMD](accuracy-simd.csv).

| Fixture | Scalar reconstruction | SIMD reconstruction | Scalar backward error | SIMD backward error |
| --- | ---: | ---: | ---: | ---: |
| precision | 1.140e-15 | 1.140e-15 | 3.589e-16 | 3.816e-16 |
| rbf | 1.360e-15 | 1.360e-15 | 4.589e-17 | 4.218e-17 |
| matern | 9.713e-16 | 1.166e-15 | 5.394e-17 | 4.594e-17 |

Maximum absolute log determinant errors on analytical diagonal fixtures:

- scalar: `1.819e-11` over scales `1e-100`, `1`, and `1e100`.
- simd: `1.819e-11` over scales `1e-100`, `1`, and `1e100`.

## Validation and follow-up

`./gradlew check lintDocs --rerun-tasks` passed. The generated JVM and Linux x64
harnesses each completed all 17 smoke cases, including the 25-matrix fitting
round. Library JVM tests also passed with SIMD disabled. Fixture unit tests
cover deterministic generation, prescribed diagonal shift, reconstruction,
backward error, analytical log determinant, and singular duplicate-input failure.
After the final test fixture adjustments, all seven fixture tests passed again
on JVM scalar, JVM SIMD, and Linux x64. The final scalar run's longest new JVM
test took 75 ms.

The smoke timings are excluded from this baseline. Native allocation data has
not been collected; the Native harness does not provide bytes/op profiling.
The allocating factor API is the measured starting point. Reusable destinations
and workspaces, rank updates and regression update/solve cycles, factor
extensions, retry/exhaustion cases, consumer posterior validation, and measured
optimizations remain follow-up work as listed in the
[suite documentation](../../../README.md#remaining-measurements).
