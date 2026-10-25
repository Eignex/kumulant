# kumulant-bench

## Unified captures

Workloads and presets are defined in Kotlin in `CaptureCases.kt`; there are no
dataset files to download or configure. The capture entry point builds the JVM
harness with the repository's JDK 25 toolchain and runs each backend in a separate
process. Requires Bash and the repository's Gradle toolchain.

```sh
# Show options or list the selected case definitions.
kumulant-bench/capture-report.sh --help
kumulant-bench/capture-report.sh --preset smoke --list

# Check quality and harness wiring on both JVM backends.
kumulant-bench/capture-report.sh --preset smoke

# Validate without timing, or measure a selected family.
kumulant-bench/capture-report.sh --suite stats --mode quality
kumulant-bench/capture-report.sh --case 'cholesky/factor/*' --targets jvm-scalar,jvm-simd

# Collect allocation evidence in a separate instrumented capture.
kumulant-bench/capture-report.sh --preset smoke --allocation --output build/allocation-capture
```

`--case` matches stable case IDs with `*` wildcards; quote the pattern to prevent
shell expansion. `--suite` is `stats` or `cholesky`. `--mode` is `performance`
(default) or `quality`; performance always requires quality validation to pass
first. A backend mismatch fails validation rather than silently publishing a
fallback's timing. Scalar and SIMD rows for stats can be identical implementations;
the labels identify the JVM configuration and selected Koblas backend.

| Preset | Selection | JVM timing per case and backend |
| --- | --- | --- |
| `smoke` | Sum, mean, variance, one small factorization and solve | 1 warmup, 2 measurements, 100 ms each, 1 fork |
| `standard` | Stats and representative precision, RBF, Matérn and solve shapes | 3 warmups, 5 measurements, 1 s each, 2 forks |
| `extended` | Stats, block-boundary matrices and larger shapes | 5 warmups, 10 measurements, 1 s each, 3 forks |

Smoke timing checks wiring only. Every performance measurement uses one thread,
average ns/op, and a fixed 512 MiB heap. A stat invocation is 4096 updates from a
precomputed weighted stream, accumulated into one stat throughout the trial; the
reported time and allocation are divided by 4096. Factorization and matrix solve
rows report one complete allocating API call. Each case definition records its
state lifecycle, parameters, seed, fixture version and operation size.

Quality runs use fresh state. Stats compare against a batch reference with scaled
absolute error at most `1e-10`. Cholesky checks relative reconstruction and normwise
solve backward error at most `1e-13`, and finite log determinant. These checks are
acceptance gates for these cases, not a complete algorithm-quality assessment.

### Reports

Each run creates a new directory under `kumulant-bench/build/captures/` unless
`--output DIR` is supplied. The destination must be absent or empty; existing
evidence is never replaced.

- `manifest.json`: report schema, source revision, working-tree status, tracked
  diff hash, compiled harness hash, resolved runtime library names, JMH version,
  runtime/hardware facts, settings and selected cases.
- `results.json`: schema 1 results, quality metrics and tolerances, input SHA-256,
  resolved backend, normalized ns/operation and optional bytes/operation.
- `summary.md`: status and timing table for reading locally or in Lab artifacts.
- `raw/`: quality JSON, original JMH JSON with fork samples and confidence
  intervals, exact command argument arrays and process logs.

Results are saved after each case. Failed validation prevents that case's timing;
other selected cases continue and the command exits nonzero. Pending entries and
raw logs preserve evidence of incomplete runs. Each measurement process has a
two-minute timeout, with child processes terminated on timeout or capture shutdown.
An allocation capture is marked instrumented in its manifest. Pin captures to
appropriate cores on an idle host and compare matching fixtures, settings and
hardware; retain raw uncertainty rather than relying on a single speedup ratio.

### Eignex Lab

Submit `kumulant-bench/lab/smoke.json` with Lab's `deploy/lab run` command. It uses
the existing committed-script profile interface; Lab supplies `--output` and
retains the report directory. Commit the harness to the selected trusted ref
before submitting it. Scripts own measurement and profiling, so the Lab spec
does not request experiment-level `measurement` instrumentation.

The unified capture currently covers JVM stat updates and Cholesky factorization
and matrix solves. Native, regression, bandit and concurrency migration, and Lab
metric ingestion, are follow-up stages in
[`BENCHMARK_PLAN.md`](../docs/internal/BENCHMARK_PLAN.md). Existing benchmark and
analysis entry points remain available during migration.

## Cholesky benchmarks

The Cholesky suite establishes the allocating public API baseline for shared
regression and GP numerical kernels. Matrix generation is deterministic (seed
139); no model or acquisition implementation is included.

The [preliminary baseline report](results/cholesky/2026-10-25/README.md) retains
selected scalar/SIMD timing, allocation, and accuracy results with raw data.

## Workloads

| Benchmark | Parameter | Timed work |
| --- | --- | --- |
| `CholeskyFactorBenchmark.factor` | `matrixCase`: `precision:p`, `rbf:n`, `matern:n` | Validate and factor an existing matrix, including owned result storage and backend workspace/packing |
| `CholeskyVectorBenchmark.solveVector` | `historySize`: `n` | Solve with an existing factor, allocating a result vector |
| `CholeskyVectorBenchmark.logDeterminant` | `historySize`: `n` | Log determinant of an existing factor |
| `CholeskySolveBenchmark.solveMatrix` | `shape`: `nxq` | Solve `A X = B`, allocating a result matrix |
| `CholeskySolveBenchmark.solveLower` / `solveLowerTranspose` | `shape`: `nxq` | Solve `L X = B` / `Lᵀ X = B`, allocating a result matrix |
| `CholeskySolveBenchmark.candidateBatches` | `shape`: `nxq` | 16 lower-triangular batch solves reusing one factor |
| `CholeskyWorkloadBenchmark.covarianceConstruction` | `historySize`: `n` | Build one covariance matrix from existing input points |
| `CholeskyWorkloadBenchmark.factorSolveLogDeterminant` | `historySize`: `n` | Factor, vector solve, and log determinant on an existing covariance |
| `CholeskyWorkloadBenchmark.covarianceFactorSolveLogDeterminant` | `historySize`: `n` | Covariance construction plus factor, solve, and log determinant |
| `CholeskyWorkloadBenchmark.fittingRound` | `historySize`: `n` | 25 factor/solve/log determinant cycles on distinct, precomputed covariances |

`p` is feature dimension; `n` is history size; `q` is simultaneous right-hand-side
count. Solve shapes are selected examples rather than a Cartesian product.
Vector-only operations use a separate state to avoid repeating them for each `q`.
One operation in a repeated workload means the **entire sequence**, not one solve
or factorization. Return values and blackholes consume the results.

Precision matrices are `I + Xᵀ X` with `2p` seeded observations, feature values
uniform on `[-1, 1] / sqrt(p)`, and a unit prior. Covariances use unit signal
variance, length scale 0.15, RBF or Matérn-5/2 kernels, noise variance `1e-6`,
and numerical jitter `1e-10`. Input pairs differ by at most `1e-8` on a regular
grid. The fitting round uses the same points with 25 length scales from 0.05
through 0.29 in steps of 0.01. Right-hand sides use seed 140.

All fixtures are built in setup except the explicitly named covariance
construction/lifecycle operations. Every factorization starts from the same
unchanged input. No timed workload accumulates updates or changes conditioning.
Public solves include the right-hand-side copy required by their ownership
contract. Factorization includes lower-triangle validation, owned factor storage,
and panel/trailing-block copies. GC allocation totals include these caller/API
and backend costs together; they do not isolate backend packing.

## Run

Use JDK 25. The Gradle toolchain resolver can provision it.

```sh
# Verify harness wiring quickly; these timings are not a performance baseline.
./gradlew :kumulant-bench:jvmCholeskySmokeBenchmark
./gradlew :kumulant-bench:linuxX64CholeskySmokeBenchmark

# Full filtered suite: 3 warmups, 5 measurements, 1 second each, average us/op.
# JVM uses two independent forks and a fixed 512 MiB heap.
./gradlew :kumulant-bench:jvmCholeskyBenchmark
./gradlew :kumulant-bench:jvmCholeskyBenchmark -Pkumulant.noSimd=true
./gradlew :kumulant-bench:linuxX64CholeskyBenchmark
```

Gradle writes JSON under `kumulant-bench/build/reports/benchmarks/`. JVM SIMD is
enabled by opening `jdk.incubator.vector`; `kumulant.noSimd=true` omits the module
to select koblas's scalar fallback. Run backends sequentially on an idle machine.
For Native comparisons, repeat the process run and retain each JSON report;
record the backend and allocator configuration. Native time/op does not imply
bytes/op; this harness does not supply a Native allocation profiler.

For selected cases and JVM allocation/GC data, build the harness JAR and use JMH
directly. Set `JAVA` to the JDK 25 executable. This example runs 14 representative
cases with the same warmup and measurement settings as the filtered suite:

```sh
./gradlew :kumulant-bench:jvmBenchmarkJar
JAVA=/path/to/jdk-25/bin/java
JAR=kumulant-bench/build/benchmarks/jvm/jars/kumulant-bench-jvm-jmh-JMH.jar

"$JAVA" --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector \
  -Xms512m -Xmx512m -jar "$JAR" '.*Cholesky.*Benchmark.*' \
  -p matrixCase=precision:8,precision:128,rbf:128,rbf:512 \
  -p shape=128x16 -p historySize=128 \
  -bm avgt -tu us -wi 3 -i 5 -w 1s -r 1s -f 2 -t 1 \
  -prof gc -foe true -rf json -rff cholesky-simd.json
```

Repeat without `--add-modules=jdk.incubator.vector` for scalar, changing the output
filename. JMH inherits these JVM arguments in its forks. The `gc` profiler reports
allocated bytes/op, allocation rate, GC count, and GC time. Keep the full JSON
(including raw samples and confidence intervals), hardware/OS, affinity, JDK,
Kotlin/koblas revisions, source revision, and command lines with any report.
Compare candidates with identical fixtures and settings. For the opt-in large
case, filter `CholeskyFactorBenchmark.factor` and set `-p matrixCase=rbf:2048`;
one input matrix alone occupies 32 MiB.

## Numerical checks

```sh
./gradlew :kumulant-bench:jvmTest :kumulant-bench:linuxX64Test
./gradlew :kumulant-bench:jvmTest -Pkumulant.noSimd=true
./gradlew :kumulant-bench:analyzeCholeskyAccuracy
./gradlew :kumulant-bench:analyzeCholeskyAccuracy -Pkumulant.noSimd=true
```

The report prints the selected backend, diagonal shift, relative maximum-entry
reconstruction error, and maximum per-column normwise backward error
`||A X - B||∞ / (||A||∞ ||X||∞ + ||B||∞)`. It covers all three fixture families at
63/64/65 and 127/128/129 with matrix scales `1e-100`, `1`, and `1e100`; it reports
absolute log determinant error for scaled diagonal matrices with an analytical
answer. Backward error is used because forward solution error can grow with
conditioning. The unit tests also check reproducibility and duplicate-input
failure/success with an explicit caller-supplied shift. Existing library tests
cover ownership, triangle, empty-shape, and failure contracts.

## Remaining measurements

This suite covers the existing allocating factor API. Reusable destination and
workspace paths, bounded rank-update versus refactorization comparisons,
regression update/solve cycles, factor extension versus full refactorization,
and caller retry/exhaustion timing remain follow-up work. No retry policy is
added to the numerical factorization. Consumer posterior mean/variance and full
GP/acquisition validation stay in Combo. Optimization acceptance requires
before/after accuracy, allocation, and timing evidence, including small matrices.
