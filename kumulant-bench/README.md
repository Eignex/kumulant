# kumulant-bench

Fixtures stay in Kotlin in `StatSpecs.kt`, `BanditSpecs.kt` and `CholeskyFixtures.kt`.
`benchmark.sh` runs existing quality checks and JMH/Native engines, retaining their
original outputs. Requires Bash and the repository's JDK 25 toolchain.

```sh
kumulant-bench/benchmark.sh --smoke
kumulant-bench/benchmark.sh --quality
kumulant-bench/benchmark.sh --concurrency
kumulant-bench/benchmark.sh --scalar --smoke
kumulant-bench/benchmark.sh --native --smoke

# Standard JMH filters, parameters and profilers pass through unchanged.
kumulant-bench/benchmark.sh --output build/factor-run -- '.*CholeskyFactorBenchmark.*' \
  -p matrixCase=precision:8,rbf:128 -bm avgt -tu us -prof gc
kumulant-bench/benchmark.sh -- -t 4 -p name=SumStat -p concurrency=Strict
```

Default timing selects all 69 registry stats and five bandit policies. Smoke uses
sum and Thompson sampling, one warmup and two 100 ms measurements in one fork.
Ordinary JVM runs use three warmups, five one-second measurements and two forks;
explicit JMH options override these defaults. The default JVM opens the Vector
API; `--scalar` omits it. Quality prints the resolved backend. Run backends
sequentially on an idle host; smoke timings verify wiring, not performance.

Each stat invocation applies 4096 prepared updates, and each bandit invocation
performs 512 prepared choose/update rounds. JMH/Native JSON preserves **batch**
units: multiply batch throughput by that count, or divide batch time/allocation
by it, for per-update/cycle values. Numerical kernel methods measure one complete
API call unless their method explicitly names a repeated workload. Regression
features and bandit reward/context inputs are prepared outside timing; timestamped
stat streams advance between batches.

Quality compares fresh stat runs with registry references, reports bandit expected-
reward pseudo-regret over five seeds, and checks Cholesky reconstruction, solve
backward error and analytical log determinants across scaled fixtures. Approximate
stats report reference error; sum/mean/variance enforce `1e-10` scaled error.
Concurrent checks cover sum/mean/variance under Strict/Relaxed/HighWrite, one/four
writers, updates and merges, with exact observation preservation and a reader.
Relaxed coupled drift is reported. Order-sensitive/shared unsynchronized timing
is rejected, and worker failures propagate with executor cleanup.

Native uses the existing `linuxX64Test` and `linuxX64Benchmark` Gradle tasks, retaining
kotlinx-benchmark JSON. Pass Gradle properties after `--`, for example
`-Pbench.filter='.*CholeskyFactorBenchmark.*'`. It uses the default Native backend;
`--scalar` and concurrent mode apply to JVM. Repeat Native process runs for
comparisons; Native timings provide no allocation profiler or JMH confidence interval.

Outputs go under `kumulant-bench/build/runs/` or `--output DIR` (absent/empty only):
`run.txt` records revision, dirty state, platform, JDK and arguments; `source.patch`
retains tracked edits; `quality.txt` and `concurrency.txt` retain reports;
`results.json`/`benchmark.txt` retain JMH results/logs. Native reports are under
`native/`. Failed checks stop timing and keep their output. JMH owns fork samples,
uncertainty and runtime settings; there is no additional result schema or catalogue.

For Eignex Lab, use the JVM entry point described below with a committed trusted
ref. It works with Lab's source archives, where Git metadata is unavailable;
the selected SHA is retained in the job receipt. `lab/smoke.json` describes the
script-profile interface for a Git checkout. Scripts own profiling; leave Lab
recording off when using their profilers. Inspect retained outputs alongside
Lab's revision, host, and resource metadata.

The [historical Cholesky baseline](results/cholesky/2026-10-25/README.md) retains its
original measurements. The [Cholesky comparison](results/cholesky/2026-10-26/README.md)
retains Lab timing, allocation, accuracy, and consumer checks. Prediction, storage,
vector and Cholesky benchmark kernels remain selectable with standard benchmark filters.
The [published-snapshot view comparison](results/cholesky/2026-10-26/views/README.md)
isolates borrowed regions using the same Koblas dependency on both implementations.

## Cholesky comparisons

`CholeskyFactorBenchmark` compares the owned factor factory with and without a
reused workspace. The owned result is allocated in both cases. JVM factorization
passes borrowed destination regions to Koblas for trailing updates. Native retains
contiguous copied blocks, which were faster in the matched comparison. A workspace
lends backend scratch and Native panel/trailing storage through Koblas's bounded
pool. Backend packing can still allocate, and large or changing shapes can exceed
the pool's retained lengths.

`CholeskySolveBenchmark` and `CholeskyVectorBenchmark` compare allocating solves
with `solveInto`/`solveLowerInto` and a reused destination/workspace. Independent
right-hand sides are copied into the destination before every solve. The
`copyRightInto` method isolates that caller copy; it does not measure packing.
`candidateBatches` methods perform 16 solves per invocation on the same factor.
Single-column destination solves use vector triangular kernels. Allocating matrix
solves retain their triangular matrix kernel route.

`CholeskyUpdateBenchmark` compares immutable rank-one updates with copying the
matrix, adding the outer product, and refactorizing. `factorCopy` isolates the
existing factor's ownership copy. Posterior sequences perform eight update/solve
rounds per invocation, including a fresh information-vector copy and, for full
refactorization, a fresh precision-matrix copy. Every invocation starts from the
same seeded precision and updates; conditioning does not evolve between samples.

`CholeskyExtensionBenchmark` appends 1, 4, or 16 entries, comparing extension with
full refactorization. The full covariance is generated once; the old factor and
both appended blocks use that same matrix, kernel, noise variance, and jitter.
Both operations return an independently owned enlarged factor. The copy baseline
copies the old factor only; extension also allocates the larger result.

`CholeskyRetryBenchmark` measures caller policy separately from ordinary kernels.
Its fixed schedule tries diagonal shifts `0`, `1e-12`, `1e-10`, and `1e-8` on fresh
copies of a 65-by-65 diagonal fixture. The last input pivot is `1`, `-1e-10`, or
`-1` for success, retry, or exhaustion. The quality report records attempts,
last applied shift, and success. Retry policy remains outside the library API.

Quality also compares eight rank updates and block extensions against full
refactorization, including RBF/Matérn inputs around 63/64/65 and 127/128/129 at
scales `1e-100`, `1`, and `1e100`. Reconstruction and backward-error limits are
`1e-13`. Extension forward/log-determinant tolerance uses a conservative
`64 * epsilon * order / noiseVariance` bound for these unit-signal kernel fixtures.
Published results keep the observed errors alongside those tolerances.

## Paired lab captures

`CholeskyLabProfileKt` runs scalar and SIMD sequentially in one Lab case, retaining
each backend's quality output, JMH JSON/log, runtime settings, and host details
under `$OUT/report`. JVM recording is optional with `--jfr`; those instrumented
timings belong to allocation attribution, separately from GC-only comparisons.
Pass `--filter REGEX` before ordinary JMH arguments to select a comparison.
`--simd-only` selects just SIMD. Quality must report the requested backend before
timing starts. The fat JAR preserves the multi-release manifest attribute required
by Koblas's JDK 25 backend; opening the Vector API alone does not prove SIMD ran.

For Native, `--native` runs two separate benchmark processes and retains both
reports, using `bench.mode=avgt` and `bench.timeUnit=us`. It runs measurement tasks
only. Parameters pass as Gradle properties, for example
`-Pbench.param.shape=32x1,128x16`; JVM parameters use standard JMH `-p` options.
Native has no bytes/op profiler in this harness.

For prepared Lab archives, add
`:kumulant-bench:linkLinuxX64BenchmarkReleaseExecutableLinuxX64` to the build tasks
and use `--native-prepared`. It invokes the generated release harness directly,
keeping compilation in preparation. Each benchmark runs in its own process, with
two repetitions, three one-second warmups, five one-second measurement iterations,
and output in microseconds. Filters and parameters use `-Pbench.filter` and
`-Pbench.param.*` as above. The configuration, invocations, samples, and engine JSON
are retained under `$OUT/report/native-*`.

Submit a committed trusted ref with `profile.main` set to
`com.eignex.kumulant.bench.CholeskyLabProfileKt`, `profile.buildTasks` containing
`:kumulant-bench:prepareBenchmark`, and `profile.classpath` containing
`kumulant-bench/build/benchmarks/jvm/jars/kumulant-bench-jvm-jmh-JMH.jar`.
The numerical benchmark tasks use JDK 25. Retain Lab's selected SHA, hardware,
resource limits, and raw samples; node capability tags alone do not identify
which instructions or backend executed.
