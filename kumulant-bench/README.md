# kumulant-bench

Benchmarks and quality checks for stats, bandits and numerical kernels. Fixtures
are defined in `StatSpecs.kt`, `BanditSpecs.kt` and `CholeskyFixtures.kt`.
Requires Bash and the repository's JDK 25 toolchain. Run commands from the repository root.

## Run

```sh
kumulant-bench/benchmark.sh --smoke
kumulant-bench/benchmark.sh --quality
kumulant-bench/benchmark.sh --concurrency
kumulant-bench/benchmark.sh --scalar --smoke
kumulant-bench/benchmark.sh --native --smoke
```

The default run checks quality, then measures every registered stat and bandit
policy. JVM runs use three one-second warmups, five one-second measurements and
two forks. Smoke selects sum and Thompson sampling, with one warmup, two 100 ms
measurements and one fork; use it to verify wiring.

Pass JMH filters, parameters and profilers after `--`:

```sh
kumulant-bench/benchmark.sh --output build/factor-run -- '.*CholeskyFactorBenchmark.*' \
  -p matrixCase=precision:8,rbf:128 -bm avgt -tu us -prof gc
kumulant-bench/benchmark.sh -- -t 4 -p name=SumStat -p concurrency=Strict
```

JMH options override the defaults. The JVM opens the Vector API unless `--scalar`
is set; quality output identifies the resolved backend. Run backend comparisons
sequentially on an idle host.

Native requires Linux x86_64 and uses `linuxX64Test` and `linuxX64Benchmark` with
the default Native backend. Pass Gradle properties after `--`, for example
`-Pbench.filter='.*CholeskyFactorBenchmark.*'`. Scalar selection and concurrency
checks apply to JVM. Native reports kotlinx-benchmark JSON without an allocation
profiler; repeat process runs when comparing timings.

## Quality and measurement units

`--quality` compares stats with registry references, reports bandit expected-reward
pseudo-regret over five seeds, and checks Cholesky reconstruction, solves, log
determinants, rank updates and block extensions. Failed checks stop timing.

`--concurrency` checks sum, mean and variance updates and merges under Strict,
Relaxed and HighWrite with one or four writers and a concurrent reader. It checks
observation preservation and reports coupled-state drift. Concurrent timing
supports these stats and synchronization levels.

Each stat invocation applies 4,096 prepared updates; each bandit invocation runs
512 prepared choose/update rounds. Reports use batch units: multiply throughput
by the batch size, or divide time and allocation by it, for per-update or
per-round values. Numerical kernels measure one API call unless the method
explicitly names a repeated workload. Inputs are prepared outside timing.

## Output

Reports go under `kumulant-bench/build/runs/`, or `--output DIR` when the directory
is absent or empty.

| File | Contents |
| --- | --- |
| `run.txt` | Revision, working-tree status, platform, JDK and arguments |
| `source.patch` | Tracked changes relative to the recorded revision |
| `quality.txt` | Quality checks and backend details |
| `concurrency.txt` | Concurrency checks, when selected |
| `results.json` | JVM benchmark measurements |
| `benchmark.txt` | Benchmark log |
| `native/` | Native benchmark reports |

## Eignex Lab

[`lab/smoke.json`](lab/smoke.json) runs the shell harness from a Git checkout.
For prepared source archives, use `com.eignex.kumulant.bench.CholeskyLabProfileKt`
with build task `:kumulant-bench:prepareBenchmark` and classpath
`kumulant-bench/build/benchmarks/jvm/jars/kumulant-bench-jvm-jmh-JMH.jar`.

The Cholesky profile runs scalar and SIMD sequentially, checks the selected backend,
and writes quality reports, JMH JSON, logs and host details under `$OUT/report`.
Use `--filter REGEX` before JMH arguments, `--simd-only` for SIMD alone, or `--jfr`
for allocation attribution. Keep instrumented timings separate from ordinary runs.
Scripts own profiling; leave Lab recording off when using their profilers.

For prepared Native runs, add
`:kumulant-bench:linkLinuxX64BenchmarkReleaseExecutableLinuxX64` and
`:kumulant-bench:linkCholeskyQualityReleaseExecutableLinuxX64` to the build tasks
and pass `--native-prepared`. Filters and parameters use `-Pbench.filter` and
`-Pbench.param.*`. Add `--stress-order=2048` to run larger numerical checks before
JVM or prepared Native timing. Retain the selected revision, host details, resource
limits and raw samples with each comparison.
