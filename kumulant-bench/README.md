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

## Bench SDK adapter

`lab/bench.json` owns the prepared JVM adapter's build and launch recipe.
`lab/native-linux-x64.json` also runs the Native bench tests and links the benchmark and quality executables.
The SDK dependency `com.eignex:lab-bench:0.1.0` is confined to `jvmMain`; the
existing JMH and kotlinx-benchmark engines continue to own measurement.

The default dependency resolves [Lab Bench 0.1.0](https://github.com/Eignex/eignex-lab/releases/tag/lab-bench-v0.1.0)
from public GitHub Release assets without a GitHub login or token. Prepare the adapter with:

```sh
./gradlew :kumulant-bench:prepareBenchmark
```

`-PlabBenchVersion=VERSION` selects a different SDK release. To develop against a
local SDK checkout, run `./gradlew :lab-bench:stageAdapterRepository -PbenchVersion=0.1.0`
in that checkout, then add
`-PlabBenchRepository=file:///absolute/eignex-lab/lab-bench/build/adapter-repository`
to the consumer build. Refresh dependencies when restaging the same version.

A lab submission uses `repository: "Eignex/kumulant"` and
`bench: {"manifest":"kumulant-bench/lab/bench.json","request":{"parameters":{"backends":"scalar,simd","smoke":"true"}}}`.
The planner expands one case per backend and freezes registry selections and timing
defaults. Every case runs its backends sequentially within the harness.

| Request key | Supported values and behavior |
| --- | --- |
| `backends` | `scalar,simd` by default; `native` requires the Native manifest and Linux x86_64 |
| `mode` | `performance` (default), `quality`, `concurrency` (JVM only) |
| `smoke` | Strict boolean; selects small fixtures and short timing defaults |
| `argv` | JSON string array for performance; preserves repeated arguments and literal filters |
| `repetitions` | Positive process repetitions; default 1 on JVM and Native smoke, 2 on ordinary Native runs |
| `stress-order` | Cholesky diagnostic order greater than 16, before measurement |
| `heap-mib` | JVM heap per process, default 512; at least 64 and within the two-JVM memory budget |
| `jvm-gc` | JVM `g1` (default), `serial`, `zgc` |
| `jfr` | Strict boolean; JVM performance only; recordings retained beneath each repetition |
| `memory-mib` | Declared case memory, at least 2048; default 2048 |
| `timeout-seconds` | Positive outer execution timeout; default 3600 |
| `native-executable` | Portable prepared `.kexe` path under `kumulant-bench/build/` containing `/linuxX64/` |
| `native-quality-executable` | Same path rules for the Native Cholesky diagnostic executable |

JVM `argv` accepts JMH filters, repeated `-p` selections, measurement modes, time
units, warmups, iterations, iteration times, batch sizes, positive fork/thread
counts, and portable `gc`, `stack`, `safepoints`, `comp` and `cl` profilers. Use
`jfr=true` for recordings. Report destinations, Java executable/flags, listing
commands and group thread counts are adapter-owned and rejected in `argv`.
These restrictions keep paths portable, backend selection verifiable and heap
reservations consistent. Concurrent timing uses the existing JMH contract:
sum/mean/variance, Strict/Relaxed/HighWrite, with no explicit filter.
JMH thread counts become declared CPUs; concurrency checks reserve five CPUs
for four writers and the concurrent snapshot reader.

Native `argv` accepts `-Pbench.filter=REGEX`, `-Pbench.param.NAME=VALUES`,
`-Pbench.stats=VALUES`, `-Pbench.policies=VALUES`, `-Pbench.mode=avgt|thrpt`,
`-Pbench.timeUnit=ns|us|ms|s|m`, `-Pbench.warmups=N`, `-Pbench.iterations=N`,
`-Pbench.iterationTime=N` and `-Pbench.iterationTimeUnit=ns|us|ms|s|m`.
Timing properties also work with the ordinary Gradle benchmark task. Omitted
workload properties retain the registry defaults even when another property is
supplied. `stats` and `param.name` (or `policies` and `param.policy`) are aliases;
select one form. Native warmups must be positive: the engine calibrates iteration counts during
warmup and produces nonfinite average times or zero throughput without it.
Duplicate properties, unsupported Gradle arguments and shared
Native concurrency levels are rejected. Submit JVM and Native argument dialects
separately. Annotation-defined benchmark modes/units apply unless overridden.

`benchmark.sh --prepared` uses `KUMULANT_BENCH_JAVA` and the prepared JMH jar
without Gradle. Each JVM repetition checks the resolved scalar/SIMD backend,
runs the full serial quality suite and optional Cholesky stress checks before
its selected operation. The prepared Native runner first runs JVM reference
quality, then Native Cholesky diagnostics and optional Native stress checks.
Native fixture tests run during preparation. Native stat/bandit timing is serial;
the Native concurrency preservation checks and multithreaded timing are unavailable.
Native selects the host's default Koblas engine, which can include an installed
vendor library; its actual backend, vendor, library and thread evidence are retained.

The planner declares both Native executables as SDK inputs, so preparation hashes
and relocates them with the source bundle. Runtime resolves them from
`LAB_WORKTREE`, independent of the launcher's working directory. No build tools
are invoked during prepared execution. The SDK remains confined to `jvmMain` and
resolves anonymously from the public release assets.

Every file beneath `report/` is retained, including nested repetition directories,
JFR recordings, Native case configurations, raw samples, progress and partial
output. JVM captures include revision, patch, platform, Java and dependency details;
Native captures also retain runtime details and executable invocations. A failure
retains completed metrics, available partial reports and logs. Malformed reports
mark the result failed without discarding valid earlier measurements.

SDK metrics come from JMH/kotlinx-benchmark primary and secondary scores and
quality/concurrency/stress rows. Scores retain the engine's units: throughput is
higher-is-better, elapsed time and numerical error/drift are lower-is-better;
uncertainty and diagnostic values carry no preference. Allocation per operation
is lower-is-better. Nonfinite optional scores are omitted; a missing or nonfinite
primary result fails the case. `details.metricSources` maps each metric to its
report, row/line, benchmark, parameters and batch size. Batch units remain unchanged;
raw reports retain samples and confidence information. The lab consumes this
protocol without Kumulant-specific capture commands or metric parsing.

For example, pass the following request parameters for a small repeated JVM
allocation capture (a wiring check, not performance evidence):

```json
{
  "backends": "scalar,simd",
  "smoke": "true",
  "repetitions": "2",
  "jvm-gc": "serial",
  "heap-mib": "256",
  "argv": "[\".*UpdateThroughputBenchmark.update\",\"-p\",\"name=SumStat\",\"-prof\",\"gc\",\"-bm\",\"avgt\",\"-tu\",\"us\"]"
}
```

Smoke verifies wiring; full runs on idle target hardware provide measurement evidence.
