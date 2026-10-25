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

For Eignex Lab, submit `lab/smoke.json` with `deploy/lab run`. Select a committed
trusted ref. Lab supplies `--output` and retains the files through its existing
profile-script interface. Scripts own profiling; leave Lab recording off. Use the
job's retained files to inspect results alongside Lab's revision and host metadata.

The [historical Cholesky baseline](results/cholesky/2026-10-25/README.md) retains its
original measurements. Prediction, storage, vector and Cholesky benchmark kernels
remain selectable with standard benchmark filters.
