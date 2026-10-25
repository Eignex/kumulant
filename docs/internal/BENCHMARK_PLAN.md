# Benchmark capture plan

## Goal

Keep deterministic workloads in Kotlin and provide one capture command for local
use and Eignex Lab. A capture preserves structured results, original measurements,
validation, and enough provenance to reproduce the run. No external dataset
catalogue or download/cache layer is needed.

## Design

- Define stable case IDs, fixture versions, seeds, parameters, operation units,
  lifecycle, and preset membership in code. Share fixtures with existing suites.
- Expose performance, quality, and concurrency modes through one interface.
  Use JMH for JVM performance and retain the multiplatform harness for Native.
- Prepare inputs outside timing, declare whether state is fresh or accumulating,
  and distinguish one API operation from one batch or complete lifecycle.
- Produce versioned `manifest.json`, `results.json`, `summary.md`, and `raw/`.
  Record failures and skips explicitly and retain partial evidence on failure.
- Run each requested JVM backend in a separate process and record the resolved
  backend. Keep instrumented captures separate from performance baselines.
- Use Eignex Lab's existing committed-script profile contract and `--output`.
  Lab owns placement and artifact retention; Kumulant owns measurement semantics.

## Implementation sequence

### 1. Capture foundation (implement first)

- [x] Add a Kotlin catalogue with representative stat and Cholesky cases and
  smoke/standard/extended selections; do not migrate every existing benchmark yet.
- [x] Add one executable capture script that builds and launches on the configured
  JDK, with case/suite/mode/backend filtering and an output directory.
- [x] Keep JVM performance on JMH, retain its original JSON, normalize batch units,
  and record timing settings and allocation metrics when requested.
- [x] Add structured quality measurements for the selected stat and Cholesky cases;
  validate finite results and explicit numerical tolerances before accepting timing.
- [x] Write provenance and incremental results, reject empty selections and output
  overwrites, and return nonzero when a measurement or validation fails.
- [x] Add an example Lab profile specification outside `.github/` and document the
  supported interface and current coverage.
- [x] Verify filtering, failure retention, normalization, backend selection, and an
  end-to-end smoke capture. Run `./gradlew check lintDocs --rerun-tasks`.

### 2. Migrate workload coverage

The workload layer is the second PR, based on the capture foundation. A common
`caseId` benchmark resolves prepared regression, storage, vector, sketch and bandit
workloads from the catalogue. Quality includes held-out predictions, owned storage,
scalar vector references, quantile ranks, frequency estimates and multi-seed
expected-reward regret. Native integration is a separate subsequent stack layer.

- Replace duplicated benchmark-name lists with catalogue-driven selection.
- Add typed regression fixtures so feature generation is outside update timing.
- Migrate vector, regression, storage, and bandit performance workloads.
- Add native captures through the existing multiplatform harness, with explicit
  unsupported-target skips and independent process repetitions.
- Expand quality to quantile rank error, sketch error, held-out prediction error,
  and multi-seed bandit expected-reward regret. Keep bookkeeping invariants separate.

### 3. Concurrency and consolidation

The third stack layer adds a concurrency mode for shared sum/mean/variance update
and merge paths. It reports aggregate throughput, one-writer scaling and parallel
efficiency, verifies exact observation preservation while a reader checks snapshots,
and propagates worker/reader failures with bounded cleanup. Cases use one/four
writers (extended adds eight), and Strict/Relaxed/HighWrite contracts. Order-sensitive
stats and legacy entry-point retirement remain follow-up work until their own
references and coverage are available.

- Add workload-specific update-preservation, snapshot, merge, and drift reports.
- Propagate worker exceptions and always shut down executors; do not equate
  disjoint timestamp ranges with globally ordered execution.
- Define fresh, pre-trained, and accumulating states explicitly; advance time for
  repeated timestamped streams.
- Remove handwritten timing loops and legacy entry points only after equivalent
  coverage is available through captures.

### 4. Lab comparisons

- Add structured metric ingestion and comparison views in Eignex Lab separately.
- Match case definitions, fixture fingerprints, settings, and hardware across refs;
  retain raw samples and show uncertainty rather than a single speedup number.
- Support paired revision comparisons on the same host without forcing synthetic
  Kumulant workloads into Lab's solver-problem model.

## Completion notes

Stage 1 is implemented. The entry point is `kumulant-bench/capture-report.sh`;
`kumulant-bench/lab/smoke.json` uses Lab's existing profile-script interface. The
README documents coverage, case lifecycles, presets and report schema. Representative
workload and concurrency migration is implemented in subsequent stack layers.
Native capture, remaining operation families, legacy consolidation and Lab metric
ingestion remain follow-up work; existing analysis entry points are retained.

The stack starts with #146 (`codex/benchmark-capture`, based on main), then #148
(`codex/benchmark-workloads`, based on the foundation), then the concurrency layer
(`codex/benchmark-concurrency`, based on workloads). Each layer has its own full
local gate; Lab jobs and performance baselines are not submitted by this work.

Validation completed:

- `./gradlew check lintDocs --rerun-tasks` passed (62 tasks executed).
- `:kumulant-bench:prepareCapture :kumulant-bench:jvmTest` passed after the final
  code edits; all 11 capture unit tests complete within 300 ms individually.
- The standard quality preset passed all 16 case/backend combinations.
- The final smoke capture with allocation profiling passed all 10 combinations.
  Checked the raw JMH JVM flags, identical input fingerprints across backends,
  normalized timing/allocation, dependency versions and manifest settings.
- A forced scalar/backend mismatch blocked timing for that case, retained its
  failure log, allowed the SIMD case to finish, and returned a failed capture.
- Reusing that nonempty output directory failed without altering its evidence.
- Shell syntax and `git diff --check` passed. No Lab job was submitted; the job
  specification and executable entry point were checked locally.

Subsequent stack-layer validation:

- The workload layer passed its own `check lintDocs --rerun-tasks` gate, all 70
  standard quality combinations, and all 30 smoke timing/allocation combinations.
- The concurrency layer passed its own full gate (62 tasks), all 72 standard
  correctness combinations, and all 32 smoke timing combinations. Four final
  four-writer update captures verified raw JMH threads, aggregate throughput,
  normalized units and per-case manifest settings on both JVM backends.
- All 28 capture JVM tests pass and complete within 300 ms individually; the
  latest full-gate run's slowest capture test took 133 ms.
- No Lab jobs were submitted and smoke numbers are not performance baselines.
