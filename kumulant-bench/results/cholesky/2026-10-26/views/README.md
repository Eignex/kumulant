# Published-snapshot Cholesky views comparison

The [complete capture](capture-status.json) covers 88 matched comparisons across
JVM scalar/SIMD and two Linux x64 Native process repetitions. Run
`python3 analyze.py` to regenerate the [tables](tables.md) and validation summaries.

This follow-up to the [original Lab comparison](../README.md) isolates borrowed
matrix regions in the blocked Cholesky trailing update. The copying baseline is
`e0260626fa71deb78a97c3b42196811f70146e4e`; the view candidate is
`bb6ef6cbd6436758e7c634586a3fbab7e93305b8`. Both use the identical published
Koblas cohort from `fc3b2fa5cb196bf09379e3039b8ffa5da9cb42dd`, containing the
merged subview implementation and subsequent SIMD fixes. Exact platform pins,
publication metadata, and artifact digests are in [dependency-cohort.json](dependency-cohort.json).

The numerical difference between these Kumulant revisions is the private trailing
update: two destination views replace extraction and write-back of panel/trailing
blocks. Factors remain independently owned; only the lower triangle is selected;
strict failures and caller-controlled jitter are unchanged. Allocating solve routes
are identical. Backend packing/scratch can still allocate, so this is not an
allocation-free factor API.

Production adopts borrowed regions on JVM and retains contiguous copied blocks
on Native. The Native view columns below measure a rejected trial implementation;
they are not the selected Native production route. The final shared factorization
calls a platform-specific trailing update whose bodies exactly match the measured
JVM view and Native copying implementations, as recorded in [provenance](provenance.json).

## JVM findings

The verified eight-lane SIMD capture reduces allocation and time on large RBF
factors and the allocating GP fitting round. Times below are mean ± JMH score
error; MB denotes decimal megabytes allocated per invocation.

| SIMD operation | Copy ms | View ms | Copy MB | View MB |
| --- | ---: | ---: | ---: | ---: |
| RBF factor with workspace, order 512 | 5.295 ± 0.453 | 3.117 ± 0.199 | 7.537 | 2.098 |
| RBF factor with workspace, order 1024 | 36.653 ± 7.005 | 16.678 ± 1.346 | 52.889 | 8.390 |
| Allocating fitting round, history 512 | 133.262 ± 11.205 | 102.225 ± 6.865 | 219.763 | 82.134 |
| Fitting round with workspace, history 512 | 126.688 ± 17.471 | 107.203 ± 18.451 | 188.433 | 52.442 |

Workspace fitting-round timing intervals overlap, so that row establishes an
allocation reduction rather than a clear timing improvement. At order 128 the
workspace already retains caller scratch; view descriptors add about 48 B/op
while eliminating copy work. At larger orders the view path avoids scratch
allocations that churn through the bounded pool. Workspace-backed allocation
approaches the owned factor payload (`8 × n²` bytes), with object/descriptor and
profiler overhead remaining. The allocating path still pays for backend scratch.

The [complete tables](tables.md) retain scalar results and mixed outcomes: scalar
precision-128 factor point estimates are higher with views, with overlapping
uncertainty. Unblocked order-8/32 JVM controls also overlap. Native order-32
comparisons reverse direction between process runs despite following unchanged
code, demonstrating cross-run drift beyond individual engine error bars. These
captures establish no universal factorization or Native timing speedup.

## Native decision

Borrowed regions substantially regress the captured Linux x64 Native workloads.
For an order-1024 RBF factor with a workspace, copying process means are
34.7–36.0 ms, while views take 84.2–96.3 ms: 2.4–2.7 times slower. Both process
repetitions show the same direction on the large factors. Native therefore keeps
the baseline's contiguous scratch blocks and bulk column copies. This choice
preserves the public contract and the shared numerical algorithm; only trailing
storage access differs by platform.

The exact Native kernel/vendor identity was not captured. Koblas's Native host
composition can pack regions through an element-wise adapter, so the measurements
do not establish that views eliminate copying on that backend. Routing is selected
from the observed end-to-end behavior, without attributing every timing difference
to a particular kernel.

## Measurements

The retained Lab definitions run identical seeded fixtures on both implementations:
order-8/32/128/512 regression precision matrices; order-128/512/1024 RBF covariance
matrices; factor/solve/log-determinant sequences; and 25-factorization GP fitting
rounds at history sizes 128 and 512. Fitting rounds use distinct precomputed
covariance matrices. Matrix construction is outside these timings and is measured
separately in the original report. Ownership, solve destination copies, and
workspace use are included according to each benchmark method.

Order 32 is a separate small-factor control using the same settings on both arms.
Orders 8 and 32 follow the unblocked route; blocking starts above 64. Each
comparison row identifies its raw capture. Precision, RBF, and GP fixtures run in
separate jobs after full captures exhausted Lab's infrastructure retry budget.
The fixture groups retain identical benchmark settings on both implementations.

JVM scalar and SIMD each use two forks, three one-second warmups, five one-second
measurement iterations, one thread, a fixed 512 MiB heap, and the GC profiler.
The runner verifies the actual backend before measuring. Linux x64 Native uses
release compilation and two separate process runs with the same warmup/measurement
settings. Native allocation profiling is unsupported by this harness.

Each job's definition, resolved source, runtime/host information, resource budget,
and exit status are retained under `lab/`. A cgroup budget is not an exclusive
pinned core; cross-run timing drift remains possible. Timing uncertainty and
individual fork/process samples must qualify comparisons.

Run `python3 analyze.py` to regenerate timing/allocation, numerical accuracy, and
consumer parity tables using only the Python standard library. Gzip-compressed
engine JSON under `raw/` retains individual samples, runtime settings, and scores;
`logs/` retains benchmark, backend quality, and invocation output. Interrupted full
jobs 8503–8506 are retained as failed infrastructure attempts and contribute no
timing rows.

The optional `--native-prepared` runner links the same generated release harness
during Lab preparation and invokes its per-benchmark protocol directly. Its local
integration check retains the same three/five warmup/measurement iterations,
one-second iteration time, release mode, and microsecond output. It avoids cold
Gradle builds in execution for future captures; its separate validation captures
are not inputs to these original matched timing tables.

## Validation

The published-snapshot [full gate](validation/full-gate.txt) passes with
`./gradlew check lintDocs --rerun-tasks`, including JVM/Linux Native tests and ABI
checks. All 54 scalar Cholesky tests also pass. Local
[scalar](validation/scalar-quality.txt) and [SIMD](validation/simd-quality.txt)
quality checks validate reconstruction/backward error, scaled and clustered GP
fixtures, update/extension equivalence, analytical log determinants, and caller
success/retry/exhaustion outcomes. Contract tests cover ownership/aliasing, upper
triangle poisoning, empty shapes, block boundaries, and nonfinite fallbacks.

[Combo validation](validation/combo.txt) at `f0674ecf` uses explicit Kumulant
platform substitution and the published Koblas dependency. Its GP surrogate tests
and frozen-history comparison at seed 401 pass. All 16 model rows match the
published Kumulant baseline exactly; 32 search rows differ by at most `2.48e-15`.
The paired consumer CSVs and [parity summary](consumer-parity.csv) are retained.

The [prepublication preview](../views-preview/README.md) used Koblas `6ab06f7` on a
working desktop. Its short local timings are exploratory and are separate from
this matched Lab comparison. The original report's update/extension and solve
measurements likewise retain their original dependency cohort.
