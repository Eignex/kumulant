# Cholesky Lab comparison, 2026-10-26

This capture evaluates reusable solves/scratch, immutable rank updates, and block
extensions for [#139](https://github.com/Eignex/kumulant/issues/139). The APIs keep
owned factors, nonmutating allocating solves, lower-triangle selection, and strict
failure without automatic jitter. Destination methods explicitly allow overwriting
the right-hand side; each concurrent call needs its own destination/workspace.

On verified eight-lane JVM SIMD, an order-512 immutable rank update takes about
**450 µs versus 6153 µs** for copying/updating/refactorizing (13.7 times faster),
and extending a 512-order factor by 16 takes **721 µs versus 5324 µs** for full
refactorization (7.4 times faster). Ownership copies are included. Scalar and Native
also benefit; the tables retain each backend's variability.

Workspace-backed order-128 RBF factorization on SIMD takes approximately **136 µs
versus 167 µs**, with allocation dropping from **411577 to 131273 B/op**. The
capture does **not** establish a general factorization speedup. At order
512, workspace factorization still copies blocks and the bounded pool churns;
its timing can be slightly worse than allocating scratch. Contemporary small-factor
repeats overlap: scalar baseline/candidate are `0.234 ± 0.010` / `0.229 ± 0.004`
µs, and SIMD is `0.230 ± 0.004` / `0.230 ± 0.008` µs. The repeated Native baseline
also slows substantially relative to its initial capture and overlaps the candidate
on existing paths, demonstrating cross-run drift. These controls qualify the
before/after estimates; they do not establish a general factorization improvement.

The [source-pinned view preview](views-preview/README.md) separately validates the
merged Koblas subview implementation before its snapshot publication. It retains
the integration patch, local allocation comparison, and source-based checks; those
exploratory measurements are separate from this Lab capture.

The [published-snapshot view comparison](views/README.md) isolates removal of the
caller block copies using one new Koblas cohort on both implementations. Its
measurements and validation are separate from the original capture below.

## Evidence and scope

- [Selected timing/allocation tables](tables.md) show point estimates and variability.
- [All measurements](measurements.csv) retain engine score errors, B/op, GC counts,
  and GC time; [raw reports](raw/) retain gzip-compressed engine JSON with
  fork/iteration samples and configuration.
- [Before/after comparisons](before-after.csv) match method and parameters exactly.
  [Alternatives](alternatives.csv) compare destination/workspace or update/extension
  methods with their reference operation within the candidate capture.
- [Accuracy](accuracy.csv), [consumer parity](consumer-parity.csv), and
  [validation output](validation.txt) retain correctness evidence.
- [Provenance](provenance.json), [Lab receipts](lab/), and [logs](logs/) record
  source/dependency revisions, job definitions, exit codes, hardware, and budgets.

Run `python3 analyze.py` from this directory to regenerate the CSVs and selected
tables using only Python's standard library. B/op values below one are profiler
overhead/residuals, rather than a promise of literally zero allocation. GC totals
belong to their complete benchmark run; compare normalized B/op across methods.

The numerical baseline is main at `8d0c8db`. Profile-only baseline commits add the
Lab entry point and packaging configuration. The numerical candidate is
`bce5218`; `44be116` fixes benchmark packaging without changing numerical code.
`63831d6` removes redundant destination clearing in allocating matrix solves;
`1958455` restores their original triangular kernel route after a small SIMD solve
regressed. The final allocating solve methods match the baseline implementation;
only destination methods specialize single-column inputs. Dedicated final solve
captures measure both alternatives. Before/after tables use those captures where
available; the raw earlier candidates remain available as diagnostics. Every
comparison row identifies its source capture.
`provenance.json` identifies the final test/quality revision. JVM quality in the
timing captures floors the solution-error denominator at one; the final local
quality rerun uses the actual solution norm, including for very small solutions.

### Runtime and backend verification

The Lab host exposes 16 virtual CPUs on an Intel Xeon Platinum 8488C. Each case
has a cgroup budget of **one CPU and 4608 MiB**, with two CPUs/4096 MiB for
preparation. This is a resource budget, not an exclusive pinned core. Other Lab
jobs may run on the same host. Resource receipts include CPU throttling counters;
they are not per-operation allocation or peak-RSS measurements.

JVM uses JDK 25.0.4.1, JMH 1.37, one benchmark thread, two forks, three one-second
warmups, five one-second measurement iterations, a fixed 512 MiB heap, average
time in microseconds, and the GC profiler. Native uses release compilation,
three/five one-second warmup/measurement iterations and two separate process
runs. Native ranges summarize the two process means; its raw engine uncertainty
is retained separately. The harness provides no Native B/op profiler.

The first captures opened the Vector API but **still resolved scalar**: flattening
Koblas's multi-release JAR lost `Multi-Release: true`. Their `simd` log directories
are retained as diagnostic evidence, and their raw files are named
`vector-module-scalar`. They are excluded from the actual SIMD comparison. The
fixed JAR preserves that attribute, and the Lab runner rejects a requested backend
unless quality reports the expected engine. Job 8402 failed this assertion on a
stale baseline task configuration; it contributes no timings. Capability tags and
module flags alone never establish the backend. The JAR's only JDK-25 override
of an existing class is `JvmBackend`, which supplies optional SIMD/vendor
compositions; the scalar engine continues to use the same portable kernel classes.
Native uses its default Linux
x64 composition; no exact native kernel identity was captured.

Both revisions pin Kotlin 2.4.21, Gradle 9.7.1, KBuild 1.3.5,
kotlinx-benchmark 0.4.17, and the same Koblas/serialization snapshots listed in
the provenance. Changes in dependencies are not part of this comparison.

## What each invocation includes

Fixtures use seed 139. Precision dimension, GP history, and RHS column count are
separate parameters. RBF/Matérn fixtures include clustered/nearly duplicate inputs,
noise variance `1e-6`, and explicit jitter `1e-10`. Construction stays outside the
kernel timings; baseline `covarianceConstruction` measures it independently.
The benchmark source retains larger optional shapes, including order 2048;
that optional stress case was not run here.

Factorization allocates an owned result with both entry points. Reusing a workspace
lends exact-size panel, trailing, and backend scratch arrays; the copies still run.
Solves copy the prepared RHS into their allocating or supplied destination on each
invocation. A reused destination/workspace has already been warmed. The single-RHS
matrix path selects vector triangular kernels. `candidateBatches` performs **16**
lower solves, and `fittingRound` performs **25** distinct covariance factorizations
plus solves/log determinants. Their reported time/allocation is per entire batch.

Rank-update comparisons include the ownership copy of the original factor.
Refactorization includes copying the precision matrix and adding the outer product.
The posterior workload starts fresh each invocation and performs **eight** bounded
update/solve rounds, including an information-vector copy; the refactor alternative
also copies the initial precision matrix. Conditioning does not drift between
samples. Extension and full refactorization use blocks of the same precomputed
enlarged covariance with identical kernel/noise/jitter. `factorCopy` separately
measures copying the original factor, which is smaller than an extension's result.

## Allocation sources

The scalar order-128 RBF factorization decreases from approximately **410425 to
131273 B/op** with a reused workspace. The latter still owns a 131072-byte double
payload. At order 512 it decreases from about **8783739 to 7537357 B/op**; the
eight-retained-length pool cannot keep every panel/trailing/backend shape. Matrix
solve `512x64` drops from approximately **1573386 to 33 B/op**, with similar timing
in the final capture. On SIMD the same solve drops from **1606163 to 10 B/op**.
These are measured values, not allocation-free API guarantees.

[Caller payloads](caller-payloads.csv) separately calculate the owned factor,
panel-copy, and trailing-copy double payloads, excluding headers and wrappers.
For order 128 they are 131072, 49152, and 114688 bytes. For order 512 they are
2097152, 917504, and 4587520 bytes. Remaining measured allocation includes backend
packing/workspace and object overhead. Pooling removes allocations, not copy work.

[JFR allocation-site samples](jfr/allocation-sites.csv) and compressed raw
`jdk.ObjectAllocationSample` events in [jfr/](jfr/) retain stack evidence of caller
block copies, owned results, and backend scratch. Categories describe double-array
samples only. Sample weights are **not exact allocation shares or B/op**; especially
large/short-lived arrays can be unevenly represented. Use GC-normalized B/op for
totals. JFR instrumentation timings are separate from the GC-only comparisons.
The JMH JFR profiler overwrites a method/parameter recording across forks, so the
retained recording is from the last fork, while the main GC-only reports retain
both forks. `recordings.json` identifies original JFR hashes and exported-event
hashes. `python3 jfr/analyze.py` regenerates the attribution table from those
exports. Original recordings remain available through the corresponding Lab job.
The Lab CLI's ordinary fetch failed on literal `%3A` directory names; retrieving
the manifest path with `%` replaced by `%25` through `lab cat` recovered each
recording, with file lengths and SHA-256 hashes verified before export.

## Accuracy and consumer validation

Final local quality passes with scalar and a verified four-lane SIMD engine.
The final Lab solve capture also passes the same checks with scalar and verified
eight-lane SIMD, including the relative forward-error check at every scale.
`accuracy.csv` distinguishes the local and Lab summaries; the table below is local.
Each backend checks 54 factor fixtures, 12 eight-update sequences, 108 extensions,
and three analytical diagonal determinants. Scales span `1e-100`, `1`, `1e100`;
block boundaries are 63/64/65 and 127/128/129. Full per-fixture outputs are in
`logs/final-quality/`; Lab quality accompanies each timing capture.

| Check | Scalar maximum | SIMD maximum | Acceptance |
| --- | ---: | ---: | --- |
| Factor reconstruction | 1.36e-15 | 1.36e-15 | < 1e-13 |
| Factor solve backward error | 3.59e-16 | 3.82e-16 | < 1e-13 |
| Update relative solution error | 7.48e-14 | 7.05e-14 | < 1e-10 |
| Update absolute log determinant delta | 5.69e-14 | 1.14e-13 | < 1e-9 |
| Extension relative solution error | 2.18e-9 | 3.24e-9 | Per-fixture bound |
| Extension absolute log determinant delta | 3.85e-9 | 4.43e-9 | Per-fixture bound |
| Analytical diagonal log determinant error | 1.82e-11 | 1.82e-11 | <= 1e-10 |

Update/extension reconstruction and backward errors also remain below `1e-13`.
Forward error is the infinity norm of the solution difference divided by the
reference solution's infinity norm. Extension's conservative bound is
`64 * epsilon * enlargedOrder / noiseVariance`, approximately `9.1e-7`–`2.1e-6`
here, reflecting the unit-signal covariance conditioning envelope. Log determinant
deltas are absolute. Low residuals alone do not imply small forward error on
ill-conditioned kernels, so both are checked against full refactorization.

Caller retry fixtures separately record ordinary success (one attempt, shift 0),
retry success (four attempts, last shift `1e-8`), and exhaustion (four attempts,
last shift `1e-8`). No retry logic or diagonal shift is introduced in the public API.

Combo at `f0674ecf` passes `GaussianProcessSurrogateTest`, including posterior mean,
variance, joint covariance, duplicate inputs, and jitter behavior. Its existing
frozen comparison runs four objectives at histories 16/64 with seed 401 and 4096
queries. Against its published Kumulant JVM dependency, the candidate's 16 model
rows agree exactly; its 32 search rows differ by at most `2.4755e-15`, with
nonnumeric fields agreeing. This is correctness evidence, not a full GP latency claim.

The [consumer directory](consumer/) retains both pairs of CSVs, validation output,
and explicit platform-artifact substitution. A plain composite build does not
replace Combo's pinned `kumulant-jvm` dependency. From the recorded Combo revision:

```sh
./gradlew :combo:jvmTest --tests '*GaussianProcessSurrogateTest' :combo:compareGp \
  -Pgp.mode=frozen -Pgp.seeds=401 \
  --init-script /path/to/candidate-substitution.init.gradle \
  -Dcholesky.candidate=/path/to/kumulant
```

## Follow-up and acceptance

The [published-snapshot view comparison](views/README.md) completes the planned
copy-versus-view evaluation on the same seeded fixtures and dependency cohort.
JVM uses borrowed trailing regions; Native retains contiguous copied blocks after
the view trial measured slower. This is a platform storage-access choice within
the shared numerical implementation.

The [acceptance audit](completion/README.md) maps the issue requirements to retained
timing, allocation, accuracy, and consumer evidence, and distinguishes optional
future optimization from acceptance work. The original cohort above retains its
own revisions, dependency pins, controls, and timing qualifications.
