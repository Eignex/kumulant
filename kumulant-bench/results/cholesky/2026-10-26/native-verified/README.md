# Backend-identified Native Cholesky comparison

This follow-up completes the Native backend evidence for
[#139](https://github.com/Eignex/kumulant/issues/139), stacked on
[#166](https://github.com/Eignex/kumulant/pull/166). The earlier
[matched view comparison](../views/README.md) retains its original samples and
backend-identification qualification. These captures are independent evidence;
they do not assign a backend retroactively to earlier measurements.

## Sources and measurements

The copying arm is `f040bc708fd15f835380a96d5b76d6c8cc4e8692`; the borrowed-region
trial is `f2a93f69df90c1dd35b830fe6e220790cbeda309`. Their only source difference is
[Native trailing storage access](trial.patch). The trial body exactly matches the
JVM view implementation. Shared numerical code, public API, fixtures, benchmark
methods, dependency declarations, and profiling/accuracy runners are identical.
Both use the [published Koblas cohort](../views/dependency-cohort.json).

The [Lab definitions](lab/) select allocating/workspace-backed factorization at
precision orders 32/128 and RBF orders 512/1024, plus factor/solve/log determinant
and 25-factorization fitting rounds at history size 512. Fixture seed is 139;
noise variance is `1e-6` and explicit jitter is `1e-10`. Covariance construction
stays outside these kernel/workload timings. Each factor owns its result; fitting
matrices are distinct precomputed covariances. Order 32 follows identical unblocked
code in both sources and supplies a control.

Each arm runs the shared Native release accuracy preflight, followed by two
process repetitions with three one-second warmups and five one-second measurement
iterations. The existing generated kotlinx-benchmark engine owns timing, samples,
and uncertainty. Every benchmark runs in its own process and records its selected
backend/library during setup, outside timing. Native B/op is unsupported.

Run `python3 analyze.py` to verify job completion/source/exit codes, retained-file
hashes, identical definitions, backend/library identity in every measurement
process, visible hardware, resource/JDK settings, numerical acceptance, fixture
coverage, and raw sample counts before regenerating the tables. Analysis refuses
to publish a matched table if those pairing checks fail. Engine errors describe
each process; cross-process/host drift can exceed them. The CPU budget does not
imply an exclusive pinned core.

## Findings and production decision

Both accepted captures finish with zero exit status on the **same Lab host**,
Xeon Platinum 8488C, and identical one-CPU/4608-MiB cgroup budgets. The preflight
and all 48 measurement processes identify **OpenBLAS 0.3.26** at
`/lib/x86_64-linux-gnu/libopenblas.so.0`, with one compute thread confirmed.
[Runtime receipts](runtime.json) retain the selected composition, library version,
host identity, JDK, and resource settings. Both source preflights report region
staging for the hypothetical borrowed `syrk` call. That dispatch description is
separate from end-to-end timing and does not isolate a packing kernel's cost.

The [24 matched comparisons](before-after.csv) and [readable tables](tables.md)
retain both process repetitions. Workspace-backed RBF order 512 takes
5.66/6.36 ms with copying versus 12.72/13.21 ms with views. At order 1024 the
corresponding means are 34.81/38.28 ms versus 84.18/88.70 ms. These rows measure
views 2.1–2.4 times slower across the two sizes and repetitions. Allocating
25-factor fitting rounds take 162.74/161.12 ms versus 313.84/321.40 ms; workspace
rounds take 162.97/153.26 ms versus 307.37/314.70 ms.

The unchanged order-32 control varies materially: allocating view/copy ratios are
0.98 and 1.56, while workspace ratios are 0.87 and 1.12. Process engine errors
alone do not explain all observed drift, even on the same host. All points remain
visible; no ratio is adjusted or normalized by the control. The repeated large
factor/workload results support the conservative choice to **retain Native copied
blocks**. The borrowed-region trial is not part of the selected library. JVM
continues to use the previously measured view implementation. There is no
universal timing or allocation claim, and no public API/ownership/jitter change.

Both arms pass the [shared numerical checks](accuracy.csv): 54 factors, 12 update
sequences, 108 extensions, three determinants, and three caller retry outcomes.
Maximum errors agree across these captures: factor reconstruction `1.36e-15`,
backward error `3.51e-16`, update relative solution error `1.17e-13`, and extension
relative solution error `4.10e-9`, within their respective acceptance bounds.

Each job recovered an infrastructure failure during preparation and a lost
measurement attempt. The retained job logs record these events. Only complete,
accepted attempts contribute timing rows; interrupted attempts contribute none.
Raw engine JSON, benchmark logs, definitions, source patches, and byte-count/hash
inventories remain alongside this report. Full prepared archives, per-case
protocol intermediates, and the JVM driver's JFR remain in Lab. Driver JFR does
not measure Native child allocation.

## Contract validation

The unchanged production library passes
`./gradlew check lintDocs --rerun-tasks`; all 63 tasks execute. The two Cholesky
unit suites contain **42 tests per platform**: 20 in `CholeskyTest` and 22 in
`CholeskyFactorTest`. Retained [JVM](validation/jvmTest-contracts.json),
[Linux Native](validation/linuxX64Test-contracts.json), and
[scalar JVM](validation/scalar-contracts.json) results include individual names
and outcomes. The separate Native view trial also passes all
[42 Native contract tests](validation/view-native-tests.json) and the shared
[local numerical preflight](validation/view-local-quality.txt.gz).

These unit tests cover input/result ownership, independent and aliased solve
storage, lower-triangle selection, empty shapes, shape/nonfinite errors, strict
pivot failures, reusable scratch after failure, update/extension ownership, and
extension equivalence across block boundaries. The **54 factor accuracy fixtures**
are a separate workload from the 42 unit tests. Shared accuracy also checks 12
bounded update sequences, 108 extensions, three analytical determinants, and
three caller-retry outcomes, with the existing scale/conditioning-aware limits.

The existing [JVM scalar/SIMD quality checks](../completion/validation/) and
[Combo posterior/frozen-history validation](../views/validation/combo.txt) remain
applicable: this follow-up's selected production library and consumer API match
#163, while #166 adds profiling diagnostics and Native numerical validation.

## Closure audit

The [requirement-to-evidence audit](../completion/README.md) covers the original
baseline and allocating/reusable solves; bounded update/refactor and extension
comparisons; separate covariance construction and lifecycle measurements; candidate
batches and 25-factor fitting rounds; JVM time/allocation/GC and raw samples;
scaled/clustered numerical diagnostics; caller-controlled retry shifts; ownership,
triangle, shape, and failure contracts; ABI/full gates; and Combo posterior
mean/variance plus frozen-history parity.

The [published JVM comparison](../views/README.md) and this backend-identified
Native pair complete the copying/view evaluation with measured platform choices.
No requested numerical feature or measurement remains unimplemented. Optional
order-2048 stress and further platform/kernel tuning remain outside the required
acceptance run. Merging the validation/report stack is sufficient to close #139.

The stack is rebased onto main after the independent Skema serialization change
in #162. The [fresh full gate](validation/rebased-full-gate.txt.gz) and parent CI
pass on that base. Cholesky source files and Koblas pins are unchanged; the paired
performance reports retain their original pinned source/dependency cohort rather
than being relabeled as measurements of the rebased head.
