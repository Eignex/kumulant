# Cholesky acceptance audit

The shared Cholesky work in [#139](https://github.com/Eignex/kumulant/issues/139)
is implemented by [#142](https://github.com/Eignex/kumulant/pull/142) and
[#163](https://github.com/Eignex/kumulant/pull/163). The following table maps the
requested measurements and acceptance checks to retained evidence.

| Requirement | Evidence |
| --- | --- |
| Dedicated factorization, vector/matrix/lower solves, and log determinant timings | [Original measurements](../measurements.csv), [tables](../tables.md), and filtered commands in the [benchmark guide](../../../../README.md) |
| Regression rank updates versus refactorization, bounded posterior update/solve sequences | [Alternative comparisons](../alternatives.csv), including ownership and reset/copy costs |
| GP extensions by 1/4/16 versus full refactorization | [Alternative comparisons](../alternatives.csv) and [extension accuracy](../accuracy.csv) |
| Allocating versus destination/workspace APIs, JVM B/op and GC | [Original measurements](../measurements.csv), [caller payload accounting](../caller-payloads.csv), and [sampled allocation stacks](../jfr/allocation-sites.csv) |
| Repeated candidate batches, covariance construction, factor/solve/logdet, and 25-factor fitting rounds | [Original measurements](../measurements.csv), with [matched fitting-round view comparisons](../views/before-after.csv) |
| Same seeded fixtures, dependencies, hardware, backend, warmup, and repetitions | [JVM view provenance](../views/provenance.json), [backend-identified Native pairing](../native-verified/runtime.json), [dependency pins](../views/dependency-cohort.json), and raw reports |
| Compare copying and borrowed matrix regions after Koblas publication | [88 matched comparisons](../views/before-after.csv); select views on JVM and copying on Native from measured outcomes |
| Numerical accuracy at block boundaries, extreme scales, and clustered RBF/Matérn inputs | [Accuracy tables](../views/accuracy.csv) and [quality logs](../views/validation/) |
| Update/extension equivalence and analytical log determinants | [Original accuracy results](../accuracy.csv) and the shared quality fixtures |
| Caller success/retry/exhaustion with recorded diagonal shift | [Original quality output](../logs/final-quality/) and `CholeskyRetryBenchmark` measurements |
| Ownership, selected triangle, empty shapes, aliases, failure contracts, JVM/Linux Native tests, ABI | [Full validation gate](../views/validation/full-gate.txt) |
| Consumer posterior mean/variance and model policy remain in Combo | [GP surrogate test and frozen-history comparison](../views/validation/combo.txt), [consumer parity](../views/consumer-parity.csv) |

The evaluation uses representative combinations, as requested, rather than every
possible size/column/kernel combination. The opt-in order-2048 stress case is not
part of the acceptance capture. Full GP fitting/acquisition latency stays in
Combo. Native allocation profiling is unsupported by the existing harness.

## Native follow-up

The prepared Native runner includes a release-mode numerical preflight, sharing
JVM's factor, update, extension, determinant, and caller-retry checks. It records
the actual selected engine and any available vendor library/version/thread
evidence before the generated benchmark engine starts. Shape-specific route
reports describe the first contiguous and borrowed trailing update at three sizes;
they inspect dispatch and do not measure those kernels independently.

Historical Native captures lack exact backend identity. A new capture cannot
retroactively identify their executing library, so their original qualification
remains. The matched large-factor regression supports retaining contiguous Native
scratch without claiming a particular vendor kernel caused that regression.

[Lab capture 8634](lab/job.json.gz) completes the prepared path end to end at
source `d40dff5`, with zero exit status. It runs on a Xeon Platinum 8488C with a
one-CPU/4608-MiB cgroup budget, JDK 25.0.4.1, and the same pinned Koblas cohort
as the published view comparison. The preflight and all eight benchmark processes
identify OpenBLAS 0.3.26 at `/lib/x86_64-linux-gnu/libopenblas.so.0`, with one
compute thread confirmed. Preflight dispatch descriptions select `cblas_dsyrk`;
the borrowed-shape description also records `host-stage/region`.

The [Native accuracy table](accuracy.csv) passes all the shared fixtures. Maximum
factor reconstruction/backward errors are `1.36e-15`/`3.51e-16`; update relative
solution error is at most `1.17e-13`. Extension relative solution error is at most
`4.10e-9`, within the per-fixture conditioning-aware bounds. The analytical log
determinant error is at most `1.82e-11`. [Raw preflight output](native-quality.txt.gz)
retains individual errors and tolerances.

[Timing rows](measurements.csv), compressed raw engine reports, configuration,
invocation logs, [host](host.txt), and [Lab resource receipts](lab/nomad/) retain two
process repetitions of allocating/workspace factors at precision order 32 and RBF
order 512. These are supplemental integration measurements without a matched
baseline; they establish no speedup and cannot identify the backend of historical
captures. The CPU budget is not an exclusive core. The JVM driver's JFR recording
does not profile Native child allocations; Native B/op remains unavailable.

Run `python3 analyze.py` to validate the Lab job/exit/source, receipt hashes,
per-process backend consistency, numerical tolerances, and ordinary three/five
one-second engine settings, then regenerate the Native CSVs. The
[retained-file inventory](lab/inventory.json) includes source byte counts and
SHA-256 digests. The preparation archive and driver's JFR remain in Lab rather
than duplicating those binary artifacts in Git.

The [backend-identified Native comparison](../native-verified/README.md) supplies
the missing paired attribution: both copying and view arms complete on the same
Lab host with identical OpenBLAS/library/thread/resource settings. All 48
measurement processes match their preflight receipts, and 24 matched comparisons
retain both repetitions and the unchanged small control. This independent
follow-up confirms the selected Native copying route while retaining the
historical capture's limitations.

The [separate desktop integration run](local/) validates all 54 factor fixtures,
12 bounded update sequences, 108 extensions, three analytical determinants, and
three caller-retry outcomes. It then completes eight timing rows: two process
repetitions of allocating/workspace factorization at precision order 32 and RBF
order 512, with the ordinary three/five one-second warmup/measurement settings.
Each benchmark process reports the same oneMKL selection as the preflight, with
one compute thread confirmed. These desktop timings validate the prepared path;
they do not extend the matched Lab comparison or establish a speedup.

`python3 analyze.py --local` checks those raw files and regenerates the desktop
accuracy/timing summaries. The [full gate](validation/full-gate.txt.gz) and JVM
[scalar](validation/scalar-quality.txt.gz)/[SIMD](validation/simd-quality.txt.gz)
checks also pass for the follow-up source.

## Further optimization

Native borrowed-region packing, backend scratch allocation, and tuning for other
hardware remain possible performance work. They are not unimplemented acceptance
criteria: the measured slower Native view trial was rejected. The [closure audit](../native-verified/README.md#closure-audit) identifies no
remaining requested numerical feature or measurement. The selected Cholesky
API and numerical implementation are unchanged from #163.
