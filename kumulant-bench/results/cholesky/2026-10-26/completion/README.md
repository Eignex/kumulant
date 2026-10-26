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
| Same seeded fixtures, dependencies, hardware, backend, warmup, and repetitions | Each cohort's [original provenance](../provenance.json) or [view provenance](../views/provenance.json), [dependency pins](../views/dependency-cohort.json), and raw reports |
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

## Further optimization

Native borrowed-region packing, backend scratch allocation, and tuning for other
hardware remain possible performance work. They are not unimplemented acceptance
criteria: the measured slower Native view trial was rejected. There is no public
contract change or automatic jitter requirement remaining in this issue.
