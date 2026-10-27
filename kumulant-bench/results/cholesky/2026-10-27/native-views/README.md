# Native Cholesky views with in-place host regions

This completes [#179](https://github.com/Eignex/kumulant/issues/179). Native Cholesky uses
borrowed trailing regions with the pinned Koblas cohort below. All 16 required factor and
GP-shaped timing rows favor views in both process repetitions. The public ownership,
destination/workspace, lower-triangle, strict-failure and caller-jitter contracts are unchanged.

The [earlier comparison](../../2026-10-26/native-verified/README.md) favored copying because
the host adapter packed borrowed regions element by element. Koblas
[#647](https://github.com/Eignex/koblas/pull/647) passes their offset and leading dimension
directly to Native host BLAS. The [packing diagnosis](../native-packing/README.md) retains
its conformance checks. This comparison measures the published fix end to end.

## Sources and publication

Both arms start from main `974eee9`, including call-local scratch reuse. Copying measures
`02d1f61fdbc2615bab0826cc766dc5d03b20e54e`; views measure
`4d740908a9cd3a95d35abbec2a67ba5be4510e6c`. They differ only in the Native trailing update:
copying contiguous panel/trailing blocks versus passing disjoint views of the owned factor.
The production Native implementation has the same contents as the measured view implementation.

Both use these strict root/platform pins:

| Artifact | Version |
| --- | --- |
| koblas | 0.1.1-20261027.124722-275 |
| koblas-jvm | 0.1.1-20261027.124722-278 |
| koblas-linuxx64 | 0.1.1-20261027.124722-274 |
| koblas-linuxarm64 | 0.1.1-20261027.124722-275 |
| koblas-macosarm64 | 0.1.1-20261027.124335-298 |

[Publication receipts](publication/) include Maven metadata, immutable artifact URLs and hashes,
and checksums for 16 relevant published source files compared byte for byte with merge `c2952c2d`.
The published revision `9b9d9dc9` differs from that merge only in benchmark files.
`python3 -I verify-publication.py` rechecks the published artifacts and recorded source hashes.
Each arm's `dependencies.txt` records the actual Gradle-selected root, JVM runtime and linuxX64
compilation coordinates; the analysis requires the pinned platform versions and identical selections.

## Protocol and pairing

Lab jobs 8775 (copying) and 8778 (views) both ended with exit status zero. Their measurement
intervals do not overlap, and they ran on the same host:

- Lab host `9e466ea1-7e65-bd2e-ae27-90033d7e6fc4`, Xeon Platinum 8375C, 16 visible vCPUs.
- Hard cgroup-v2 budget: one CPU (`cpu.max 100000 100000`) and 4608 MiB; no memory-limit/OOM events.
- OpenBLAS 0.3.26 SkylakeX, `/lib/x86_64-linux-gnu/libopenblas.so.0`, one thread confirmed.
- Driver JDK 25.0.4.1. The driver JFR does not profile Native children.

Both use the existing prepared release harness, seed 139 fixtures, and identical filters/parameters.
Each of 12 cases runs in its own Native process, with three one-second warmups and five one-second
measurement iterations, repeated in two process repetitions. The selected backend, library/version
and confirmed thread count are recorded outside timing in every process. The view preflight's
borrowed route has no `host-stage/region`.

Cases include allocating and workspace-backed factors at precision orders 32/128 and RBF orders
512/1024, plus history-512 allocating/destination evaluations and both 25-factor fitting rounds.
All use the existing covariance, solve and log-determinant fixtures. No alternate factorization,
GP implementation, smoke timer, or allocation profiler is introduced.

## Results and decision

[All 24 paired rows](tables.md), [raw scores](before-after.csv), and both arms' engine JSON and
measurement samples are retained. Ratios below one favor views:

| Required case | View/copy, repetition 0 | View/copy, repetition 1 |
| --- | ---: | ---: |
| Allocating RBF factor 512 | 0.626 | 0.616 |
| Workspace RBF factor 512 | 0.620 | 0.636 |
| Allocating RBF factor 1024 | 0.444 | 0.448 |
| Workspace RBF factor 1024 | 0.450 | 0.453 |
| Factor/solve/log-determinant 512 | 0.627 | 0.811 |
| Destination-backed evaluation 512 | 0.626 | 0.636 |
| Allocating fitting round 512 | 0.640 | 0.656 |
| Workspace fitting round 512 | 0.651 | 0.642 |

Order-1024 factors fall from roughly 54–55 ms to 24–25 ms. Fitting rounds fall from
229–232 ms to 147–152 ms. Every required row is faster in both repetitions; the least
favorable ratio is 0.811. The allocating evaluation varies more between view repetitions
(5.85 and 7.47 ms), and both values remain below their copying counterparts.

The unchanged order-32 control has ratios 0.962–1.044. No control-based normalization is
applied. The order-128 rows also favor views (0.869–0.968). Native engine errors and all
samples remain in the tables/JSON; they do not bound between-process or host drift.

The recorded acceptance rule requires all 16 required rows to be no slower with views.
[decision.json](decision.json) records that result, supporting the Native view route.
These are Linux x86_64/OpenBLAS observations on one shared host, with a quota rather than
an exclusive pinned core. No ARM/macOS timing, universal backend ranking, Native B/op,
or precise allocation reduction is claimed.

## Correctness and validation

Both arms pass the shared preflight: 54 factors, 12 update sequences, 108 extensions,
three analytical determinants and three caller-retry outcomes. [Accuracy summaries](accuracy.csv)
retain the observed errors and the original bounds. The largest factor reconstruction error
is `1.36e-15`, and factor backward error is at most `3.51e-16`; update and extension checks
also pass. The production change keeps owned factors/results and explicit per-invocation
workspaces; it adds no global strategy switch or hidden jitter.

Compilation, JVM/Native tests and the full gate run in GitHub CI for
[PR #181](https://github.com/Eignex/kumulant/pull/181). No local Gradle or library numerical
workload was run for this follow-up. Local analysis checks retained data and artifact hashes.

## Retention and replay

`python3 -I analyze.py` verifies sources, publication/retained-file hashes, actual dependency
coordinates, host identity, serial intervals, backend/library/thread evidence, resource limits,
numerical bounds and all measurement settings before regenerating tables and the decision.
Definitions, accepted job/case records, resource receipts and per-file inventories are under
[lab/](lab/). Preparation archives and driver recordings remain in Lab.

The four old source-composite jobs (8713–8716) never started measurement cases. Their
[supersession receipts](superseded/) record cancellation after checking their live state;
the published captures replace them. No other sessions' jobs were changed.

Use the public endpoint explicitly when replaying retained definitions:

```sh
LAB_URL=https://lab.eignex.com /home/rasmus/Workspaces/eignex-lab/deploy/lab run lab/copy/definition.json
# Let copying finish before submitting views; validate host identity before accepting the pair.
LAB_URL=https://lab.eignex.com /home/rasmus/Workspaces/eignex-lab/deploy/lab run lab/view/definition.json
```

After fetching each completed job, `retain.py` stores its outputs and byte/hash inventory.
The analysis rejects mismatched pairs rather than normalizing or combining incompatible captures.
