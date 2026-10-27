# Order-2048 Cholesky stress and hardware scaling

This optional follow-up to the completed [Cholesky evaluation](../../2026-10-26/README.md)
adds an opt-in order-2048 numerical stress check and profiles the existing benchmark
harness on every Lab hardware pool the profile interface could place. It is a
**scaling study and hardware inventory**, not a before/after source comparison: no
Cholesky kernel, pool policy, routing, backend selection or public API changed.

## Sources, fixtures and protocol

The x86 captures measure `ca3200444c0c7db9dc7fdcd35c84d797b8862c78`. The arm64 captures
measure `28030b60dfeb92c6cfeaea3d4e492c51b53a1a26`, which adds README text and skips the
kotlinx-benchmark `linuxX64` target on Linux arm64 hosts. Benchmark, fixture and numerical
code are identical between them. Both use the strict Koblas/Skema platform pins in
`kumulant/build.gradle.kts` ([provenance](provenance.json)).

Every capture ran through `CholeskyLabProfileKt` with `--stress-order=2048` (the
[definitions](lab/) retain the exact arguments). JVM captures run scalar and then SIMD
in one Lab case. Each backend runs the shared quality preflight and the order-2048 stress
check, then JMH with three one-second warmups, five one-second measurements, two forks,
a 512 MiB heap and the GC profiler. The Native capture uses the prepared release harness:
the shared preflight, the Native stress check, then two process repetitions with the same
warmup/measurement protocol and per-process backend/library receipts. Fixture seed is 139,
noise variance `1e-6`, explicit jitter `1e-10`. Covariance construction is measured
separately and stays outside the factor/solve timings.

Selected cases keep stress apart from controls:

- **Stress:** RBF/Matérn order-2048 factors with and without a workspace, solves of
  2048×1 and 2048×256 right-hand sides (allocating and into a caller destination), and
  order-2048 covariance construction plus factor/solve/log-determinant evaluations.
- **Controls and scaling points:** the unblocked precision order-32 factor, RBF orders
  512/1024, 32×1 and 1024×256 solves, and history size 512 evaluations.

## Memory accounting

An order-2048 matrix occupies 33,554,432 bytes, and so does each owned factor. Each
factor benchmark holds one input matrix and allocates its owned result per call. Solve
benchmarks hold one factor, the right-hand side and a destination (4 MiB each at
2048×256). Fitting rounds hold 25 distinct covariances, 800 MiB at order 2048, which
exceeds the 512 MiB harness heap. `CholeskyWorkloadBenchmark` therefore builds them only
up to history size 512; larger sizes measure single evaluations and reject
`fittingRound`. The stress check releases each fixture before the next and peaked at
470 MB RSS for the local SIMD JVM run under `-Xmx512m`, and 765 MB for local Native
([local checks](validation/)). No Lab case recorded a cgroup `max`, OOM or OOM-kill
event under the 4608 MiB limit.

JMH B/op separates the caller-owned result from everything else
([measurements](measurements.csv), `owned_result_bytes` and `other_bytes_per_op`).
Workspace-backed order-2048 factors allocate the 33.554 MB owned factor plus 3–5 KB on
every JVM host, so the bounded pool retains order-2048 scratch. Allocating factors add
5.9 MB of backend scratch. Allocating 2048×256 solves allocate the 4.19 MB result plus
12.1 MB of temporary copies and packing; `solveMatrixInto` allocates at most 3 KB.
Native has no allocation profiler, and the Lab driver's JFR does not measure JMH forks
or Native children.

## Hardware and backends profiled

[Runtime receipts](runtime.json) retain the Lab host, `lscpu` identity, resolved
backend strings, cgroup limits (one CPU via `cpu.max 100000 100000`, 4608 MiB) and
load average at start. One CPU of quota is not an exclusive pinned core. Other tenants
may share a 16-vCPU node.

| Capture | Job | CPU | JVM SIMD backend | Native backend |
| --- | ---: | --- | --- | --- |
| avx2-jvm | 8709 | AMD EPYC 7R32 (AVX2) | `simd(4 lanes)`, tile 8×4 | |
| avx2-native | 8738 | same Lab host as 8709 | | OpenBLAS 0.3.26 Zen level-1, scalar tiles, 1 thread confirmed |
| sve2-jvm | 8722 | Neoverse-V2 (SVE2) | `simd(2 lanes)`, tile 4×4 | |
| sve-jvm | 8723 | Neoverse-V1 (SVE) | `simd(4 lanes)`, tile 8×4 | |
| neon-jvm | 8724 | Neoverse-N1 (NEON) | `simd(2 lanes)`, tile 4×4 | |

Every JVM capture also ran the scalar backend. The Vector API's preferred species picks
two lanes on Neoverse-V2: its SVE2 vectors are 128 bits, while Neoverse-V1 has 256-bit SVE.

## Findings

[Tables](tables.md) list every case with engine error, raw-sample range in
[measurements.csv](measurements.csv), owned/other allocation and GC counts.

Selected order-2048 results, in milliseconds (mean ± engine error):

| Capture | Backend | RBF factor | RBF factor, workspace | Solve 2048×256 | Solve into 2048×256 |
| --- | --- | ---: | ---: | ---: | ---: |
| avx2-jvm | scalar | 484.9 ± 3.3 | 484.8 ± 4.4 | 422.8 ± 3.8 | 421.6 ± 2.9 |
| avx2-jvm | SIMD | 139.6 ± 7.4 | 140.8 ± 12.1 | 104.5 ± 3.7 | 103.0 ± 3.7 |
| avx2-native | process 0 / 1 | 397.5 / 395.8 | 396.1 / 395.8 | 60.5 / 61.3 | 58.3 / 58.1 |
| sve2-jvm | scalar | 347.5 ± 3.3 | 342.6 ± 2.1 | 282.2 ± 5.2 | 279.6 ± 5.5 |
| sve2-jvm | SIMD | 198.2 ± 4.9 | 200.0 ± 5.9 | 147.9 ± 3.6 | 147.9 ± 3.8 |
| sve-jvm | scalar | 453.9 ± 3.6 | 443.1 ± 3.7 | 361.7 ± 2.8 | 355.5 ± 1.1 |
| sve-jvm | SIMD | 173.0 ± 6.7 | 173.7 ± 7.9 | 114.8 ± 3.8 | 114.1 ± 3.9 |
| neon-jvm | scalar | 811.5 ± 9.4 | 802.4 ± 23.4 | 637.1 ± 10.5 | 636.7 ± 15.6 |
| neon-jvm | SIMD | 428.1 ± 4.9 | 430.1 ± 15.2 | 334.6 ± 16.8 | 327.5 ± 19.3 |

Within-host observations, which share hardware, dependencies and protocol:

- At order 2048 the workspace does not change factor time beyond engine error on any
  host; its benefit is removing 5.9 MB of scratch allocation per factor. Matérn and RBF
  factors cost the same within 2.5%.
- Going from order 1024 to 2048 multiplies factor time by 6.9–7.6 for scalar and Native,
  and by 5.7–6.8 for SIMD, below the cubic factor of 8. Order-1024 SIMD points carry
  proportionally more fixed overhead.
- On the AVX2 host, JVM SIMD factors order 2048 2.8 times faster than the Native
  OpenBLAS-level-1/scalar-tile route. Native 2048×256 solves are 1.7 times faster than
  JVM SIMD. This is a platform observation on identical hardware; it does not alter the
  measured copied-block Native choice, which was a same-platform comparison.
- Native covariance construction takes 204.5 ms at order 2048 versus 31–32 ms on JVM
  on the same host. This cost is in fixture construction and stays outside the
  Cholesky timings.
- The order-32 control does not vary by backend on any JVM host. Native's order-32
  control is 0.019 ms in both processes.

Across hosts, the rows are a descriptive inventory: instance types, co-tenancy and
turbo behavior differ, so they support no hardware ranking or production backend
switch.

## Numerical stress

All 54 stress rows pass ([stress.csv](stress.csv)), as do the shared preflights:
54 factors, 12 update sequences, 108 extensions, three determinants and three retry
outcomes per backend ([accuracy.csv](accuracy.csv)). At order 2048, the maximum relative
reconstruction error is `5.55e-15` and the maximum solve backward error `8.75e-16`, both
under the `1e-13` limit. Allocating, destination and aliased solves have identical
backward errors. Workspace and allocating factors are bitwise identical on every
platform. Input matrices and right-hand sides are unchanged, and a factor's solves are
unaffected after its exported lower factor is overwritten.

Order-2032+16 extensions reach relative solution error `9.65e-9` and log-determinant
error `2.57e-9`. Eight rank updates reach `1.62e-6` and `1.17e-7`. All are within the
conservative kernel bound `64 * epsilon * order / noiseVariance = 2.91e-5`; the update
margin is about 18×. Scalar and 4-lane SIMD JVM stress results on the Lab AVX2 host
match the local desktop bit for bit.

The stress oracles traverse column-major storage for speed. Each entry still sums in
ascending index order, and a scratch A/B over 72 quality fixtures shows bitwise-identical
reconstruction and backward errors on scalar and SIMD
([oracle-equivalence](validation/oracle-equivalence.txt)).

## Attempts, limitations and inventory

- Jobs 8710–8712 (arm64 at `ca32004`) failed during Gradle configuration with
  `Unknown host target: linux aarch64`. kotlinx-benchmark resolves the Kotlin/Native host
  while wiring Native tasks. They produced no measurements; their definitions and job
  records are under [lab/failed](lab/failed/). Jobs 8722–8724 reran at `28030b6`.
- 8722 lost its first case attempt and 8723 its first preparation attempt to
  infrastructure. Only committed attempts contribute.
- The Lab's local node (macOS arm64 with SME2) was excluded. The profile records
  hardware with `lscpu`, and the benchmark Native target is Linux x64 only. No macOS
  numbers are claimed.
- Native timing exists only for Linux x86_64 (AVX2 pool). The AVX-512 pool was
  unavailable for most of the session because of EC2 spot capacity and a Lab admission
  backlog. Job 8708 (AVX-512 JVM) is recorded below when complete. No AVX-512 Native
  capture was launched.
- Every capture is a single Lab job. Drift between hosts or jobs exceeds engine error, as
  the 2026-10-26 controls showed.

Retained per capture: the definition, job/case records, logs, Nomad resource receipts,
the `lscpu` host, quality/stress output, raw JMH/Native JSON, benchmark logs and Native
configuration/samples. A byte/hash [inventory](lab/) covers each. Preparation archives
and the driver JFR remain in Lab. To regenerate, run `python3 -I retain.py <label>
<fetched job> <job.json> <cases.jsonl>` per `lab fetch`, then `python3 analyze.py`. The
analysis verifies job status/SHA/exit code, hashes, backend identity per process, resource
limits, accuracy/stress bounds, case coverage and sample counts before writing tables.

Local validation: `./gradlew check lintDocs --rerun-tasks --max-workers=2` executed all
63 tasks successfully ([log](validation/full-gate.txt.gz)). Simulating an arm64 host with
`./gradlew -Dos.arch=aarch64 --no-configuration-cache :kumulant-bench:help` fails without the
host guard and succeeds with it.
