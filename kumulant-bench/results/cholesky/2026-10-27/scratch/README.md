# Call-local Cholesky scratch, 2026-10-27

This capture measures one change. When a call above one block (order 64) gets no workspace, it now
creates a workspace that lives only for that call. The calls are allocating factorization,
`solve(DenseMatrix)`, `solveLower` and `extended`. Up to one block nothing changes. A caller workspace is
used as before. #139 is completed background. Owned results, alias rules, lower-triangle selection,
failures and jitter are unchanged.

## Attribution

The [published views capture](../../2026-10-26/views/README.md) already showed this for JVM: a factor
with a workspace allocates only its owned factor (`n² · 8` bytes). The allocating path also paid for
trailing `syrk` scratch at every block step. Allocating solves paid for `trsm` window scratch at every
diagonal step, and `solve` paid for it twice. Koblas lends by exact length and keeps up to eight idle
lengths. So one workspace per call reuses every repeating length and drops all of it when the call
returns. Some scratch remains: the last block steps borrow lengths that do not repeat. Native B/op is
not measured by the harness, so this capture attributes allocation on JVM only.

## Sources and protocol

- Baseline: `dc1fa17e15d436694f9a124ca37ccb605ac19aec` (main).
- Candidate: `9289a82455133930d194fb76e61aaaeb36401c54`.
- The two refs differ only in `Cholesky.kt` and `CholeskyFactor.kt`, plus Cholesky tests that do not
  run here. Later commits on the branch change only tests and this report.
- Both arms use the same Koblas dependency pin, fixtures (seed 139), benchmark methods and Lab entry
  point (`CholeskyLabProfileKt`).

Lab jobs [8718](lab/8718/) and [8719](lab/8719/) each run the JVM accuracy preflight and then JMH
with `-prof gc`, for scalar and then verified eight-lane SIMD. JMH uses two forks, three one-second
warmups and five one-second measurements.

Both jobs ended with exit status 0 on the same Lab host:

- host `fb6b7800-3ab3-e237-468e-4b1ea2dd24b5`, a Xeon Platinum 8375C;
- JDK 25.0.4.1;
- a hard cgroup-v2 budget of one CPU and 4608 MiB, with no OOM events.

`python3 analyze.py` checks those pairings and refuses to write tables unless they hold. It then
regenerates [before-after.csv](before-after.csv) and [tables.md](tables.md) from the
[raw JMH JSON](raw/). The CSV keeps both arms' score errors, sample standard deviations, B/op and GC
counts. The [logs](logs/) keep each run's command, benchmark output and accuracy preflight.

## Findings

Allocation per operation falls wherever a call repeats scratch lengths, and it matches across
backends:

| operation | scalar B/op | SIMD B/op | owned result |
|---|---|---|---|
| allocating factor, order 512 | 3278634 → 2425943 | 3281259 → 2426264 | 2097168 |
| allocating factor, order 1024 | 11145353 → 8718445 | 11150783 → 8718415 | 8388624 |
| `solve`, 512x64 | 1573402 → 393631 | 1606167 → 409998 | 262160 |
| `solve`, 1024x256 | 7931465 → 2327552 | 7963858 → 2343453 | 2097168 |
| `solveLower`, 1024x256 | 5014323 → 2327243 | 5030527 → 2343396 | 2097168 |
| `extended`, 1024x16 | 9892675 → 8925867 | 9901249 → 8934441 | 8652928 |
| allocating 25-factor fitting round, history 512 | 82068661 → 60751272 | 82134221 → 60759205 | 52428800 factors |

Some paths are unchanged within profiler residue:

- every workspace path;
- `rankUpdated`;
- shapes at order 64 or below (8248, 296 and 2584 B/op);
- order-128 factors.

Some order-128 operations allocate 280 B more, which is the empty workspace: `solveLower` 128x16 and
`extended` 128x16. At order 128, `solve` still drops from 98409 to 57697 B/op on scalar and from 114824
to 65904 on SIMD, because its two substitutions share scratch.

**Throughput.** All 72 matched timing rows (ten samples per arm) have overlapping 99.9% intervals. Candidate over
baseline ratios range from 0.90 to 1.08 and go both ways. This includes the unchanged
`factorWithWorkspace` and `solveMatrixInto` controls, which range from 0.90 to 1.03. So the capture
shows **no measurable throughput change in either direction**. It establishes an allocation reduction,
not a speedup. GC counts are recorded but not interpreted separately.

## Numerical checks

Both arms pass the shared accuracy preflight that the profile requires before timing. It covers 54
factors, update sequences, extensions, determinants and caller retries. The scalar preflight output is
byte-identical between the arms. The SIMD output differs only in last-place diagnostics.

On JVM, `trsm` results can vary in the last place across JIT tiers even with no workspace. A 129x3
transposed solve differed bitwise in 276 of 300 repeats on the baseline code path. For this reason the
new unit test compares allocating and destination solves within `1e-12` and not bitwise.

## Native

Native jobs 8720 (baseline) and 8721 (candidate) ran on **different hosts**: 4fb55f46 and 24fb6d22.
They are not a matched pair, so this report makes **no Native throughput claim**. The Native harness
does not measure B/op.

Native trailing staging borrows a different length at every block step, and it is unchanged here. With
a call-local workspace, up to eight idle lengths of those stale staging buffers can stay reachable until
the call returns. A simulation of the pool estimates this at roughly 4 MB at order 512 and 15 MB at
order 1024. They were previously collectable after each step. A caller-supplied workspace already
behaves this way today. Changing the staging is outside this change.
