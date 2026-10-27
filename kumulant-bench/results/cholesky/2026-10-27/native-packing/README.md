# Native borrowed-region packing follow-up

This follow-up investigates why the [backend-identified Native view trial](../../2026-10-26/native-verified/README.md)
from the completed [#139](https://github.com/Eignex/kumulant/issues/139) evaluation was slower than copying.
It records the Koblas fix and the downstream validation of that fix. **Production routing is unchanged:**
Native keeps copied trailing blocks and the JVM keeps views. Matched Lab timings for the fix are not
available; see [Measurement status](#measurement-status).

## Diagnosis

On Kotlin/Native the default Koblas engine composes the installed host BLAS. Before
[Eignex/koblas#647](https://github.com/Eignex/koblas/pull/647), an eligible borrowed `syrk` went through
`regionSyrk`, which did two element-by-element copies:

- It packed the panel and the selected triangle of the trailing block into workspace loans, one entry at a
  time through the interface `get`, with bounds checks on every entry.
- After the library call, it copied the triangle back through `set` in the same way.

Every entry of the panel and triangle costs an interface call and a bounds check, in both directions. The
copying arm instead moves whole columns with `copyInto`
and calls the contiguous `syrk`, which pins the arrays in place. Both arms reach the same vendor entry point.
The retained preflights show the difference as the `host-stage/region` route component on the borrowed call.
CBLAS already takes a pointer and a leading dimension, so the packing bought nothing.

## Koblas change

Koblas#647 gives the Native binding internal region entry points for `gemv`, `gemm`, `syrk`, `trsv` and
`trsm`. Each pins a region at its offset and passes its leading dimension. `HostDenseBlas` now sends an
eligible borrowed call there and snapshots only an input whose rectangle overlaps the destination. That is
the same rule the portable path uses. Bindings without the seam (the JVM FFM binding) still pack.
Transpose, triangle, unit-diagonal, zero-multiplier, empty-shape and alias behaviour is unchanged, and the
public `Blas` API is untouched. The measured candidate is Koblas `5eb646d1`; the PR head `6e7087fb` adds
only a test.

## Downstream validation

All runs below build Kumulant `dc1fa17` against Koblas `5eb646d1` source through `--include-build`.
[Resolution](validation/composite-resolution.txt) shows `com.eignex:koblas` replaced by the included
project in both the JVM runtime and the linuxX64 klib configurations. No `koblas-jvm` or `koblas-linuxx64`
snapshot coordinate is resolved. No published snapshot was used for this validation; this exact-source
composite is the evidence, and no dependency pin is changed.

- **Production copying:** all 42 Cholesky unit tests (20 `CholeskyTest`, 22 `CholeskyFactorTest`) pass on
  the [JVM and linuxX64](validation/copy-candidate/). The Native test binary passes under both installed
  vendors: oneMKL 2026.1 by default, and OpenBLAS 0.3.32 with `HOME` emptied. The
  [loaded libraries](validation/native-test-libraries.txt) were confirmed with `LD_DEBUG=libs`.
- **Native view trial:** this is the retained [trial patch](../../2026-10-26/native-verified/trial.patch),
  applied locally and not committed. It passes the same 42 Native tests under both vendors.
- **Shared Native accuracy preflight:** it passes for the view trial under
  [oneMKL](validation/view-candidate/quality-onemkl.txt.gz) and
  [OpenBLAS](validation/view-candidate/quality-openblas.txt.gz). Maximum factor reconstruction error is
  `1.36e-15` and maximum backward error is at most `3.48e-16`. The retained captures report `1.36e-15` and
  `3.51e-16`. The borrowed trailing route is now `host-dense+<vendor>/cblas_dsyrk`, with no
  `host-stage/region`.

These are local correctness checks on a shared desktop, not timings.

## Measurement status

Four matched Lab arms were defined from the retained Native definition. Each changes only the name, the
description and the Kumulant ref. All four reuse the same filter, fixtures (seed 139), node tags, one-CPU
budget, release-process repetitions, warmup and the prepared Native entry point. The definitions are in
[lab/](lab/).

| Job | Arm | Kumulant ref | Koblas source |
| --- | --- | --- | --- |
| 8713 | copying | `e50c7fa6` | `905bbc22` (before) |
| 8715 | views | `b4d25cac` | `905bbc22` (before) |
| 8714 | views | `98adc598` | `5eb646d1` (in place) |
| 8716 | copying | `c400e5ae` | `5eb646d1` (in place) |

The refs are measurement-only commits on the unmerged `codex/cholesky-native-packing-measure` branch.
Each adds the [pinned composite substitution](lab/composite-settings.gradle.kts) to the settings, and the
view arms add the trial patch. The substitution clones Koblas, checks out the named commit, and refuses to
build unless `HEAD` matches it and the tree is clean.

At 12:22 UTC on 2026-10-27 this report was finalized while all four jobs were still `RUNNING`. None had
committed a preparation or started a measurement case. Jobs 8713 and 8714 had lost four preparations,
8715 and 8716 three, each `lost:preparation` / `lost:infrastructure:failed` after roughly one to eight
minutes. The jobs were left active for continuation; their job IDs are above. During the same
period, other Kumulant profile jobs on the pool failed preparation at most once and then committed in about
6.5 minutes. The probable difference is that these arms also compile Koblas from source during preparation.
The Lab exposes no preparation log, so that cause is not confirmed. No timing row exists, and none is
inferred.

## Decision and remaining work

Native views stay disabled. The fix removes the copies that the diagnosis attributes the regression to, but
switching the production backend requires matched end-to-end timings, and none were captured.

Koblas#647 is merged. To continue, either consume jobs 8713–8716 if they complete, or pin a verified
published Koblas snapshot that contains the merge commit in place of the source composite, which removes
the extra preparation work. Then run the copying and view arms with this definition.

A continuation is acceptable evidence only when all of the following hold:

- Both arms finish with zero exit status on the same Lab host, with the same resource budget.
- Every measurement process records the same library, version and one confirmed thread.
- The preflight shows the borrowed route without `host-stage/region`.
- Both numerical preflights pass.
- Both process repetitions retain their raw samples.

Enable Native views only if the workspace-backed and allocating factor rows at RBF orders 512 and 1024,
and both fitting rounds, are no slower with views in both repetitions. The order-32 control is reported but
not used to normalize. Otherwise, keep copying and record the result as negative.
