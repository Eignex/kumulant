# Borrowed views: source validation before snapshot publication

Koblas [PR #644](https://github.com/Eignex/koblas/pull/644) merged as
`6ab06f70c80b2c67396f19f9d9ebb7b4660fb57f`. This local preview substitutes a clean
archive of that committed source into both the copying and view-backed Kumulant
builds. It uses no edits from the separate Koblas working checkout.

The copying baseline is Kumulant `008d778`; the view implementation is that revision
plus [compressed source patch](source.patch.gz). The public API, factor ownership, selected
triangle, strict failure policy, and allocating solve routes are unchanged. Only
the blocked trailing update borrows two destination regions for `syrk` in place of
extracting and writing back panels/blocks.

The [full gate](validation.txt) passes with `check lintDocs --rerun-tasks` and the
retained composite-build substitution, including Linux Native tests and ABI checks.
Both implementations also pass scalar and four-lane SIMD quality checks. Existing
tests cover block boundaries, poisoned upper triangles, reused/aliased destinations,
extreme scales, regularization, and nonfinite fallback behavior.

To repeat source validation, check out Kumulant `008d778` and archive Koblas
`6ab06f70c80b2c67396f19f9d9ebb7b4660fb57f` into a clean directory. Set that
directory in [the captured substitution script](source-substitution.init.gradle).
Apply the decompressed patch only to the view arm, then run
`./gradlew check lintDocs --rerun-tasks --init-script /path/to/substitution.init.gradle`.
The script's temporary absolute path records this capture's source location; it
must be adjusted when replaying elsewhere.

[Combo validation](consumer-validation.txt) passes its GP tests and frozen comparison
with both Kumulant and Koblas explicitly substituted from source. Its 16 model rows
differ from the published dependency by at most `2.2743e-15`; its 32 acquisition rows
differ by at most `1.4262e-12`. The paired consumer CSVs are retained here.

## Exploratory allocation comparison

The same seeded `CholeskyFactorBenchmark` fixtures and Koblas commit run on both
implementations. Each JMH case has two forks, two 500 ms warmups, three 500 ms
measurements, one thread, a 512 MiB heap, and the GC profiler. Scalar runs copy then
view; SIMD reverses that order. [Runtime commands](scalar-copy-command.json),
[host details](host.txt), and [source/JAR provenance](provenance.json) are retained.
`python3 analyze.py` regenerates [all allocation rows](allocations.csv) from the
compressed engine JSON. The owned result is allocated in both implementations.

| SIMD fixture | Copy B/op | View B/op |
| --- | ---: | ---: |
| Factor, order 8 | 568 | 568 |
| Factor, RBF 128 | 410810 | 246923 |
| Factor, RBF 512 | 8784643 | 3279506 |
| Factor with workspace, RBF 128 | 131273 | 131321 |
| Factor with workspace, RBF 512 | 7537363 | 2097724 |
| Factor with workspace, RBF 1024 | 52889554 | 8389929 |

At order 128 the old workspace already retains the caller arrays; views introduce
a small descriptor cost while eliminating copy work. At larger orders they also
avoid caller scratch allocations that churn through the bounded workspace pool.
Backend packing/scratch and the owned output remain; this is not an allocation-free
factor API.

This capture ran on a working desktop and overlapped other Gradle/Combo activity.
Its short timing settings are exploratory; timing samples are retained for audit
but establish no speedup or regression. They must not be compared with the earlier
Lab timings as a controlled before/after result.

## Pending publication and acceptance

The view integration needs a published Koblas snapshot containing the merged API
before the normal dependency pins and PR can adopt it. The final Lab comparison
must pin that same dependency cohort for both copying and view implementations,
then measure factorization and GP fitting workloads with the standard warmup/forks.
This preview does not mark #139 complete.
