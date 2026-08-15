package com.eignex.kumulant.stream

/**
 * CPU hint issued inside a busy-wait loop, so a spinning reader doesn't monopolise its core's
 * pipeline while the writer it is waiting on makes progress.
 *
 * Per target:
 * - **JVM**: `Thread.onSpinWait()`, which emits the architecture's pause instruction.
 * - **JS / Wasm**: a no-op, correctly - these targets are single-threaded, so nothing else can be
 *   holding the value being waited on.
 * - **Native** (linux, macos, mingw, ios): also a no-op, *not* correctly. These targets are
 *   genuinely multi-threaded, but the source set wiring in `kumulant/build.gradle.kts` points
 *   `nativeMain` at `nonJvmMain`, and the Kotlin/Native stdlib has no portable pause intrinsic to
 *   call instead. The only caller is the bounded busy-wait in
 *   [com.eignex.kumulant.stat.quantile.TDigestStat]'s drain, whose iteration cap stops it wedging,
 *   so the cost is wasted cycles under contention rather than a wrong answer or a hang.
 */
internal expect fun spinHint()
