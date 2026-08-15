@file:Suppress("MatchingDeclarationName", "Filename")

package com.eignex.kumulant.stream

/**
 * No-op. Correct for JS and Wasm, which are single-threaded; a known gap on native, which shares this
 * source set but is not. See the `expect` declaration for why there is nothing to call instead.
 */
internal actual fun spinHint() = Unit
