@file:Suppress("MatchingDeclarationName", "Filename")

package com.eignex.kumulant.stream

/**
 * No-op. Kotlin/Native has no portable pause intrinsic. See the `expect` declaration for why there
 * is nothing to call instead.
 */
internal actual fun spinHint() = Unit
