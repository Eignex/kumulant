@file:OptIn(ExperimentalForeignApi::class)
@file:Suppress("MatchingDeclarationName", "Filename")

package com.eignex.kumulant.stream

import kotlinx.cinterop.ExperimentalForeignApi
import platform.posix.PTHREAD_MUTEX_ERRORCHECK

// toInt() regardless of the declared width: the constant is unsigned on some targets and signed on
// others, and Int.toInt() is the identity.
internal actual val ERRORCHECK_MUTEX_TYPE: Int = PTHREAD_MUTEX_ERRORCHECK.toInt()
