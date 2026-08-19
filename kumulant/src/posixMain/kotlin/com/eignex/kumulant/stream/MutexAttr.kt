package com.eignex.kumulant.stream

/**
 * `PTHREAD_MUTEX_ERRORCHECK` for the target being built.
 *
 * The constant is a different width per platform, so the shared posix view cannot name it directly;
 * each target restates it as the [Int] `pthread_mutexattr_settype` wants.
 */
internal expect val ERRORCHECK_MUTEX_TYPE: Int
