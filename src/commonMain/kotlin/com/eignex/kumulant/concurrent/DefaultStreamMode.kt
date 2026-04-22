package com.eignex.kumulant.concurrent

/**
 * Global default [StreamMode] used by stat constructors when none is passed.
 *
 * Mutable via [withMode] or direct assignment. Not thread-isolated: in concurrent
 * contexts where different threads need different defaults, pass `mode =` explicitly
 * to each stat constructor instead of relying on this global.
 */
var defaultStreamMode: StreamMode = SerialMode
