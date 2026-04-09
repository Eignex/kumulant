package com.eignex.kumulant.core

import com.eignex.kumulant.stat.*
import kotlin.test.*

class FlattenTest {

    @Test
    fun `flatten Result2 includes both sub-results`() {
        val s = Sum(name = "s") + Mean(name = "m")
        s.update(4.0)
        s.update(6.0)
        val entries = s.read().flatten()
        val keys = entries.map { it.name }
        assertTrue("s.sum" in keys, "expected 's.sum' in $keys")
        assertTrue(
            "m.mean" in keys || keys.any { it.endsWith("mean") },
            "expected mean in $keys"
        )
    }

    @Test
    fun `flatten Result3 includes all three sub-results`() {
        val s = Sum(name = "s") + Mean(name = "m") + Variance(name = "v")
        s.update(1.0)
        s.update(3.0)
        val keys = s.read().flatten().map { it.name }
        assertTrue(keys.any { it.endsWith("sum") })
        assertTrue(keys.any { it.endsWith("mean") })
        assertTrue(keys.any { it.endsWith("variance") })
    }
}
