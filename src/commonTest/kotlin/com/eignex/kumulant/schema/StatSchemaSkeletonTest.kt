package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private class SkeletonHttpMetrics : StatSchema() {
    val requests by series(SumConfig)
    val latencyMs by series(DDSketchConfig(probabilities = listOf(0.5, 0.99)))
}

private class SkeletonNested : StatSchema() {
    val outer by series(SumConfig)
    val inner by group(SkeletonHttpMetrics())
}

class StatSchemaSkeletonTest {

    @Test
    fun `skeleton schema has empty specs but populated def`() {
        val schema = StatSchema.skeleton(::SkeletonHttpMetrics)
        assertEquals(0, schema.specs.size)
        val def = schema.definition()
        assertEquals(2, def.stats.size)
        assertEquals("requests", def.stats[0].name)
        assertEquals("latencyMs", def.stats[1].name)
    }

    @Test
    fun `skeleton mode is reentrant and restores the prior flag`() {
        StatSchema.skeleton(::SkeletonHttpMetrics)
        val normal = SkeletonHttpMetrics()
        assertEquals(2, normal.specs.size)

        val outer = StatSchema.skeleton(::SkeletonNested)
        assertEquals(0, outer.specs.size)
    }

    @Test
    fun `non-skeleton schema unchanged - default path materializes live stats`() {
        val schema = SkeletonHttpMetrics()
        assertEquals(2, schema.specs.size)
        assertEquals(2, schema.definition().stats.size)
    }

    @Test
    fun `bindTo returns a typed schema with empty specs - only the live group carries state`() {
        val def = SkeletonHttpMetrics().definition()
        val (typed, group) = def.bindTo(::SkeletonHttpMetrics, Concurrency.Strict)

        assertEquals(0, typed.specs.size)
        assertTrue(group.read().results.isEmpty().not())

        group.update(10.0)
        group.update(20.0)
        val snap = group.read()
        assertEquals(30.0, snap[typed.requests].sum)
    }

    // Live-stat overload always materializes regardless of skeleton mode (it doesn't
    // populate def at all, so a schema using only live-stat delegates fails to serialize).
    @Test
    fun `live-stat overload ignores the skeleton flag`() {
        val schema = StatSchema.skeleton {
            object : StatSchema() {
                val s by series(com.eignex.kumulant.stat.summary.Sum())
            }
        }
        assertEquals(1, schema.specs.size)
        val ex = runCatching { schema.definition() }.exceptionOrNull()
        assertTrue(ex is IllegalArgumentException)
        assertFalse(ex.message.isNullOrEmpty())
    }
}
