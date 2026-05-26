package com.eignex.kumulant.stat.anomaly

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class QuantileFilterStatTest {

    @Test
    fun `threshold approximates the configured quantile`() {
        val s = QuantileFilterStat(probability = 0.9, relativeError = 0.005)
        for (i in 1..1000) s.update(i.toDouble())
        val r = s.read()
        // 90th percentile of 1..1000 is ~900; allow slack for DDSketch relative error.
        assertTrue(r.threshold in 870.0..930.0, "threshold=${r.threshold}")
    }

    @Test
    fun `score is one above the threshold and zero below`() {
        val s = QuantileFilterStat(probability = 0.95)
        for (i in 1..200) s.update(i.toDouble())
        val r = s.read()
        assertEquals(1.0, r.score(r.threshold + 50.0))
        assertEquals(0.0, r.score(r.threshold - 50.0))
        assertEquals(0.0, r.score(r.threshold))
    }

    @Test
    fun `merge is unsupported`() {
        val a = QuantileFilterStat()
        val b = QuantileFilterStat()
        a.update(1.0)
        b.update(2.0)
        assertFailsWith<UnsupportedOperationException> { a.merge(b.read()) }
    }

    @Test
    fun `requires probability in 0 to 1`() {
        assertFailsWith<IllegalArgumentException> { QuantileFilterStat(probability = 0.0) }
        assertFailsWith<IllegalArgumentException> { QuantileFilterStat(probability = 1.0) }
    }
}
