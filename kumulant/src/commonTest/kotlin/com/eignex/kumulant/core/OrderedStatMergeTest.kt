package com.eignex.kumulant.core

import com.eignex.kumulant.schema.runtime.ListStats
import com.eignex.kumulant.stat.change.AdwinStat
import com.eignex.kumulant.stat.change.CusumStat
import com.eignex.kumulant.stat.change.PageHinkleyStat
import com.eignex.kumulant.stat.forecast.HoltStat
import com.eignex.kumulant.stat.forecast.RecursiveVarianceStat
import com.eignex.kumulant.stat.forecast.SeasonalSmoothingStat
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class OrderedStatMergeTest {

    @Test
    fun `ordered stats reject snapshot merges without changing state`() {
        val stats = listOf(
            AdwinStat(),
            CusumStat(),
            PageHinkleyStat(),
            HoltStat(alpha = 0.3),
            SeasonalSmoothingStat(alpha = 0.3, beta = 0.2, gamma = 0.1, period = 4),
            RecursiveVarianceStat(omega = 0.1, alpha = 0.2, beta = 0.7),
        )
        for (candidate in stats) {
            @Suppress("UNCHECKED_CAST")
            val stat = candidate as SeriesStat<Result>
            repeat(8) { stat.update(it.toDouble()) }
            val snapshot = stat.read()
            val empty = stat.create()

            assertFailsWith<UnsupportedOperationException> { empty.validateMerge(snapshot) }
            assertFailsWith<UnsupportedOperationException> { empty.merge(snapshot) }
            assertFailsWith<UnsupportedOperationException> { stat.merge(snapshot) }
            assertEquals(snapshot, stat.read())
        }
    }

    @Test
    fun `a refusing detector leaves earlier group entries unchanged`() {
        val group = ListStats<Result>("sum" to SumStat(), "detector" to AdwinStat())
        group.update(2.0)
        val before = group.read()

        assertFailsWith<UnsupportedOperationException> { group.merge(before) }

        assertEquals(before, group.read())
    }
}
