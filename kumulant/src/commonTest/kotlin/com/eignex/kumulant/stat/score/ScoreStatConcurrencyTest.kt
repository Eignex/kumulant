package com.eignex.kumulant.stat.score

import com.eignex.kumulant.assertModesAgree
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.calibration.ReliabilityStat
import kotlin.test.Test

class ScoreStatConcurrencyTest {

    private val preds = doubleArrayOf(0.1, 0.4, 0.7, 0.9, 0.2, 0.6, 0.3, 0.8)
    private val obs = doubleArrayOf(0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0)

    @Test
    fun `ReliabilityStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = ReliabilityStat(numBins = 5, concurrency = mode)
            for (i in preds.indices) s.update(preds[i], obs[i])
            s.read(0L)
        }
        assertModesAgree("ReliabilityStat", reads)
    }
}
