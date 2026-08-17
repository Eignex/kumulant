package com.eignex.kumulant.stat.score

import com.eignex.kumulant.assertModesAgree
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.calibration.ReliabilityStat
import kotlin.test.Test
import kotlin.test.assertEquals

class ScoreStatConcurrencyTest {

    private val preds = doubleArrayOf(0.1, 0.4, 0.7, 0.9, 0.2, 0.6, 0.3, 0.8)
    private val obs = doubleArrayOf(0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0)

    @Test
    fun `BrierScoreStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = BrierScoreStat(concurrency = mode)
            for (i in preds.indices) s.update(preds[i], obs[i])
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.mean, r.mean, 1e-9, "BrierScoreStat mode=$mode")
        }
    }

    @Test
    fun `MseLossStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = MseLossStat(concurrency = mode)
            for (i in preds.indices) s.update(preds[i], obs[i])
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.mean, r.mean, 1e-9, "MseLossStat mode=$mode")
        }
    }

    @Test
    fun `MaeLossStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = MaeLossStat(concurrency = mode)
            for (i in preds.indices) s.update(preds[i], obs[i])
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.mean, r.mean, 1e-9, "MaeLossStat mode=$mode")
        }
    }

    @Test
    fun `LogLossStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = LogLossStat(concurrency = mode)
            for (i in preds.indices) s.update(preds[i], obs[i])
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.mean, r.mean, 1e-9, "LogLossStat mode=$mode")
        }
    }

    @Test
    fun `PinballLossStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = PinballLossStat(tau = 0.5, concurrency = mode)
            for (i in preds.indices) s.update(preds[i], obs[i])
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.mean, r.mean, 1e-9, "PinballLossStat mode=$mode")
        }
    }

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
