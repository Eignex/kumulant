package com.eignex.kumulant.stat.change

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ChangeStatMergeAdoptionTest {

    private fun cusumSnapshot(): CusumResult {
        val worker = CusumStat(target = 0.0, referenceValue = 0.5, threshold = 5.0)
        repeat(20) { worker.update(2.0) }
        return worker.read()
    }

    @Test
    fun `an empty CusumStat adopts a merged snapshot exactly`() {
        val snapshot = cusumSnapshot()
        assertTrue(snapshot.cusumPositive > 0.0, "the fixture never drifted, so it proves nothing")

        val coordinator = CusumStat(target = 0.0, referenceValue = 0.5, threshold = 5.0)
        coordinator.merge(snapshot)

        val merged = coordinator.read()
        assertEquals(snapshot.cusumPositive, merged.cusumPositive, DELTA, "the drift was halved")
        assertEquals(snapshot.cusumNegative, merged.cusumNegative, DELTA, "the negative drift was halved")
    }

    @Test
    fun `a merged snapshot still raises the alarm it raised on the worker`() {
        // Place the threshold between the full drift and half of it so accidental averaging changes the alarm.
        val worker = CusumStat(target = 0.0, referenceValue = 0.5, threshold = 20.0)
        repeat(20) { worker.update(2.0) }
        val snapshot = worker.read()
        assertTrue(snapshot.alarmUp, "the fixture did not alarm, so the assertion below is vacuous")
        assertTrue(0.5 * snapshot.cusumPositive < 20.0, "halving would still alarm, so this cannot discriminate")

        val coordinator = CusumStat(target = 0.0, referenceValue = 0.5, threshold = 20.0)
        coordinator.merge(snapshot)

        assertTrue(coordinator.read().alarmUp, "the coordinator lost an alarm the worker had raised")
    }

    @Test
    fun `a non-empty CusumStat still averages`() {
        val a = CusumStat(target = 0.0, referenceValue = 0.5, threshold = 5.0)
        repeat(20) { a.update(2.0) }
        val own = a.read().cusumPositive

        val snapshot = cusumSnapshot()
        a.merge(snapshot)

        assertEquals(0.5 * (own + snapshot.cusumPositive), a.read().cusumPositive, DELTA)
    }

    @Test
    fun `an empty CusumStat that has been reset adopts again`() {
        val coordinator = CusumStat(target = 0.0, referenceValue = 0.5, threshold = 5.0)
        repeat(20) { coordinator.update(2.0) }
        coordinator.reset()

        val snapshot = cusumSnapshot()
        coordinator.merge(snapshot)

        assertEquals(snapshot.cusumPositive, coordinator.read().cusumPositive, DELTA, "reset left it primed")
    }

    // A constant input produces no Page-Hinkley drift because it subtracts its running mean.
    private fun pageHinkleySnapshot(): PageHinkleyResult {
        val worker = PageHinkleyStat(delta = 0.005, threshold = 50.0)
        repeat(60) { worker.update(0.0) }
        repeat(60) { worker.update(5.0) }
        return worker.read()
    }

    @Test
    fun `an empty PageHinkleyStat adopts the merged drift exactly`() {
        val snapshot = pageHinkleySnapshot()
        assertTrue(snapshot.cumulativePositive > 0.0, "the fixture never drifted, so it proves nothing")

        val coordinator = PageHinkleyStat(delta = 0.005, threshold = 50.0)
        coordinator.merge(snapshot)

        val merged = coordinator.read()
        assertEquals(snapshot.cumulativePositive, merged.cumulativePositive, DELTA, "the drift was halved")
        assertEquals(snapshot.cumulativeNegative, merged.cumulativeNegative, DELTA, "the drift was halved")
        assertEquals(snapshot.mean, merged.mean, DELTA, "the mean should always have adopted")
    }

    @Test
    fun `a non-empty PageHinkleyStat still averages the drift`() {
        val a = PageHinkleyStat(delta = 0.005, threshold = 50.0)
        repeat(60) { a.update(0.0) }
        repeat(60) { a.update(5.0) }
        val own = a.read().cumulativePositive

        val snapshot = pageHinkleySnapshot()
        a.merge(snapshot)

        assertEquals(0.5 * (own + snapshot.cumulativePositive), a.read().cumulativePositive, DELTA)
    }
}
