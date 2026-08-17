package com.eignex.kumulant.bandit.univariate

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotSame
import kotlin.test.assertTrue

class MultiArmedBanditExtraTest {

    @Test
    fun `armStat exposes the live per-arm SeriesStat`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(0))
        val stat = mab.armStat(1)
        repeat(5) { mab.update(1, 4.0) }
        val view = stat.read(0L)
        assertTrue(view.totalWeights >= 5.0)
    }

    @Test
    fun `updateAll fans observations across arms`() {
        val mab = MultiArmedBandit(nbrArms = 3, policy = NormalTS(), random = Random(1))
        mab.updateAll(intArrayOf(0, 1, 2, 0), doubleArrayOf(1.0, 2.0, 3.0, 4.0))
        assertTrue(mab.armResult(0).totalWeights >= 2.0)
        assertTrue(mab.armResult(1).totalWeights >= 1.0)
        assertTrue(mab.armResult(2).totalWeights >= 1.0)
    }

    @Test
    fun `updateAll honours per-observation weights`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(2))
        mab.updateAll(
            armIndices = intArrayOf(0, 0),
            values = doubleArrayOf(1.0, 1.0),
            weights = doubleArrayOf(2.0, 3.0),
        )
        assertTrue(mab.armResult(0).totalWeights >= 5.0)
    }

    @Test
    fun `updateAll rejects mismatched arms-values arrays`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(3))
        assertFailsWith<IllegalArgumentException> {
            mab.updateAll(intArrayOf(0), doubleArrayOf(1.0, 2.0))
        }
    }

    @Test
    fun `updateAll rejects mismatched weights array`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(4))
        assertFailsWith<IllegalArgumentException> {
            mab.updateAll(intArrayOf(0, 1), doubleArrayOf(1.0, 2.0), doubleArrayOf(1.0))
        }
    }

    @Test
    fun `merge fans per-arm snapshots into running stats`() {
        val a = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(5))
        val b = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(6))
        repeat(10) {
            a.update(0, 1.0)
            b.update(0, 3.0)
        }
        val before = a.armResult(0).totalWeights
        a.merge(b.snapshot())
        assertTrue(a.armResult(0).totalWeights > before)
    }

    @Test
    fun `merge rejects size mismatch`() {
        val a = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(7))
        val b = MultiArmedBandit(nbrArms = 3, policy = NormalTS(), random = Random(8))
        assertFailsWith<IllegalArgumentException> { a.merge(b.snapshot()) }
    }

    @Test
    fun `reset clears running counts back to priors`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(9))
        repeat(20) { mab.update(0, 5.0) }
        val populated = mab.armResult(0).totalWeights
        mab.reset()
        val reseed = mab.armResult(0).totalWeights
        assertTrue(reseed < populated)
    }

    @Test
    fun `create spawns independent replica`() {
        val a = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(10))
        repeat(10) { a.update(0, 1.0) }
        val b = a.create(Random(11))
        assertNotSame(a, b)
        // Bandit b starts at priors; totalWeights smaller than populated a.
        assertTrue(a.armResult(0).totalWeights > b.armResult(0).totalWeights)
    }

    @Test
    fun `evaluate returns finite for each arm`() {
        val mab = MultiArmedBandit(nbrArms = 3, policy = NormalTS(), random = Random(12))
        repeat(3) { mab.update(it, 1.0) }
        for (a in 0 until 3) {
            val s = mab.evaluate(a)
            assertTrue(s.isFinite(), "evaluate($a) = $s")
        }
    }

    @Test
    fun `constructor rejects non-positive nbrArms`() {
        assertFailsWith<IllegalArgumentException> {
            MultiArmedBandit(nbrArms = 0, policy = NormalTS())
        }
    }

    @Test
    fun `snapshot equals per-arm armResult collection`() {
        val mab = MultiArmedBandit(nbrArms = 3, policy = NormalTS(), random = Random(13))
        for (a in 0 until 3) repeat(a + 1) { mab.update(a, a.toDouble()) }
        val snap = mab.snapshot()
        for (a in 0 until 3) assertEquals(mab.armResult(a), snap[a])
    }
}
