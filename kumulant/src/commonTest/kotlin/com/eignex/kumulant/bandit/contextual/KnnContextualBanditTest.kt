package com.eignex.kumulant.bandit.contextual

import com.eignex.kumulant.math.DenseVector
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class KnnContextualBanditTest {

    private fun feat(vararg xs: Double): DenseVector = DenseVector.of(xs)

    @Test
    fun `KnnContextualBandit rejects bad inputs`() {
        assertFailsWith<IllegalArgumentException> { KnnContextualBandit(nbrArms = 0) }
        assertFailsWith<IllegalArgumentException> { KnnContextualBandit(nbrArms = 2, k = 0) }
        assertFailsWith<IllegalArgumentException> { KnnContextualBandit(nbrArms = 2, maxHistoryPerArm = 0) }
        assertFailsWith<IllegalArgumentException> { KnnContextualBandit(nbrArms = 2, exploration = -1.0) }
    }

    @Test
    fun `cold-start scoring favours uninformed arms`() {
        val bandit =
            KnnContextualBandit(nbrArms = 2, k = 3, coldStartScore = 1.0, exploration = 0.0, random = Random(0))
        // arm 0 has some bad data; arm 1 is untouched (still cold).
        repeat(5) { bandit.update(0, feat(0.0), -1.0) }
        val s0 = bandit.evaluate(0, feat(0.0))
        val s1 = bandit.evaluate(1, feat(0.0))
        assertTrue(s1 > s0, "cold arm should win: s0=$s0 s1=$s1")
    }

    @Test
    fun `choose picks the arm whose local neighbourhood pays better`() {
        val rng = Random(1)
        val bandit = KnnContextualBandit(nbrArms = 2, k = 3, exploration = 0.0, random = rng)
        // Arm 0 pays well around x<0, arm 1 pays well around x>=0.
        repeat(80) {
            val x = rng.nextDouble() * 2 - 1
            val xv = feat(x)
            bandit.update(0, xv, if (x < 0.0) 1.0 else -1.0)
            bandit.update(1, xv, if (x < 0.0) -1.0 else 1.0)
        }
        val picksAtNeg = IntArray(2)
        val picksAtPos = IntArray(2)
        repeat(50) {
            picksAtNeg[bandit.choose(feat(-0.5))]++
            picksAtPos[bandit.choose(feat(0.5))]++
        }
        assertTrue(picksAtNeg[0] > picksAtNeg[1], "at x<0 arm 0 wins: $picksAtNeg")
        assertTrue(picksAtPos[1] > picksAtPos[0], "at x>0 arm 1 wins: $picksAtPos")
    }

    @Test
    fun `update caps history at maxHistoryPerArm and rolls oldest off`() {
        val bandit = KnnContextualBandit(nbrArms = 1, k = 1, maxHistoryPerArm = 3, exploration = 0.0)
        repeat(10) { bandit.update(0, feat(it.toDouble()), it.toDouble()) }
        assertEquals(3, bandit.historySize(0))
        // armWeight reflects only the surviving 3 entries (all weight 1).
        assertEquals(3.0, bandit.armWeight(0), 1e-9)
        // Latest x=9 dominates; querying near 9 should return the recent reward.
        val s = bandit.evaluate(0, feat(9.0))
        assertTrue(s in 6.5..9.5, "expected score near recent rewards 7/8/9, got $s")
    }

    @Test
    fun `reset clears every arm's history`() {
        val bandit = KnnContextualBandit(nbrArms = 2, k = 2)
        bandit.update(0, feat(0.0), 1.0)
        bandit.update(1, feat(0.0), 1.0)
        bandit.reset()
        assertEquals(0, bandit.historySize(0))
        assertEquals(0, bandit.historySize(1))
        assertEquals(0.0, bandit.armWeight(0), 1e-9)
    }

    @Test
    fun `create spawns independent replica`() {
        val a = KnnContextualBandit(nbrArms = 2, k = 1)
        repeat(5) { a.update(0, feat(0.0), 1.0) }
        val b = a.create(a.random)
        assertEquals(0, b.historySize(0))
        assertEquals(5, a.historySize(0))
    }
}
