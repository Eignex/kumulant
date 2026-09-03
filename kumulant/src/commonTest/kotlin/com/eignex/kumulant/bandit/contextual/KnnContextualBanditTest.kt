package com.eignex.kumulant.bandit.contextual

import com.eignex.koblas.Workspace
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64SparseVector
import com.eignex.kumulant.bandit.contextual.KnnContextualBandit.Companion.squaredL2
import com.eignex.kumulant.feat
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class KnnContextualBanditTest {

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
    fun `snapshot lists the surviving history oldest first after the ring wraps`() {
        val bandit = KnnContextualBandit(nbrArms = 1, k = 1, maxHistoryPerArm = 3, exploration = 0.0)
        repeat(5) { bandit.update(0, feat(it.toDouble()), it.toDouble()) }

        val arm = bandit.snapshot()[0]

        assertContentEquals(listOf(2.0, 3.0, 4.0), arm.contexts.map { it.single() })
        assertContentEquals(doubleArrayOf(2.0, 3.0, 4.0), arm.rewards)
        assertContentEquals(doubleArrayOf(1.0, 1.0, 1.0), arm.weights)
        assertEquals(3.0, arm.totalWeight, 1e-9)
    }

    @Test
    fun `evaluate scores a wrapped history from the survivors alone`() {
        val bandit = KnnContextualBandit(nbrArms = 1, k = 1, maxHistoryPerArm = 3, exploration = 0.0)
        repeat(5) { bandit.update(0, feat(it.toDouble()), it.toDouble()) }

        assertEquals(2.0, bandit.evaluate(0, feat(2.0)), 1e-12)
        assertEquals(4.0, bandit.evaluate(0, feat(4.0)), 1e-12)
        // x=0 was evicted, so the nearest neighbour of x=0 is the oldest survivor.
        assertEquals(2.0, bandit.evaluate(0, feat(0.0)), 1e-12)
    }

    @Test
    fun `merge appends foreign samples and trims the oldest across a wrap`() {
        val local = KnnContextualBandit(nbrArms = 1, k = 1, maxHistoryPerArm = 4, exploration = 0.0)
        val foreign = KnnContextualBandit(nbrArms = 1, k = 1, maxHistoryPerArm = 4, exploration = 0.0)
        local.update(0, feat(0.0), 0.0)
        local.update(0, feat(1.0), 1.0)
        for (i in 2..4) foreign.update(0, feat(i.toDouble()), i.toDouble())

        local.merge(foreign.snapshot())

        val arm = local.snapshot()[0]
        assertContentEquals(listOf(1.0, 2.0, 3.0, 4.0), arm.contexts.map { it.single() })
        assertContentEquals(doubleArrayOf(1.0, 2.0, 3.0, 4.0), arm.rewards)
        assertEquals(4.0, arm.totalWeight, 1e-9)
    }

    @Test
    fun `a wrapped history keeps dense and sparse slots apart as they overwrite each other`() {
        val bandit = KnnContextualBandit(nbrArms = 1, k = 1, maxHistoryPerArm = 2, exploration = 0.0)
        bandit.update(0, feat(1.0, 0.0), 1.0)
        bandit.update(0, feat(0.0, 2.0), 2.0)
        bandit.update(0, F64SparseVector.of(2, intArrayOf(1), doubleArrayOf(3.0)), 3.0)
        bandit.update(0, feat(4.0, 0.0), 4.0)

        val contexts = bandit.snapshot()[0].contexts

        assertContentEquals(doubleArrayOf(0.0, 3.0), contexts[0])
        assertContentEquals(doubleArrayOf(4.0, 0.0), contexts[1])
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

    @Test
    fun `squaredL2 dense and sparse agree across storage combinations`() {
        val aDense = F64DenseVector.of(doubleArrayOf(1.0, 0.0, -2.0, 3.0, 0.0))
        val bDense = F64DenseVector.of(doubleArrayOf(0.0, 4.0, -2.0, 1.0, 5.0))
        val aSparse = F64SparseVector.of(
            size = 5,
            indices = intArrayOf(0, 2, 3),
            values = doubleArrayOf(1.0, -2.0, 3.0),
        )
        val bSparse = F64SparseVector.of(
            size = 5,
            indices = intArrayOf(1, 2, 3, 4),
            values = doubleArrayOf(4.0, -2.0, 1.0, 5.0),
        )
        val reference = squaredL2(aDense, bDense)
        assertEquals(reference, squaredL2(aSparse, bDense), 1e-12)
        assertEquals(reference, squaredL2(aDense, bSparse), 1e-12)
        assertEquals(reference, squaredL2(aSparse, bSparse), 1e-12)
    }

    @Test
    fun `sparse update preserves sparse storage and matches dense scoring`() {
        val sparseInput = KnnContextualBandit(nbrArms = 1, k = 2, exploration = 0.0)
        val denseInput = KnnContextualBandit(nbrArms = 1, k = 2, exploration = 0.0)
        val rows = listOf(
            doubleArrayOf(1.0, 0.0, 0.0, 2.0) to 0.5,
            doubleArrayOf(0.0, 0.0, 3.0, 0.0) to -0.25,
            doubleArrayOf(0.5, 0.0, 0.0, 1.5) to 1.0,
        )
        for ((x, r) in rows) {
            denseInput.update(0, F64DenseVector.of(x), r)
            val nz = x.withIndex().filter { it.value != 0.0 }
            sparseInput.update(
                0,
                F64SparseVector.of(
                    size = x.size,
                    indices = nz.map { it.index }.toIntArray(),
                    values = nz.map { it.value }.toDoubleArray(),
                ),
                r,
            )
        }
        val q = F64DenseVector.of(doubleArrayOf(0.5, 0.0, 0.0, 1.5))
        assertEquals(denseInput.evaluate(0, q), sparseInput.evaluate(0, q), 1e-12)
    }

    @Test
    fun `update retains an independent sparse snapshot including stored zeros`() {
        val indices = intArrayOf(0, 2)
        val values = doubleArrayOf(1.0, 0.0)
        val bandit = KnnContextualBandit(nbrArms = 1, k = 1, exploration = 0.0)
        bandit.update(0, F64SparseVector.wrap(3, indices, values), 4.0)

        indices[0] = 1
        values[0] = 99.0
        assertEquals(4.0, bandit.evaluate(0, feat(1.0, 0.0, 0.0)), 1e-12)
    }

    @Test
    fun `update retains an empty sparse snapshot`() {
        val bandit = KnnContextualBandit(nbrArms = 1, k = 1, exploration = 0.0)
        bandit.update(0, F64SparseVector.wrap(3, IntArray(0), DoubleArray(0)), 4.0)

        assertEquals(4.0, bandit.evaluate(0, feat(0.0, 0.0, 0.0)), 1e-12)
    }

    @Test
    fun `zero-weight update does not displace real history`() {
        val bandit = KnnContextualBandit(nbrArms = 1, k = 1, maxHistoryPerArm = 2, exploration = 0.0)
        val x = feat(0.0)
        bandit.update(0, x, 9.0, weight = 1.0)
        bandit.update(0, x, 0.0, weight = 0.0)
        bandit.update(0, x, 0.0, weight = 0.0)
        assertEquals(9.0, bandit.evaluate(0, x))
    }

    @Test
    fun `squaredL2 counts a stored zero once`() {
        val a = F64SparseVector.of(2, intArrayOf(0), doubleArrayOf(0.0))
        val b = F64SparseVector.of(2, intArrayOf(0), doubleArrayOf(3.0))
        assertEquals(9.0, squaredL2(a, b))
    }

    @Test
    fun `workspace scoring matches allocating scoring across sparse contexts`() {
        val allocating = KnnContextualBandit(nbrArms = 2, k = 2, exploration = 0.0)
        val reused = KnnContextualBandit(nbrArms = 2, k = 2, exploration = 0.0)
        val samples = listOf(
            Triple(0, F64SparseVector.of(3, intArrayOf(0), doubleArrayOf(1.0)), 0.5),
            Triple(0, F64SparseVector.of(3, intArrayOf(1), doubleArrayOf(-2.0)), -1.0),
            Triple(1, F64SparseVector.of(3, intArrayOf(2), doubleArrayOf(3.0)), 1.5),
            Triple(1, F64SparseVector.of(3, intArrayOf(0, 2), doubleArrayOf(-1.0, 1.0)), 0.25),
        )
        for ((arm, x, reward) in samples) {
            allocating.update(arm, x, reward)
            reused.update(arm, x, reward)
        }
        val x = F64SparseVector.of(3, intArrayOf(0), doubleArrayOf(0.5))
        val workspace = Workspace().apply { reserve(6, 1) }

        for (arm in 0 until 2) assertEquals(allocating.evaluate(arm, x), reused.evaluate(arm, x, workspace), 1e-12)
        assertEquals(allocating.choose(x), reused.choose(x, workspace))
    }

    @Test
    fun `a failed distance leaves the scan buffer usable for the next call`() {
        var failing = true
        val bandit = KnnContextualBandit(
            nbrArms = 1,
            k = 1,
            exploration = 0.0,
            distance = { a, b -> if (failing) error("distance failed") else squaredL2(a, b) },
        )
        bandit.update(0, feat(1.0), 4.0)

        assertFailsWith<IllegalStateException> { bandit.evaluate(0, feat(1.0)) }
        failing = false

        assertEquals(4.0, bandit.evaluate(0, feat(1.0)), 1e-12)
    }

    @Test
    fun `the k nearest are the same whatever order the contexts arrive in`() {
        // Contexts sit at 1..6 from the query, so the three nearest carry rewards 1, 2 and 3 and
        // average 2.0. Ascending admits each candidate once and then rejects; descending displaces
        // on every step, which is the order that exercises the rescan hardest.
        val ascending = listOf(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
        for (order in listOf(ascending, ascending.reversed(), listOf(4.0, 1.0, 6.0, 2.0, 5.0, 3.0))) {
            val bandit = KnnContextualBandit(nbrArms = 1, k = 3, exploration = 0.0)
            for (d in order) bandit.update(0, feat(d), d)
            assertEquals(2.0, bandit.evaluate(0, feat(0.0)), 1e-12, "arrival order $order")
        }
    }

    @Test
    fun `equal distances keep the neighbour that arrived first`() {
        val bandit = KnnContextualBandit(nbrArms = 1, k = 1, exploration = 0.0)
        bandit.update(0, feat(1.0), 10.0)
        bandit.update(0, feat(1.0), 20.0)

        assertEquals(10.0, bandit.evaluate(0, feat(1.0)), 1e-12)
    }
}

class KnnContextDimensionTest {

    @Test
    fun `update rejects a context of the wrong width at the entry point`() {
        val b = KnnContextualBandit(nbrArms = 2, k = 5)
        b.update(0, feat(1.0, 2.0, 3.0), 1.0)
        assertFailsWith<IllegalArgumentException> {
            b.update(0, feat(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0), 1.0)
        }
    }

    @Test
    fun `a negative weight does not spend a history slot`() {
        val b = KnnContextualBandit(nbrArms = 2, k = 1)
        val x = feat(1.0, 2.0)
        b.update(0, x, 10.0, weight = 1.0)
        b.update(0, x, 10.0, weight = -1.0)
        assertEquals(1, b.historySize(0))
    }
}
