package com.eignex.kumulant.stat.regression.tree

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DefaultMtryTest {

    @Test
    fun `matches ceil of the square root`() {
        assertEquals(1, defaultMtry(1))
        assertEquals(2, defaultMtry(2))
        assertEquals(2, defaultMtry(4))
        assertEquals(3, defaultMtry(5))
        assertEquals(3, defaultMtry(9))
        assertEquals(4, defaultMtry(10))
        assertEquals(10, defaultMtry(100))
        assertEquals(11, defaultMtry(101))
    }

    @Test
    fun `an empty candidate pool means no growth rather than one candidate`() {
        // The distinction that makes this worth pinning: a forest built with no split candidates should
        // report an mtry of zero, not clamp up to one and then try to draw from an empty list.
        assertEquals(0, defaultMtry(0))
        assertEquals(0, defaultMtry(-1))
    }

    @Test
    fun `never exceeds the pool it draws from`() {
        // mtry larger than the pool would make the per-leaf subset draw meaningless. sqrt(p) <= p for
        // every p >= 1, but the clamp is easy to get wrong, so check the whole small range.
        for (p in 1..64) {
            val mtry = defaultMtry(p)
            assertEquals(true, mtry in 1..p, "defaultMtry($p) = $mtry is outside 1..$p")
        }
    }

    @Test
    fun `both forests default to it and honour an explicit override`() {
        val splits = List(9) { ThresholdSplit(featureIndex = it % 3, threshold = 0.5) }

        assertEquals(3, RandomForestRegressionStat(3, splits, nbrTrees = 2).config.mtry)
        assertEquals(
            3,
            RandomForestClassifierStat(3, numClasses = 2, splitCandidates = splits, nbrTrees = 2).config.mtry,
        )

        // An explicit mtry has to survive the defaulting, or the config knob would be unreachable.
        assertEquals(
            2,
            RandomForestRegressionStat(
                3,
                splits,
                config = RegressionTreeConfig(mtry = 2),
                nbrTrees = 2,
            ).config.mtry,
        )
        assertEquals(
            2,
            RandomForestClassifierStat(
                3,
                numClasses = 2,
                splitCandidates = splits,
                config = ClassificationTreeConfig(mtry = 2),
                nbrTrees = 2,
            ).config.mtry,
        )
    }
}

class MtryValidationTest {

    @Test
    fun `a negative mtry is rejected at construction`() {
        for (bad in listOf(-1, -7)) {
            assertFailsWith<IllegalArgumentException>("mtry=$bad was accepted") {
                DecisionTreeRegressionStat(
                    featureSize = 2,
                    splitCandidates = emptyList(),
                    config = RegressionTreeConfig(mtry = bad),
                )
            }
        }
    }
}
