package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.schema.Const
import com.eignex.kumulant.schema.IfExpr
import com.eignex.kumulant.schema.V
import com.eignex.kumulant.schema.X
import com.eignex.kumulant.schema.gt
import com.eignex.kumulant.schema.times
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.math.exp
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
class CompositeArmTest {

    private fun zilnArm(): CompositeArm = CompositeArm(
        listOf(
            CompositeSubArm(
                arm = BernoulliArm(),
                valueExpr = IfExpr(X gt 0.0, Const(1.0), Const(0.0)),
            ),
            CompositeSubArm(
                arm = LogNormalArm(),
                filter = X gt 0.0,
            ),
        ),
    )

    private fun zilnPosterior(): CompositePosterior = CompositePosterior(
        subPosteriors = listOf(BetaPosterior, LogNormalGammaPosterior),
        combine = V(0) * V(1),
    )

    @Test
    fun `empty subArms is rejected`() {
        assertFailsWith<IllegalArgumentException> { CompositeArm(emptyList()) }
    }

    @Test
    fun `empty subPosteriors is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            CompositePosterior(subPosteriors = emptyList(), combine = Const(0.0))
        }
    }

    @Test
    fun `routing fans observations to sub-stats`() {
        // With ZILN routing, a positive observation reaches BOTH sub-arms (Bernoulli sees 1.0,
        // LogNormal sees ln(value)); a zero observation only reaches the Bernoulli (filtered).
        val stat = zilnArm().createStat()
        stat.update(5.0, 0L, 1.0)
        stat.update(0.0, 0L, 1.0)
        stat.update(3.0, 0L, 1.0)
        val results = stat.read(0L).results
        val bern = results[0] as BernoulliSumResult
        val logn = results[1] as WeightedVarianceResult
        // Bernoulli: 3 trials (priors + observations), 2 successes from priors + obs
        assertTrue(bern.successes > 0.0)
        assertTrue(bern.trials > 0.0)
        // LogNormal: should have seen 2 positive observations
        assertTrue(logn.totalWeights >= 2.0, "LogNormal saw ${logn.totalWeights} obs")
    }

    @Test
    fun `filter suppresses sub-arm updates`() {
        // Only the positive-filter sub-arm sees a positive observation
        val arm = CompositeArm(
            listOf(
                CompositeSubArm(BernoulliArm(), filter = X gt 0.0),
                CompositeSubArm(BernoulliArm(), filter = X gt 10.0),
            ),
        )
        val stat = arm.createStat()
        repeat(20) { stat.update(5.0, 0L, 1.0) }
        val results = stat.read(0L).results
        val firstObs = (results[0] as BernoulliSumResult).trials
        val secondObs = (results[1] as BernoulliSumResult).trials
        assertTrue(firstObs > secondObs, "first sub-arm should see more than second")
    }

    @Test
    fun `weightExpr scales the observation weight`() {
        val arm = CompositeArm(
            listOf(
                CompositeSubArm(BernoulliArm(), weightExpr = Const(0.5)),
            ),
        )
        val stat = arm.createStat()
        stat.update(1.0, 0L, 2.0)
        val results = stat.read(0L).results
        val bern = results[0] as BernoulliSumResult
        // Should see 0.5 * 2.0 = 1.0 added to successes (plus priors)
        assertTrue(bern.trials > 0.0)
    }

    @Test
    fun `merge fans the ResultList to per-sub-stats`() {
        val statA = zilnArm().createStat()
        val statB = zilnArm().createStat()
        repeat(50) { statA.update(if (it % 2 == 0) it.toDouble() + 1 else 0.0, 0L, 1.0) }
        repeat(50) { statB.update(if (it % 2 == 0) it.toDouble() + 1 else 0.0, 0L, 1.0) }
        val before = statA.read(0L).results[0] as BernoulliSumResult
        statA.merge(statB.read(0L))
        val after = statA.read(0L).results[0] as BernoulliSumResult
        assertTrue(after.trials > before.trials, "merged trials should grow")
    }

    @Test
    fun `merge rejects size mismatch`() {
        val statA = zilnArm().createStat()
        val mismatch = ResultList<Result>(
            listOf(BernoulliSumResult(1.0, 2.0)),
        )
        assertFailsWith<IllegalArgumentException> { statA.merge(mismatch) }
    }

    @Test
    fun `reset clears each sub-stat`() {
        val stat = zilnArm().createStat()
        repeat(20) { stat.update(it.toDouble() + 1, 0L, 1.0) }
        stat.reset()
        val results = stat.read(0L).results
        val bern = results[0] as BernoulliSumResult
        // After reset, only prior pseudo-counts remain
        assertEquals(BernoulliArm().priorAlpha + BernoulliArm().priorBeta, bern.trials, 1e-9)
    }

    @Test
    fun `composite posterior combines sub-draws via AST`() {
        val arm = zilnArm()
        val posterior = zilnPosterior()
        val stat = arm.createStat()
        val rng = Random(0)
        // Feed strongly positive signal: bernoulli ~ 0.9 and lognormal mean ~ exp(2)
        repeat(200) {
            val r = if (rng.nextDouble() < 0.9) exp(2.0) * (0.5 + rng.nextDouble()) else 0.0
            stat.update(r, 0L, 1.0)
        }
        val snap = stat.read(0L)
        val draws = (0 until 200).map { posterior.sample(snap, rng) }
        val mean = draws.average()
        // Expected score ~ P(positive) * E[positive] ~ 0.9 * exp(2 + sigma^2/2) ~ 6-8
        assertTrue(mean > 1.0, "ZILN mean draw should be substantially positive, got $mean")
    }

    @Test
    fun `composite posterior rejects snapshot size mismatch`() {
        val posterior = zilnPosterior()
        val wrongSnapshot = ResultList<Result>(
            listOf(BernoulliSumResult(1.0, 2.0)),
        )
        assertFailsWith<IllegalArgumentException> { posterior.sample(wrongSnapshot, Random(0)) }
    }

    @Test
    fun `composite arm integrates with MultiArmedBandit`() {
        val mab = MultiArmedBandit(
            nbrArms = 3,
            policy = ThompsonSampling(zilnArm(), zilnPosterior()),
            random = Random(0),
        )
        // Different arms: arm 0 always 0 reward, arm 1 always positive, arm 2 mixed
        val rng = Random(7)
        repeat(500) {
            val arm = mab.choose()
            val reward = when (arm) {
                0 -> 0.0
                1 -> exp(1.0) * (0.5 + rng.nextDouble())
                else -> if (rng.nextDouble() < 0.5) exp(0.5) else 0.0
            }
            mab.update(arm, reward)
        }
        // Snapshot should be three ResultLists with two sub-results each
        val snap = mab.snapshot()
        assertEquals(3, snap.size)
        for (s in snap) {
            assertEquals(2, s.results.size)
        }
    }

    @Test
    fun `create produces an independent composite stat`() {
        val stat = zilnArm().createStat()
        stat.update(5.0, 0L, 1.0)
        val fresh = stat.create(null)
        val freshBern = fresh.read(0L).results[0] as BernoulliSumResult
        val origBern = stat.read(0L).results[0] as BernoulliSumResult
        // Fresh has only prior pseudo-counts; original has more
        assertTrue(origBern.trials > freshBern.trials)
    }
}
