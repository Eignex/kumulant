@file:Suppress("UnusedPrivateMember", "unused", "UNUSED_PARAMETER")

package com.eignex.kumulant.samples

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.schema.Mean
import com.eignex.kumulant.schema.Rate
import com.eignex.kumulant.schema.SeriesStatSpec
import com.eignex.kumulant.schema.StatGroup
import com.eignex.kumulant.schema.StatKey
import com.eignex.kumulant.schema.StatSchema
import com.eignex.kumulant.schema.StatSpec
import com.eignex.kumulant.schema.materialize
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.skema.SchemaJson
import kotlinx.serialization.encodeToString

/**
 * Build a [MeanStat], fold a few observations into it, take a snapshot, and merge
 * a peer's snapshot back in. The four-verb shape (`update` / `read` / `merge` /
 * `reset` / `create`) is the same for every stat.
 */
internal fun basicMeanLifecycle() {
    val mean = MeanStat()
    for (x in doubleArrayOf(1.0, 2.0, 3.0)) mean.update(x)
    val snapshot = mean.read()
    println(snapshot.mean) // 2.0

    val peer = MeanStat()
    for (x in doubleArrayOf(4.0, 5.0)) peer.update(x)
    mean.merge(peer.read())
    println(mean.read().mean) // 3.0
}

/**
 * Picking a [Concurrency] level per stat at construction. The contract is
 * user-facing: each stat translates it into the right cell encoding and lock
 * strategy for its mathematical structure.
 */
internal fun perStatConcurrency() {
    val hits = SumStat(concurrency = Concurrency.HighWrite)
    val ols = UnivariateRegressionStat(concurrency = Concurrency.Strict)
    hits.update(1.0)
    ols.update(x = 1.0, y = 2.0)
}

/**
 * Declaring a schema, materialising it into a live [StatGroup], updating, and
 * reading results back by typed [StatKey].
 */
internal fun schemaDeclarationAndRead() {
    val telemetry = object : StatSchema(concurrency = Concurrency.Strict) {
        val latencyMean by series(Mean)
        val errorRate by series(Rate)
    }
    val group = StatGroup(telemetry)
    group.update(42.0)
    val results = group.read()
    println(results[telemetry.latencyMean].mean)
}

/**
 * Encoding a [StatSpec] to JSON, decoding it on the other side, and
 * materialising the same stat to absorb merge inputs from a peer.
 */
internal fun specRoundTrip() {
    val spec: StatSpec = Mean
    val json = SchemaJson.encodeToString(spec)
    val decoded = SchemaJson.decodeFromString<StatSpec>(json)
    val live = (decoded as SeriesStatSpec<WeightedMeanResult>).materialize(Concurrency.None)
    live.update(1.0)
}

/**
 * Driving a [StochasticRegressionStat] over a stream of feature vectors and
 * a scalar response. The same shape works for every multivariate regressor.
 */
internal fun regressionUpdate() {
    val sgd = StochasticRegressionStat(featureSize = 3)
    sgd.update(doubleArrayOf(0.5, 1.0, -0.2), y = 1.5)
    val r = sgd.read()
    println(r.weights)
}
