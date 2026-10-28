// Referenced only from KDoc @sample directives, which the compiler does not see as usages.
@file:Suppress("unused")

package com.eignex.kumulant.samples

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.schema.runtime.StatGroup
import com.eignex.kumulant.schema.runtime.StatSchema
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.Mean
import com.eignex.kumulant.schema.spec.Rate
import com.eignex.kumulant.schema.spec.SeriesStatSpec
import com.eignex.kumulant.schema.spec.StatSpec
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.skema.SchemaJson
import kotlinx.serialization.encodeToString

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

internal fun perStatConcurrency() {
    val hits = SumStat(concurrency = Concurrency.HighWrite)
    val ols = UnivariateRegressionStat(concurrency = Concurrency.Strict)
    hits.update(1.0)
    ols.update(x = 1.0, y = 2.0)
}

internal fun schemaDeclarationAndRead() {
    val telemetry = object : StatSchema() {
        val latencyMean by series(Mean)
        val errorRate by series(Rate)
    }
    val group = StatGroup(telemetry, concurrency = Concurrency.Strict)
    group.update(42.0)
    val results = group.read()
    println(results[telemetry.latencyMean].mean)
}

internal fun specRoundTrip() {
    val spec: StatSpec = Mean
    val json = SchemaJson.encodeToString(spec)
    val decoded = SchemaJson.decodeFromString<StatSpec>(json)
    val live = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
    live.update(1.0)
}

internal fun regressionUpdate() {
    val sgd = StochasticRegressionStat(featureSize = 3)
    sgd.update(doubleArrayOf(0.5, 1.0, -0.2), y = 1.5)
    val r = sgd.read()
    println(r.weights)
}
