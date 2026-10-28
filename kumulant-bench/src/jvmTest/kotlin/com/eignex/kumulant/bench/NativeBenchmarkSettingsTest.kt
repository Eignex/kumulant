package com.eignex.kumulant.bench

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class NativeBenchmarkSettingsTest {
    @Test fun `partial native overrides retain registry defaults`() {
        val settings = NativeBenchmarkSettings(listOf("-Pbench.filter=.*Cholesky.*", "-Pbench.param.matrixCase=precision:8,rbf:128"), false)
        val arguments = settings.arguments()
        assertTrue("-Pbench.param.name=${allSpecs.joinToString(",") { it.name }}" in arguments)
        assertTrue("-Pbench.param.policy=${allBanditSpecs.joinToString(",") { it.name }}" in arguments)
        assertTrue(settings.configuration().containsAll(listOf("param:matrixCase=precision:8", "param:matrixCase=rbf:128")))
    }

    @Test fun `native Gradle controls map to the prepared engine protocol`() {
        val settings = NativeBenchmarkSettings(listOf("-Pbench.stats=SumStat,MeanStat", "-Pbench.policies=MultiArmed/Thompson",
            "-Pbench.warmups=1", "-Pbench.iterations=3", "-Pbench.iterationTime=20", "-Pbench.iterationTimeUnit=ms",
            "-Pbench.mode=thrpt", "-Pbench.timeUnit=s"), true)
        assertTrue(settings.configuration().containsAll(listOf("param:name=SumStat", "param:name=MeanStat", "warmups:1",
            "iterations:3", "iterationTime:20", "iterationTimeUnit:ms", "mode:thrpt", "outputTimeUnit:s")))
        assertEquals(settings.arguments(), NativeBenchmarkSettings(settings.arguments(), false).arguments())
    }

    @Test fun `unsupported native properties and concurrency are rejected`() {
        for (args in listOf(
            listOf("-Pbench.param.unknown=1"), listOf("-Pbench.output=/tmp/out"), listOf("-Pbench.mode=sample"), listOf("-Pbench.threads=4"),
            listOf("-Pbench.param.concurrency=Strict"), listOf("-Pbench.iterations=0"), listOf("-Pbench.warmups=-1"), listOf("-Pbench.warmups=0"),
            listOf("-Pbench.stats=SumStat", "-Pbench.param.name=MeanStat"), listOf("-Pbench.filter=a", "-Pbench.filter=b"),
            listOf("-Pbench.param.name="), listOf("-Pbench.timeUnit=hours"), listOf("--no-daemon"),
        )) assertFailsWith<IllegalArgumentException>(args.toString()) { NativeBenchmarkSettings(args, true) }
    }
}
