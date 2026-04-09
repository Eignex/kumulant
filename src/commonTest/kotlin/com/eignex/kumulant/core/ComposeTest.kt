@file:OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)

package com.eignex.kumulant.core

import com.eignex.kumulant.concurrent.AtomicMode
import com.eignex.kumulant.concurrent.SerialMode
import com.eignex.kumulant.stat.*
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import kotlinx.serialization.json.Json
import kotlinx.serialization.protobuf.ProtoBuf
import kotlin.test.*

private const val DELTA = 1e-12

class ComposeTest {

    @Test
    fun `SeriesStat2 updates both stats`() {
        val s = Sum() + Mean()
        s.update(2.0)
        s.update(4.0)
        val r = s.read()
        assertEquals(6.0, r.first.sum, DELTA)
        assertEquals(3.0, r.second.mean, DELTA)
    }

    @Test
    fun `SeriesStat2 merge propagates to both`() {
        val s1 = Sum() + Mean()
        s1.update(2.0)
        s1.update(4.0)
        val s2 = Sum() + Mean()
        s2.update(6.0)
        s1.merge(s2.read())
        assertEquals(12.0, s1.read().first.sum, DELTA)
    }

    @Test
    fun `SeriesStat2 reset clears both`() {
        val s = Sum() + Mean()
        s.update(5.0)
        s.reset()
        assertEquals(0.0, s.read().first.sum, DELTA)
        assertEquals(0.0, s.read().second.totalWeights, DELTA)
    }

    @Test
    fun `SeriesStat2 create produces fresh stat`() {
        val s1 = Sum(AtomicMode, "a") + Mean(AtomicMode, "b")
        s1.update(10.0)
        val s2 = s1.create(SerialMode)
        s2.update(5.0)
        // s1 must not see s2's update
        assertEquals(10.0, s1.read().first.sum, DELTA)
    }

    @Test
    fun `SeriesStat2 withName sets outer name`() {
        val s = (Sum() + Mean()).withName("metrics")
        assertEquals("metrics", s.name)
    }

    @Test
    fun `SeriesStat3 via plus operator`() {
        val s = Sum() + Mean() + Variance()
        s.update(1.0)
        s.update(3.0)
        val r = s.read()
        assertEquals(4.0, r.first.sum, DELTA)
        assertEquals(2.0, r.second.mean, DELTA)
        assertEquals(1.0, r.third.variance, DELTA)
    }

    @Test
    fun `SeriesStat3 create produces fresh stat`() {
        val s1 = Sum() + Mean() + Variance()
        s1.update(10.0)
        val s2 = s1.create(SerialMode)
        s2.update(5.0)
        assertEquals(10.0, s1.read().first.sum, DELTA)
        assertEquals(5.0, s2.read().first.sum, DELTA)
    }

    @Test
    fun `SeriesStat4 via plus operator`() {
        val s = Sum() + Mean() + Variance() + Moments()
        s.update(1.0)
        s.update(3.0)
        s.update(5.0)
        val r = s.read()
        assertEquals(9.0, r.first.sum, DELTA)
        assertEquals(3.0, r.second.mean, DELTA)
    }

    @Test
    fun `SeriesStat4 create produces fresh stat`() {
        val s1 = Sum() + Mean() + Variance() + Moments()
        s1.update(10.0)
        val s2 = s1.create(SerialMode)
        s2.update(20.0)
        assertEquals(10.0, s1.read().first.sum, DELTA)
        assertEquals(20.0, s2.read().first.sum, DELTA)
    }

    @Test
    fun `SeriesStat4 withName`() {
        val s = (Sum() + Mean() + Variance() + Moments()).withName("quad")
        assertEquals("quad", s.name)
    }

    @Test
    fun `Result2 round-trips through JSON with typed decode`() {
        val stat = Sum(name = "s") + Mean(name = "m")
        stat.update(2.0)
        stat.update(4.0)
        val json = Json.encodeToString(stat.read())
        val decoded: Result2<SumResult, WeightedMeanResult> = Json.decodeFromString(json)
        assertEquals(6.0, decoded.first.sum, DELTA)
        assertEquals(3.0, decoded.second.mean, DELTA)
    }

    @Test
    fun `Result3 round-trips through JSON with typed decode`() {
        val stat = Sum(name = "s") + Mean(name = "m") + Variance(name = "v")
        stat.update(1.0)
        stat.update(3.0)
        val json = Json.encodeToString(stat.read())
        val decoded: Result3<SumResult, WeightedMeanResult, WeightedVarianceResult> = Json.decodeFromString(json)
        assertEquals(4.0, decoded.first.sum, DELTA)
        assertEquals(2.0, decoded.second.mean, DELTA)
        assertEquals(1.0, decoded.third.variance, DELTA)
    }

    @Test
    fun `Result2 round-trips through protobuf with typed decode`() {
        val stat = Sum(name = "s") + Mean(name = "m")
        stat.update(2.0)
        stat.update(4.0)
        val bytes = ProtoBuf.encodeToByteArray(stat.read())
        val decoded: Result2<SumResult, WeightedMeanResult> = ProtoBuf.decodeFromByteArray(bytes)
        assertEquals(6.0, decoded.first.sum, DELTA)
        assertEquals(3.0, decoded.second.mean, DELTA)
    }

    @Test
    fun `Result3 round-trips through protobuf with typed decode`() {
        val stat = Sum(name = "s") + Mean(name = "m") + Variance(name = "v")
        stat.update(1.0)
        stat.update(3.0)
        val bytes = ProtoBuf.encodeToByteArray(stat.read())
        val decoded: Result3<SumResult, WeightedMeanResult, WeightedVarianceResult> = ProtoBuf.decodeFromByteArray(
            bytes
        )
        assertEquals(4.0, decoded.first.sum, DELTA)
        assertEquals(2.0, decoded.second.mean, DELTA)
        assertEquals(1.0, decoded.third.variance, DELTA)
    }

    @Test
    fun `Result4 round-trips through protobuf with typed decode`() {
        val stat = Sum(name = "s") + Mean(name = "m") + Variance(name = "v") + Moments(name = "mo")
        stat.update(1.0)
        stat.update(3.0)
        stat.update(5.0)
        val bytes = ProtoBuf.encodeToByteArray(stat.read())
        val decoded: Result4<SumResult, WeightedMeanResult, WeightedVarianceResult, MomentsResult> =
            ProtoBuf.decodeFromByteArray(bytes)
        assertEquals(9.0, decoded.first.sum, DELTA)
        assertEquals(3.0, decoded.second.mean, DELTA)
        assertEquals(8.0 / 3.0, decoded.third.variance, DELTA)
        assertEquals(3.0, decoded.fourth.mean, DELTA)
    }
}
