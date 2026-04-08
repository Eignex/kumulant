@file:Suppress("SERIALIZER_TYPE_INCOMPATIBLE")

package com.eignex.kumulant.core

import com.eignex.kumulant.concurrent.StreamMode
import kotlinx.serialization.*
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

interface ComposedStat {
    fun withName(name: String): ComposedStat
}

@Serializable
@SerialName("ResultList")
data class ResultList<out R : Result>(
    val results: List<R>,
    override val name: String? = null
) : Result, ComposedStat {
    override fun withName(name: String) = ResultList<R>(results, name)
}

class ResultDelegateSerializer<T : Result> : KSerializer<T> {

    private val delegate = Result.serializer()
    override val descriptor = delegate.descriptor

    override fun serialize(encoder: Encoder, value: T) {
        encoder.encodeSerializableValue(delegate, value as Result)
    }

    @Suppress("UNCHECKED_CAST")
    override fun deserialize(decoder: Decoder): T {
        return decoder.decodeSerializableValue(delegate) as T
    }
}

@Serializable
@SerialName("Result2")
data class Result2<A : Result, B : Result>(
    @Serializable(with = ResultDelegateSerializer::class) val first: A,
    @Serializable(with = ResultDelegateSerializer::class) val second: B,
    override val name: String? = null
) : Result

@Serializable
@SerialName("Result3")
data class Result3<A : Result, B : Result, C : Result>(
    @Serializable(with = ResultDelegateSerializer::class) val first: A,
    @Serializable(with = ResultDelegateSerializer::class) val second: B,
    @Serializable(with = ResultDelegateSerializer::class) val third: C,
    override val name: String? = null
) : Result

@Serializable
@SerialName("Result4")
data class Result4<A : Result, B : Result, C : Result, D : Result>(
    @Serializable(with = ResultDelegateSerializer::class) val first: A,
    @Serializable(with = ResultDelegateSerializer::class) val second: B,
    @Serializable(with = ResultDelegateSerializer::class) val third: C,
    @Serializable(with = ResultDelegateSerializer::class) val fourth: D,
    override val name: String? = null
) : Result

class SeriesStat2<A : Result, B : Result>(
    val s1: SeriesStat<A>,
    val s2: SeriesStat<B>,
    override val name: String? = null
) : SeriesStat<Result2<A, B>>, ComposedStat {

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        s1.update(value, timestampNanos, weight)
        s2.update(value, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long) =
        Result2<A, B>(
            s1.read(timestampNanos),
            s2.read(timestampNanos),
            name
        )

    override fun merge(values: Result2<A, B>) {
        s1.merge(values.first)
        s2.merge(values.second)
    }

    override fun reset() {
        s1.reset()
        s2.reset()
    }

    override fun withName(name: String) = SeriesStat2(s1, s2, name)

    override fun copy(mode: StreamMode?, name: String?) =
        SeriesStat2(s1.copy(mode), s2.copy(mode), name ?: this.name)
}

class SeriesStat3<A : Result, B : Result, C : Result>(
    val s1: SeriesStat<A>,
    val s2: SeriesStat<B>,
    val s3: SeriesStat<C>,
    override val name: String? = null
) : SeriesStat<Result3<A, B, C>>, ComposedStat {

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        s1.update(value, timestampNanos, weight)
        s2.update(value, timestampNanos, weight)
        s3.update(value, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long): Result3<A, B, C> {
        val r1 = s1.read(timestampNanos)
        val r2 = s2.read(timestampNanos)
        val r3 = s3.read(timestampNanos)
        return Result3(r1, r2, r3, name)
    }

    override fun merge(values: Result3<A, B, C>) {
        s1.merge(values.first)
        s2.merge(values.second)
        s3.merge(values.third)
    }

    override fun reset() {
        s1.reset()
        s2.reset()
        s3.reset()
    }

    override fun withName(name: String) = SeriesStat3(s1, s2, s3, name)

    override fun copy(mode: StreamMode?, name: String?) =
        SeriesStat3(s1.copy(mode), s2.copy(mode), s3.copy(mode), name ?: this.name)
}

class SeriesStat4<A : Result, B : Result, C : Result, D : Result>(
    val s1: SeriesStat<A>,
    val s2: SeriesStat<B>,
    val s3: SeriesStat<C>,
    val s4: SeriesStat<D>,
    override val name: String? = null
) : SeriesStat<Result4<A, B, C, D>> {

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        s1.update(value, timestampNanos, weight)
        s2.update(value, timestampNanos, weight)
        s3.update(value, timestampNanos, weight)
        s4.update(value, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long): Result4<A, B, C, D> {
        val r1 = s1.read(timestampNanos)
        val r2 = s2.read(timestampNanos)
        val r3 = s3.read(timestampNanos)
        val r4 = s4.read(timestampNanos)
        return Result4(r1, r2, r3, r4, name)
    }

    override fun merge(values: Result4<A, B, C, D>) {
        s1.merge(values.first)
        s2.merge(values.second)
        s3.merge(values.third)
        s4.merge(values.fourth)
    }

    override fun reset() {
        s1.reset()
        s2.reset()
        s3.reset()
        s4.reset()
    }

    override fun copy(mode: StreamMode?, name: String?) =
        SeriesStat4(s1.copy(mode), s2.copy(mode), s3.copy(mode), s4.copy(mode), name ?: this.name)
}

operator fun <A : Result, B : Result> SeriesStat<A>.plus(
    other: SeriesStat<B>
) = SeriesStat2(this, other)

operator fun <A : Result, B : Result, C : Result> SeriesStat2<A, B>.plus(
    other: SeriesStat<C>
) = SeriesStat3(s1, s2, other)

operator fun <A : Result, B : Result, C : Result> SeriesStat<A>.plus(
    other: SeriesStat2<B, C>
) = SeriesStat3(this, other.s1, other.s2)

operator fun <A : Result, B : Result, C : Result, D : Result> SeriesStat3<A, B, C>.plus(
    other: SeriesStat<D>
) = SeriesStat4(s1, s2, s3, other)

operator fun <A : Result, B : Result, C : Result, D : Result> SeriesStat<A>.plus(
    other: SeriesStat3<B, C, D>
) = SeriesStat4(this, other.s1, other.s2, other.s3)
