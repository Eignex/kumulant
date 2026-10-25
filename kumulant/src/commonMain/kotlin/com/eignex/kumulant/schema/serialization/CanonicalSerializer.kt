@file:OptIn(InternalSerializationApi::class)
@file:Suppress("UNCHECKED_CAST")

package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.stream.AtomicReference
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SealedClassSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*
import kotlinx.serialization.serializer
import kotlin.reflect.KClass

internal inline fun <reified T : Any> canonical(vararg nodes: WireNode): KSerializer<T> =
    CanonicalSerializer<T>(T::class, *nodes)

internal inline fun <reified T : Any> canonical(noinline nodes: () -> List<WireNode>): KSerializer<T> =
    CanonicalSerializer<T>(T::class, nodes)

private val catalogs = AtomicReference(emptyMap<String, List<WireNode>>())
internal val KSerializer<*>.nodes: List<WireNode> get() = descriptor.nodes!!
internal val SerialDescriptor.nodes: List<WireNode>? get() = catalogs.load()[serialName.removeSuffix("?")]

private fun register(descriptor: SerialDescriptor, nodes: List<WireNode>) {
    while (true) {
        val current = catalogs.load()
        if (descriptor.serialName in current) return
        if (catalogs.compareAndSet(current, current + (descriptor.serialName to nodes))) return
    }
}

internal inline fun <reified T : Any> node(
    name: String,
    shape: WireShape = WireShape.Fields,
    vararg operands: String,
): WireNode = WireNode(T::class, name, shape, operands.toList()) { serializer<T>() }

internal inline fun <reified T : Any> node(name: String, noinline bodySerializer: () -> KSerializer<T>): WireNode =
    WireNode(T::class, name, serializer = bodySerializer)

internal enum class WireShape { Fields, Literal, Operand, Operands, Decay, Smoothing }

internal class WireNode(
    val type: KClass<*>,
    val name: String,
    val shape: WireShape = WireShape.Fields,
    val operands: List<String> = emptyList(),
    serializer: () -> KSerializer<*>,
) {
    val serializer: KSerializer<Any> by lazy { serializer() as KSerializer<Any> }

    fun encode(body: JsonObject): JsonElement = when (shape) {
        WireShape.Fields -> body

        WireShape.Literal, WireShape.Operand -> body.getValue(operands.single())

        WireShape.Operands -> JsonArray(operands.map { body.getValue(it) })

        WireShape.Decay -> JsonObject(mapOf(operands.single() to body.getValue("weighting").jsonObject.values.single()))

        WireShape.Smoothing -> JsonObject(
            body.map { (key, value) ->
                val unwrapped = if (key.endsWith("Weighting")) value.jsonObject.getValue("alpha") else value
                key.removeSuffix("Weighting") to unwrapped
            }.toMap(),
        )
    }

    fun decode(payload: JsonElement, path: String): JsonObject = when (shape) {
        WireShape.Fields -> payload.objectAt(path)

        WireShape.Literal, WireShape.Operand -> JsonObject(mapOf(operands.single() to payload))

        WireShape.Operands -> {
            val array = payload as? JsonArray ?: invalid(path, "expected an operand array")
            if (array.size != operands.size) invalid(path, "expected ${operands.size} operands")
            JsonObject(operands.zip(array).toMap())
        }

        WireShape.Decay -> {
            val body = payload.objectAt(path)
            val field = operands.single()
            if (body.keys != setOf(field)) invalid(path, "expected only '$field'")
            val parameter = if (field == "alpha") "alpha" else "durationMillis"
            JsonObject(mapOf("weighting" to JsonObject(mapOf(parameter to body.getValue(field)))))
        }

        WireShape.Smoothing -> JsonObject(
            payload.objectAt(path).map { (key, value) ->
                if (key.endsWith("Weighting")) invalid("$path.$key", "unknown field '$key'")
                if (key in setOf("alpha", "beta", "gamma")) {
                    "${key}Weighting" to JsonObject(mapOf("alpha" to value))
                } else {
                    key to value
                }
            }.toMap(),
        )
    }
}

internal class CanonicalSerializer<T : Any>(private val base: KClass<*>, nodes: () -> List<WireNode>) :
    KSerializer<T> {
    constructor(base: KClass<*>, vararg nodes: WireNode) : this(base, { nodes.toList() })

    internal val nodes by lazy(nodes)
    private val legacy by lazy {
        SealedClassSerializer(
            base.qualifiedName!!,
            base as KClass<T>,
            this.nodes.map { it.type as KClass<out T> }.toTypedArray(),
            this.nodes.map { it.serializer as KSerializer<out T> }.toTypedArray(),
        )
    }
    override val descriptor: SerialDescriptor by lazy {
        legacy.descriptor.also { register(it, this.nodes) }
    }

    override fun serialize(encoder: Encoder, value: T) {
        if (encoder !is JsonEncoder) {
            encoder.encodeSerializableValue(legacy, value)
            return
        }
        val node = nodes.singleOrNull { it.type == value::class }
            ?: invalid("$", "unsupported ${base.simpleName} implementation")
        val body = encoder.json.encodeToJsonElement(node.serializer, value).jsonObject
        val payload = node.encode(body)
        encoder.encodeJsonElement(
            when {
                node.shape == WireShape.Literal -> payload
                payload is JsonObject && payload.isEmpty() -> JsonPrimitive(node.name)
                else -> JsonObject(mapOf(node.name to payload))
            },
        )
    }

    override fun deserialize(decoder: Decoder): T {
        if (decoder !is JsonDecoder) return decoder.decodeSerializableValue(legacy)
        val element = decoder.decodeJsonElement()
        validateJson(descriptor, element, "$")
        val (node, payload) = selectNode(nodes, element, "$")
        val path = pathFor(node, "$")
        try {
            return decoder.json.decodeFromJsonElement(node.serializer, node.decode(payload, path)) as T
        } catch (error: IllegalArgumentException) {
            throw SerializationException("$path: ${error.message}", error)
        }
    }
}
