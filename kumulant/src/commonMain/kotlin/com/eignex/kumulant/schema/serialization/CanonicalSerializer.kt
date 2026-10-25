@file:OptIn(kotlinx.serialization.InternalSerializationApi::class)
@file:Suppress("UNCHECKED_CAST")

package com.eignex.kumulant.schema.serialization

import kotlinx.serialization.KSerializer
import kotlinx.serialization.SealedClassSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*
import kotlinx.serialization.serializer
import kotlin.reflect.KClass

internal inline fun <reified T : Any> node(
    name: String,
    shape: WireShape = WireShape.Fields,
    vararg operands: String,
): WireNode = WireNode(T::class, name, shape, operands.toList()) { serializer<T>() }

internal inline fun <reified T : Any> node(name: String, noinline bodySerializer: () -> KSerializer<T>): WireNode =
    WireNode(T::class, name, serializer = bodySerializer)

internal enum class WireShape { Fields, Literal, Reference, Operand, Operands, Decay, Smoothing, Filter }

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

        WireShape.Filter -> JsonObject(body.mapKeys { (key, _) -> if (key == "pred") "expr" else key })

        WireShape.Literal, WireShape.Operand -> body.getValue(operands.single())

        WireShape.Reference -> JsonPrimitive(operands.single())

        WireShape.Operands -> JsonArray(operands.map { body.getValue(it) })

        WireShape.Decay -> JsonObject(mapOf(operands.single() to body.getValue("weighting").jsonObject.values.single()))

        WireShape.Smoothing -> JsonObject(
            body.map { (key, value) ->
                if (key.endsWith("Weighting")) {
                    key.removeSuffix("Weighting") to value.jsonObject.getValue("alpha")
                } else {
                    key to value
                }
            }.toMap(),
        )
    }

    fun decode(payload: JsonElement, path: String): JsonObject = when (shape) {
        WireShape.Fields -> payload.objectAt(path)

        WireShape.Filter -> {
            val body = payload.objectAt(path)
            if ("pred" in body) invalid("$path.pred", "unknown field 'pred'; use 'expr'")
            JsonObject(body.mapKeys { (key, _) -> if (key == "expr") "pred" else key })
        }

        WireShape.Literal, WireShape.Operand -> JsonObject(mapOf(operands.single() to payload))

        WireShape.Reference -> {
            if (payload != JsonPrimitive(operands.single())) invalid(path, "invalid reference")
            JsonObject(emptyMap())
        }

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

internal open class CanonicalSerializer<T : Any>(internal val base: KClass<*>, nodes: () -> List<WireNode>) :
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
    override val descriptor get() = legacy.descriptor

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
            if (node.shape == WireShape.Literal) payload else JsonObject(mapOf(node.name to payload)),
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
        } catch (error: SerializationException) {
            throw SerializationException("$path: ${error.message}", error)
        } catch (error: IllegalArgumentException) {
            throw SerializationException("$path: ${error.message}", error)
        }
    }
}
