package com.eignex.kumulant.schema.serialization

import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.*
import kotlinx.serialization.json.*

internal fun validateJson(descriptor: SerialDescriptor, element: JsonElement, path: String) {
    if (element == JsonNull) {
        if (!descriptor.isNullable) invalid(path, "null is not allowed")
        return
    }
    if (descriptor.isInline) {
        validateJson(descriptor.getElementDescriptor(0), element, path)
        return
    }
    val nodes = descriptor.nodes
    if (nodes != null) {
        val (node, payload) = selectNode(nodes, element, path)
        val body = node.decode(payload, pathFor(node, path))
        validateRecord(node.serializer.descriptor, body, pathFor(node, path)) { field ->
            when (node.shape) {
                WireShape.Literal, WireShape.Reference -> path
                WireShape.Operand, WireShape.Decay -> pathFor(node, path)
                WireShape.Operands -> "${pathFor(node, path)}[${node.operands.indexOf(field)}]"
                WireShape.Smoothing -> "${pathFor(node, path)}.${field.removeSuffix("Weighting")}"
                WireShape.Fields -> "${pathFor(node, path)}.$field"
                WireShape.Filter -> "${pathFor(node, path)}.${if (field == "pred") "expr" else field}"
            }
        }
        return
    }
    when (descriptor.kind) {
        StructureKind.CLASS, StructureKind.OBJECT -> validateRecord(descriptor, element.objectAt(path), path)

        StructureKind.LIST -> {
            val array = element as? JsonArray ?: invalid(path, "expected an array")
            val child = descriptor.getElementDescriptor(0)
            array.forEachIndexed { index, value -> validateJson(child, value, "$path[$index]") }
        }

        StructureKind.MAP -> element.objectAt(path).forEach { (key, value) ->
            validateJson(descriptor.getElementDescriptor(1), value, "$path.$key")
        }

        SerialKind.ENUM -> {
            val literal = element as? JsonPrimitive
            val names = (0 until descriptor.elementsCount).map { descriptor.getElementName(it) }
            if (literal?.isString != true || literal.content !in names) invalid(path, "expected one of $names")
        }

        else -> validatePrimitive(descriptor, element, path)
    }
}

private fun validateRecord(
    descriptor: SerialDescriptor,
    body: JsonObject,
    path: String,
    fieldPath: (String) -> String = { "$path.$it" },
) {
    val fields = (0 until descriptor.elementsCount).associateBy { descriptor.getElementName(it) }
    body.keys.firstOrNull { it !in fields }?.let { invalid("$path.$it", "unknown field '$it'") }
    for ((name, index) in fields) {
        val value = body[name]
        val child = descriptor.getElementDescriptor(index)
        val required = !descriptor.isElementOptional(index) && !child.isNullable
        if (value == null) {
            if (required) invalid(fieldPath(name), "missing operand '$name'")
        } else {
            validateJson(child, value, fieldPath(name))
        }
    }
}

private fun validatePrimitive(descriptor: SerialDescriptor, element: JsonElement, path: String) {
    val primitive = element as? JsonPrimitive ?: invalid(path, "expected ${descriptor.kind}")
    val valid = when (descriptor.kind) {
        PrimitiveKind.BOOLEAN -> !primitive.isString && primitive.booleanOrNull != null

        PrimitiveKind.BYTE, PrimitiveKind.SHORT, PrimitiveKind.INT, PrimitiveKind.LONG ->
            !primitive.isString && primitive.longOrNull != null

        PrimitiveKind.FLOAT, PrimitiveKind.DOUBLE -> !primitive.isString && primitive.doubleOrNull != null

        PrimitiveKind.CHAR, PrimitiveKind.STRING -> primitive.isString

        else -> true
    }
    if (!valid) invalid(path, "expected ${descriptor.kind}")
}

internal fun selectNode(nodes: List<WireNode>, element: JsonElement, path: String): Pair<WireNode, JsonElement> {
    if (element is JsonPrimitive && !element.isString && element.doubleOrNull != null) {
        val literal = nodes.singleOrNull { it.shape == WireShape.Literal }
            ?: invalid(path, "expected an operator object")
        return literal to element
    }
    val obj = element.objectAt(path)
    if (obj.size != 1) invalid(path, "expected exactly one operator")
    val (operator, operand) = obj.entries.single()
    val candidates = nodes.filter {
        it.name == operator && it.shape != WireShape.Literal &&
            (it.shape != WireShape.Reference || operand == JsonPrimitive(it.operands.single()))
    }
    val node = when (candidates.size) {
        0 -> invalid("$path.$operator", "unknown operator or reference '$operator'")

        1 -> candidates.single()

        else -> {
            val innerPath = "$path.$operator.inner"
            val inner = operand.objectAt("$path.$operator")["inner"] ?: invalid(innerPath, "missing inner spec")
            val innerNode = selectNode(StatSpecSerializer.nodes, inner, innerPath).first
            candidates.singleOrNull { statModalities[it.type] == statModalities[innerNode.type] }
                ?: invalid(innerPath, "operator '$operator' does not support this input modality")
        }
    }
    return node to operand
}

internal fun pathFor(node: WireNode, path: String): String =
    if (node.shape == WireShape.Literal) path else "$path.${node.name}"

internal fun JsonElement.objectAt(path: String): JsonObject = this as? JsonObject ?: invalid(path, "expected an object")

internal fun invalid(path: String, message: String): Nothing = throw SerializationException("$path: $message")
