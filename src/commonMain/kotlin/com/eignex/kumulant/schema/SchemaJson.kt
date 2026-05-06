package com.eignex.kumulant.schema

import kotlinx.serialization.json.Json

/**
 * Recommended `Json` configuration for serializing [StatSchemaDef] over HTTP or
 * to JSON sidecar files. Uses `@type` as the polymorphic discriminator and
 * suppresses defaults so authored payloads stay terse.
 *
 * For YAML cloud configs, configure your YAML library (e.g. kaml) with the
 * matching `classDiscriminator = "@type"` so producers and consumers agree.
 *
 * Consumers who want a different shape (different discriminator, pretty-print,
 * lenient parsing) can build their own `Json {}` and reuse the same
 * polymorphic `StatConfig` hierarchy.
 */
val SchemaJson: Json = Json {
    classDiscriminator = "@type"
    encodeDefaults = false
    explicitNulls = false
    prettyPrint = false
}
