package com.eignex.kumulant.stream

/**
 * SplitMix64 — a fast, high-quality 64-bit mixer suitable for spreading sequential
 * or low-entropy keys into a uniform 64-bit hash before feeding them to cardinality
 * sketches. Output passes BigCrush; not collision-resistant (use a cryptographic hash
 * if adversarial input is a concern).
 */
fun splitmix64(value: Long): Long {
    var z = value + -7046029254386353133L // 0x9E3779B97F4A7C15
    z = (z xor (z ushr 30)) * -4658895280553007687L // 0xBF58476D1CE4E5B9
    z = (z xor (z ushr 27)) * -6534734903238641935L // 0x94D049BB133111EB
    return z xor (z ushr 31)
}
