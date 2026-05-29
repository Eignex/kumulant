package com.eignex.kumulant.schema

import com.eignex.kumulant.math.HasherRef
import com.eignex.kumulant.math.Hashers
import com.eignex.kumulant.math.LongHasher
import com.eignex.kumulant.math.splitmix64
import com.eignex.kumulant.stat.sketch.contains
import com.eignex.kumulant.stat.sketch.estimate
import com.eignex.skema.SchemaJson
import kotlinx.serialization.encodeToString
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The discrete sketch specs carry a [LongHasher] by name; a custom mixer is supplied through
 * the [Hashers] registry and referenced by that name on the wire. These tests cover the full
 * path: register, serialize, materialize, and re-query via the result-side helpers.
 */
class HasherConfigTest {

    private object SaltedSplitMix : LongHasher {
        override val name: String = "test-salted-splitmix"
        override fun mix(value: Long): Long = splitmix64(value xor 0x1234_5678_9ABC_DEF0L)
    }

    @BeforeTest
    fun registerCustomHasher() {
        Hashers.register(SaltedSplitMix)
    }

    @Test
    fun `specs default to the splitmix64 hasher`() {
        assertEquals(HasherRef.SplitMix64, HyperLogLog().hasher)
        assertEquals(HasherRef.SplitMix64, BloomFilter().hasher)
        assertEquals(HasherRef.SplitMix64, CountMinSketch().hasher)
    }

    @Test
    fun `custom hasher name survives the wire as a plain string`() {
        val spec = CountMinSketch(depth = 5, width = 1024, hasher = HasherRef(SaltedSplitMix.name))
        val json = SchemaJson.encodeToString<StatSpec>(spec)
        assertTrue(json.contains(SaltedSplitMix.name), "expected hasher name in $json")
        assertEquals(spec, SchemaJson.decodeFromString<StatSpec>(json))
    }

    @Test
    fun `CountMinSketch materializes with the custom hasher and the result re-resolves it`() {
        val cms = CountMinSketch(depth = 5, width = 1024, hasher = HasherRef(SaltedSplitMix.name)).materialize()
        cms.update(42L)
        val result = cms.read()
        assertEquals(HasherRef(SaltedSplitMix.name), result.hasher)
        // estimate() resolves the recorded hasher, so it lands on the same counters as the update.
        assertEquals(1L, result.estimate(42L))
    }

    @Test
    fun `BloomFilter materializes with the custom hasher and contains re-resolves it`() {
        val bf = BloomFilter(bits = 4096, hashes = 4, hasher = HasherRef(SaltedSplitMix.name)).materialize()
        bf.update(7L)
        val result = bf.read()
        assertEquals(HasherRef(SaltedSplitMix.name), result.hasher)
        assertTrue(result.contains(7L))
        assertFalse(result.contains(123_456_789L))
    }

    @Test
    fun `unknown hasher name fails fast at materialization`() {
        assertFailsWith<IllegalStateException> {
            HyperLogLog(precision = 10, hasher = HasherRef("no-such-hasher")).materialize()
        }
    }
}
