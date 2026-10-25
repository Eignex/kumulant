package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.stat.regression.glm.FactorisedGaussian
import com.eignex.kumulant.stat.regression.glm.LinUcb
import com.eignex.kumulant.stat.regression.glm.MultivariateGaussian
import com.eignex.kumulant.stat.regression.glm.PointPosterior
import com.eignex.kumulant.stat.regression.tree.MeanForestPosterior
import com.eignex.kumulant.stat.regression.tree.MeanTreePosterior
import com.eignex.kumulant.stat.regression.tree.ThompsonForestPosterior
import com.eignex.kumulant.stat.regression.tree.ThompsonTreePosterior
import com.eignex.kumulant.stat.regression.tree.UcbForestPosterior
import com.eignex.kumulant.stat.regression.tree.UcbTreePosterior
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SealedClassSerializer
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

/** Serializes built-in [RegressionPosterior] implementations without a caller-supplied module. */
@OptIn(InternalSerializationApi::class)
object RegressionPosteriorSerializer : KSerializer<RegressionPosterior<*>> {
    private val delegate = SealedClassSerializer(
        "com.eignex.kumulant.stat.regression.RegressionPosterior",
        RegressionPosterior::class,
        arrayOf(
            PointPosterior::class,
            FactorisedGaussian::class,
            MultivariateGaussian::class,
            LinUcb::class,
            MeanTreePosterior::class,
            ThompsonTreePosterior::class,
            UcbTreePosterior::class,
            MeanForestPosterior::class,
            ThompsonForestPosterior::class,
            UcbForestPosterior::class,
        ),
        arrayOf(
            PointPosterior.serializer(),
            FactorisedGaussian.serializer(),
            MultivariateGaussian.serializer(),
            LinUcb.serializer(),
            MeanTreePosterior.serializer(),
            ThompsonTreePosterior.serializer(),
            UcbTreePosterior.serializer(),
            MeanForestPosterior.serializer(),
            ThompsonForestPosterior.serializer(),
            UcbForestPosterior.serializer(),
        ),
    )

    override val descriptor get() = delegate.descriptor

    // Dispatch through the format so JSON can apply its configured polymorphic discriminator.
    override fun serialize(encoder: Encoder, value: RegressionPosterior<*>) =
        encoder.encodeSerializableValue(delegate, value)

    override fun deserialize(decoder: Decoder): RegressionPosterior<*> = decoder.decodeSerializableValue(delegate)
}
