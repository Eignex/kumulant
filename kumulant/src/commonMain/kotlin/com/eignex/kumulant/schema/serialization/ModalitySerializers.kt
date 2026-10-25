@file:Suppress("UNCHECKED_CAST")

package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.schema.spec.*
import kotlinx.serialization.KSerializer

@Suppress("UNUSED_PARAMETER")
internal class SeriesStatSpecSerializer<R : Result>(resultSerializer: KSerializer<R>) :
    CanonicalSerializer<SeriesStatSpec<R>>(
        SeriesStatSpec::class,
        { seriesStatSpecNodes },
    )

@Suppress("UNUSED_PARAMETER")
internal class PairedStatSpecSerializer<R : Result>(resultSerializer: KSerializer<R>) :
    CanonicalSerializer<PairedStatSpec<R>>(
        PairedStatSpec::class,
        { pairedStatSpecNodes },
    )

@Suppress("UNUSED_PARAMETER")
internal class VectorStatSpecSerializer<R : Result>(resultSerializer: KSerializer<R>) :
    CanonicalSerializer<VectorStatSpec<R>>(
        VectorStatSpec::class,
        { vectorStatSpecNodes },
    )

@Suppress("UNUSED_PARAMETER")
internal class DiscreteStatSpecSerializer<R : Result>(resultSerializer: KSerializer<R>) :
    CanonicalSerializer<DiscreteStatSpec<R>>(
        DiscreteStatSpec::class,
        { discreteStatSpecNodes },
    )

@Suppress("UNUSED_PARAMETER")
internal class RegressionStatSpecSerializer<R : Result>(resultSerializer: KSerializer<R>) :
    CanonicalSerializer<RegressionStatSpec<R>>(
        RegressionStatSpec::class,
        { regressionStatSpecNodes },
    )
