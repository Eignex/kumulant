# Canonical structured JSON

Kumulant owns the JSON encoding of `StatSpec`, its five modality roots,
`ScalarExpr`, `BoolExpr`, `VectorExpr`, and the configuration and bandit roots
listed below. Use the ordinary kotlinx.serialization entry points with `Json`
or skema's `SchemaJson`; no serializers module, parser, or application schema is
required. The Kotlin DSL, evaluation, and materialization use the same typed
objects after decoding.

```kotlin
val spec: StatSpec = EwmaMean(Alpha(0.1)).transform(X + 1.0)
val json = Json.encodeToString(spec)
val restored = Json.decodeFromString<StatSpec>(json)
```

```json
{
  "transform": {
    "expr": {"add": [{"input": "value"}, 1]},
    "inner": {"ewma": {"alpha": 0.1}}
  }
}
```

A plain EWMA is `{"ewma":{"alpha":0.1}}`.

## Conventions

- Each node is an object with exactly one operator key. Operator names are
  explicit library contracts; neither Kotlin class names nor the JSON
  `classDiscriminator` setting determines them.
- Scalar constants are JSON numbers, including integer literals, negative
  values, and fractional values. Encoding uses Kotlin's numeric representation
  (`1.0` is equivalent to the accepted literal `1`). Expression strings and
  objects such as `{"const":1}` are rejected.
- Parameterless nodes have an empty object payload, for example `{"mean":{}}`.
  Ordinary configuration records have named fields. Binary operators take
  exactly two operands in order; unary operators take their operand directly.
- A decorator contains `inner`; nesting defines operation order. Common
  decorators use the same operator across modalities. Their inner tree selects
  the modality, so `filter` works for series, paired, vector, discrete, and
  regression specs without class-name suffixes. Modality-changing operators
  have distinct names and remain explicit tree nodes.
- Tables below show the complete field set. A field with `?` may be omitted
  because its Kotlin constructor supplies a default. `?` is documentation
  notation, not part of the JSON key. Missing required fields are rejected.
  Fields keep their declared Kotlin types; lists are JSON arrays, string-keyed
  maps are objects, and nullable fields accept `null`.
- `encodeDefaults` and `explicitNulls` retain their normal kotlinx.serialization
  meaning for every record. The default `Json` and `SchemaJson` omit defaults;
  `Json { encodeDefaults = true }` emits them. Decoding an omitted default and
  encoding it with the same settings produces the same shape. Object key order
  is insignificant. There is one node grammar regardless of those settings.
- Unknown operators, unknown record fields, incorrect operand counts, and
  invalid primitive shapes raise `SerializationException` with a path such as
  `$.transform.expr.add[1].input`. Shape validation also applies when the caller
  sets `ignoreUnknownKeys` or `coerceInputValues`; these settings do not add
  accepted configuration grammars. Numerical constructor and materialization
  constraints remain the constraints of the Kotlin objects.
- Concurrency and live state are supplied at materialization time and do not
  belong in a spec payload.

Serialize configuration through its root or modality interface, including when
using type inference. The concrete generated serializers describe constructor
records and support embedding known record types; they are not configuration
entry points. `StatSchemaDef` and the bandit roots already select the correct
root serializers for their nested fields.

## Expressions

`scalar` means a `ScalarExpr`, `boolean` a `BoolExpr`, and `vector` a `VectorExpr`.
`input:value` reads `x`; `input:response` reads `y`. Optimizer schedules bind the
step counter to `x` exactly as in Kotlin. `input:index` reads the element index
from an `IndexedResult`. `element` reads a coordinate of the input vector.
Feedback references require the same result capabilities as the Kotlin nodes.

| Kotlin node | JSON shape |
| --- | --- |
| `X` | `{"input": "value"}` |
| `Y` | `{"input": "response"}` |
| `Center` | `{"feedback": "center"}` |
| `Scale` | `{"feedback": "scale"}` |
| `Low` | `{"feedback": "min"}` |
| `High` | `{"feedback": "max"}` |
| `VIndex` | `{"input": "index"}` |
| `V` | `{"element": index}` |
| `Const` | `number` |
| `Add` | `{"add": [left, right]}` |
| `Sub` | `{"sub": [left, right]}` |
| `Mul` | `{"mul": [left, right]}` |
| `Div` | `{"div": [left, right]}` |
| `Neg` | `{"neg": scalar}` |
| `Abs` | `{"abs": scalar}` |
| `Log` | `{"log": scalar}` |
| `Exp` | `{"exp": scalar}` |
| `Sqrt` | `{"sqrt": scalar}` |
| `Pow` | `{"pow": [left, right]}` |
| `MinExpr` | `{"min": [left, right]}` |
| `MaxExpr` | `{"max": [left, right]}` |
| `IfExpr` | `{"if": {cond, then, otherwise}}` |
| `Switch` | `{"switch": {on, cases, otherwise}}` |
| `Standardize` | `{"standardize": {}}` |
| `MinMax` | `{"minMax": {targetLow?, targetHigh?}}` |
| `VFold` | `{"fold": {op}}` |
| `VDot` | `{"dot": [number, ...]}` |
| `In` | `{"in": {of, values}}` |
| `Gt` | `{"gt": [left, right]}` |
| `Ge` | `{"ge": [left, right]}` |
| `Lt` | `{"lt": [left, right]}` |
| `Le` | `{"le": [left, right]}` |
| `Eq` | `{"eq": [left, right]}` |
| `And` | `{"and": [left, right]}` |
| `Or` | `{"or": [left, right]}` |
| `Not` | `{"not": boolean}` |
| `IsNaN` | `{"isNaN": scalar}` |
| `IsFinite` | `{"isFinite": scalar}` |
| `AllFinite` | `{"allFinite": {}}` |
| `InRange` | `{"inRange": {a, min, max}}` |
| `VElements` | `{"elements": [scalar, ...]}` |

`Switch.cases` is an array of records `{value: number, then: scalar}`; cases
retain their order. `In.values` is an array of numbers. `fold.op` is one of
`"Sum"`, `"Product"`, `"Mean"`, `"Min"`, `"Max"`, or `"Norm2"`. `dot` accepts
numeric coefficients, whereas `elements` accepts scalar expressions. Standardize
and min-max scaling retain their Kotlin behavior for degenerate scales.

## Statistical specs and decorators

All payloads below are nested under the operator key. `inner`, `primary`,
`secondary`, and other spec-valued fields contain canonical spec nodes, not
constructor records. Expression-valued fields contain canonical expressions.
Result type parameters do not appear in JSON. The modality roots accept their
own catalog; `StatSpec` accepts the entire catalog.

| Kotlin spec | Modality | Operator | Payload fields |
| --- | --- | --- | --- |
| `Mean` | series | `mean` | `{}` |
| `Sum` | series | `sum` | `{}` |
| `Min` | series | `min` | `{}` |
| `Max` | series | `max` | `{}` |
| `ArgMin` | series | `argMin` | `{}` |
| `ArgMax` | series | `argMax` | `{}` |
| `Range` | series | `range` | `{}` |
| `Excursion` | series | `excursion` | `{}` |
| `RunLength` | series | `runLength` | `{}` |
| `Recency` | series | `recency` | `{}` |
| `Crossing` | series | `crossing` | `{level}` |
| `Cusum` | series | `cusum` | `{target?, referenceValue?, threshold?}` |
| `PageHinkley` | series | `pageHinkley` | `{delta?, threshold?}` |
| `Adwin` | series | `adwin` | `{delta?, maxBucketsPerSize?}` |
| `Mad` | series | `mad` | `{compression?}` |
| `FrugalMad` | series | `frugalMad` | `{stepSize?}` |
| `ThresholdBucket` | series | `thresholdBucket` | `{thresholds}` |
| `Variance` | series | `variance` | `{}` |
| `Moments` | series | `moments` | `{}` |
| `ClassCounts` | series | `classCounts` | `{numClasses}` |
| `Summary` | series | `summary` | `{}` |
| `BernoulliSum` | series | `bernoulliSum` | `{}` |
| `TotalWeights` | series | `totalWeights` | `{}` |
| `Count` | series | `count` | `{}` |
| `Rate` | series | `rate` | `{}` |
| `CounterRate` | series | `counterRate` | `{treatDecreaseAsReset?}` |
| `DDSketch` | series | `ddSketch` | `{relativeError?, probabilities?, minIndexableValue?, maxIndexableValue?}` |
| `FrugalQuantile` | series | `frugalQuantile` | `{q, stepSize?, initialEstimate?}` |
| `HdrHistogram` | series | `hdrHistogram` | `{lowestDiscernibleValue?, initialHighestTrackableValue?, significantDigits?}` |
| `LinearHistogram` | series | `linearHistogram` | `{lowerBound, upperBound, binCount}` |
| `ReservoirHistogram` | series | `reservoirHistogram` | `{capacity?, seed}` |
| `TDigest` | series | `tDigest` | `{compression?, probabilities?}` |
| `PitHistogram` | series | `pitHistogram` | `{numBins}` |
| `PairedSum` | paired | `pairedSum` | `{}` |
| `UnivariateRegression` | paired | `univariateRegression` | `{penalty?}` |
| `Covariance` | paired | `covariance` | `{}` |
| `MseLoss` | paired | `mseLoss` | `{}` |
| `MaeLoss` | paired | `maeLoss` | `{}` |
| `LogLoss` | paired | `logLoss` | `{}` |
| `PinballLoss` | paired | `pinballLoss` | `{tau}` |
| `Auc` | paired | `auc` | `{numBins?, lowerBound?, upperBound?}` |
| `Reliability` | paired | `reliability` | `{numBins}` |
| `PlattCalibrator` | paired | `plattCalibrator` | `{optimizer?}` |
| `ConfusionMatrix` | paired | `confusionMatrix` | `{numClasses}` |
| `Accuracy` | paired | `accuracy` | `{numClasses}` |
| `HyperLogLog` | discrete | `hyperLogLog` | `{precision?, hasher?}` |
| `LinearCounting` | discrete | `linearCounting` | `{bits?, hasher?}` |
| `BloomFilter` | discrete | `bloomFilter` | `{bits?, hashes?, hasher?}` |
| `CountMinSketch` | discrete | `countMinSketch` | `{depth?, width?, seed?, hasher?}` |
| `MinHash` | discrete | `minHash` | `{numHashes?, seed?, hasher?}` |
| `Sojourn` | discrete | `sojourn` | `{states}` |
| `SpaceSaving` | discrete | `spaceSaving` | `{capacity}` |
| `BayesianRegression` | regression | `bayesianRegression` | `{featureSize, priorVariance?, link?}` |
| `StochasticRegression` | regression | `stochasticRegression` | `{featureSize, optimizer?, biasOptimizer?, penalty?, link?}` |
| `GaussianNaiveBayes` | regression | `gaussianNaiveBayes` | `{featureSize, numClasses, varianceFloor?}` |
| `SoftmaxRegression` | regression | `softmaxRegression` | `{featureSize, numClasses, optimizer?, biasOptimizer?}` |
| `DiagonalRegression` | regression | `diagonalRegression` | `{featureSize, priorPrecision?, learningRate?, penalty?, link?}` |
| `DecisionTreeRegression` | regression | `decisionTreeRegression` | `{featureSize, splitCandidates, config?, randomSeed?}` |
| `RandomForestRegression` | regression | `randomForestRegression` | `{featureSize, splitCandidates, nbrTrees?, config?, bagging?, randomSeed?}` |
| `DecisionTreeClassifier` | regression | `decisionTreeClassifier` | `{featureSize, numClasses, splitCandidates, config?, randomSeed?}` |
| `RandomForestClassifier` | regression | `randomForestClassifier` | `{featureSize, numClasses, splitCandidates, nbrTrees?, config?, bagging?, randomSeed?}` |
| `WithWeightSeries` | series | `withWeight` | `{inner, weight}` |
| `WithWeightPaired` | paired | `withWeight` | `{inner, weight}` |
| `WithWeightVector` | vector | `withWeight` | `{inner, weight}` |
| `WithWeightDiscrete` | discrete | `withWeight` | `{inner, weight}` |
| `WithValueSeries` | series | `withValue` | `{inner, value}` |
| `WithValueDiscrete` | discrete | `withValue` | `{inner, value}` |
| `AsSeries` | series | `asSeries` | `{inner}` |
| `AsDiscrete` | discrete | `asDiscrete` | `{inner}` |
| `AtX` | paired | `atX` | `{inner}` |
| `AtY` | paired | `atY` | `{inner}` |
| `AtIndex` | vector | `atIndex` | `{inner, index}` |
| `AtIndices` | vector | `atIndices` | `{inner, indexX, indexY}` |
| `WithFixedX` | series | `withFixedX` | `{inner, fixedX}` |
| `WithFixedY` | series | `withFixedY` | `{inner, fixedY}` |
| `WithTimeAsX` | series | `withTimeAsX` | `{inner}` |
| `WithTimeAsY` | series | `withTimeAsY` | `{inner}` |
| `WindowedSeries` | series | `window` | `{inner, durationMillis, slices?}` |
| `WindowedPaired` | paired | `window` | `{inner, durationMillis, slices?}` |
| `WindowedVector` | vector | `window` | `{inner, durationMillis, slices?}` |
| `WindowedDiscrete` | discrete | `window` | `{inner, durationMillis, slices?}` |
| `Vectorized` | vector | `vectorize` | `{dimensions, template, skipZeros?}` |
| `TransformValueSeries` | series | `transform` | `{inner, expr}` |
| `TransformValueDiscrete` | discrete | `transform` | `{inner, expr}` |
| `FilterValueSeries` | series | `filter` | `{inner: spec, expr: boolean}` |
| `FilterValueDiscrete` | discrete | `filter` | `{inner: spec, expr: boolean}` |
| `TransformPair` | paired | `transformPair` | `{inner, xExpr, yExpr}` |
| `FilterPaired` | paired | `filter` | `{inner: spec, expr: boolean}` |
| `TransformVectorElement` | vector | `transformElements` | `{inner, expr}` |
| `FilterVector` | vector | `filter` | `{inner: spec, expr: boolean}` |
| `FoldPaired` | paired | `foldPaired` | `{inner, expr}` |
| `FoldVector` | vector | `foldVector` | `{inner, expr}` |
| `FoldVectorPaired` | vector | `foldVectorPaired` | `{inner, xExpr, yExpr}` |
| `TransformVector` | vector | `transformVector` | `{inner, expr}` |
| `WeightByValueSeries` | series | `weightBy` | `{inner, expr}` |
| `WeightByValuePaired` | paired | `weightBy` | `{inner, expr}` |
| `WeightByValueVector` | vector | `weightBy` | `{inner, expr}` |
| `WeightByValueDiscrete` | discrete | `weightBy` | `{inner, expr}` |
| `ThrottleSeries` | series | `throttle` | `{inner, every}` |
| `ThrottlePaired` | paired | `throttle` | `{inner, every}` |
| `ThrottleVector` | vector | `throttle` | `{inner, every}` |
| `ThrottleDiscrete` | discrete | `throttle` | `{inner, every}` |
| `LagSeries` | series | `lag` | `{inner, k}` |
| `DiffSeries` | series | `diff` | `{inner, k?}` |
| `DerivativeSeries` | series | `derivative` | `{inner}` |
| `HysteresisSeries` | series | `hysteresis` | `{inner, low, high}` |
| `ResampleByTimeSeries` | series | `resampleByTime` | `{inner, bucketMillis, aggregator?}` |
| `BandSeries` | series | `band` | `{inner, k}` |
| `WithSelfLagSeries` | series | `selfLag` | `{inner, k}` |
| `WithFeedbackSeries` | series | `feedback` | `{inner, primary, project}` |
| `StandardScalerVector` | vector | `standardScaler` | `{inner, dimensions}` |
| `MinMaxScalerVector` | vector | `minMaxScaler` | `{inner, dimensions, targetLow?, targetHigh?}` |
| `StandardScalerRegression` | regression | `standardScaler` | `{inner}` |
| `MinMaxScalerRegression` | regression | `minMaxScaler` | `{inner, targetLow?, targetHigh?}` |
| `StandardScalerPaired` | paired | `standardScaler` | `{inner}` |
| `MinMaxScalerPaired` | paired | `minMaxScaler` | `{inner, targetLow?, targetHigh?}` |
| `StandardScalerSeries` | series | `standardScaler` | `{inner}` |
| `MinMaxScalerSeries` | series | `minMaxScaler` | `{inner, targetLow?, targetHigh?}` |
| `SampleSeries` | series | `sample` | `{inner, rate, seed}` |
| `SamplePaired` | paired | `sample` | `{inner, rate, seed}` |
| `SampleVector` | vector | `sample` | `{inner, rate, seed}` |
| `SampleDiscrete` | discrete | `sample` | `{inner, rate, seed}` |
| `FilterRegression` | regression | `filter` | `{inner: spec, expr: boolean}` |
| `TransformYRegression` | regression | `transformY` | `{inner, expr}` |
| `TransformXRegression` | regression | `transformX` | `{inner, expr, featureSize?}` |
| `WithWeightRegression` | regression | `withWeight` | `{inner, weight}` |
| `WeightByValueRegression` | regression | `weightBy` | `{inner, expr}` |
| `ThrottleRegression` | regression | `throttle` | `{inner, every}` |
| `SampleRegression` | regression | `sample` | `{inner, rate, seed}` |
| `FoldRegression` | regression | `foldRegression` | `{inner, featureSize, project}` |
| `GroupStatSpec` | series | `group` | `{stats}` |
| `DecayingSum` | series | `decayingSum` | `{halfLifeMillis: number}` |
| `DecayingMean` | series | `decayingMean` | `{halfLifeMillis: number}` |
| `DecayingVariance` | series | `decayingVariance` | `{halfLifeMillis: number}` |
| `EwmaMean` | series | `ewma` | `{alpha: number}` |
| `EwmaVariance` | series | `ewmaVariance` | `{alpha: number}` |
| `Holt` | series | `holt` | `{alpha, beta?, phi?}` |
| `SeasonalSmoothing` | series | `seasonalSmoothing` | `{alpha, beta, gamma, period, mode?, phi?}` |
| `RecursiveVariance` | series | `recursiveVariance` | `{omega, alpha, beta}` |
| `DecayingRate` | series | `decayingRate` | `{halfLifeMillis}` |

Decay fields are intentionally flat: EWMA uses `alpha`, wall-clock decay uses
`halfLifeMillis`, and smoothing uses numeric `alpha`, `beta`, and `gamma`.
Only these parameter records are flattened; decorators always retain `inner`.
For Holt, omitted `beta` defaults to `alpha`. A group's `stats` is a name-to-spec
object. Tree `splitCandidates` contain the split nodes in the next table.
Hasher references are strings, for example `"SplitMix64"`; enum fields retain
their declared spelling, including seasonal modes `"Additive"` and
`"Multiplicative"`.

## Configuration and bandits

These roots use the same one-key rule. Their ordinary fields retain their
Kotlin constructor names. Stateless scorers have empty object payloads. Arm,
posterior, and policy expressions use the expression grammar above.

| Root | Kotlin node | Operator | Payload |
| --- | --- | --- | --- |
| `DecayWeightingSpec` | `HalfLife` | `halfLife` | `{durationMillis}` |
| `DecayWeightingSpec` | `Alpha` | `alpha` | `number` |
| `OptimizerSpec` | `Sgd` | `sgd` | `{learningRate?}` |
| `OptimizerSpec` | `Adagrad` | `adagrad` | `{learningRate?, epsilon?}` |
| `OptimizerSpec` | `Rmsprop` | `rmsprop` | `{learningRate?, rho?, epsilon?}` |
| `OptimizerSpec` | `Adam` | `adam` | `{learningRate?, beta1?, beta2?, epsilon?}` |
| `UnivariateBanditSpec` | `MultiArmedSpec` | `multiArmed` | `{nbrArms, policy}` |
| `UnivariateBanditSpec` | `RouletteWheelSpec` | `rouletteWheel` | `{nbrArms, reactionFactor?, segmentLength?, initialWeight?, minWeight?}` |
| `UnivariateBanditSpec` | `BoltzmannSpec` | `boltzmann` | `{nbrArms, priorMean?, priorWeight?, initialTau?, minTau?, decay?}` |
| `UnivariateBanditSpec` | `Exp3Spec` | `exp3` | `{nbrArms, eta?, gamma?}` |
| `UnivariateBanditSpec` | `TopTwoThompsonSpec` | `topTwoThompson` | `{nbrArms, policy, beta?, maxResamples?}` |
| `ContextualBanditSpec` | `RegressionContextualSpec` | `regressionContextual` | `{nbrArms, regression, posterior, exploration?, globalRegression?}` |
| `SerializableSplit` | `ThresholdSplit` | `thresholdSplit` | `{featureIndex, threshold}` |
| `SerializableSplit` | `ExprSplit` | `exprSplit` | `{expr}` |
| `BanditPolicySpec` | `ThompsonSamplingSpec` | `thompsonSampling` | `{arm, posterior}` |
| `BanditPolicySpec` | `Ucb1Spec` | `ucb1` | `{alpha?, priorAlpha?, priorBeta?}` |
| `BanditPolicySpec` | `Ucb1NormalSpec` | `ucb1Normal` | `{alpha?, priorMean?, priorWeight?}` |
| `BanditPolicySpec` | `Ucb1TunedSpec` | `ucb1Tuned` | `{alpha?, priorMean?, priorWeight?}` |
| `BanditPolicySpec` | `GreedySpec` | `greedy` | `{priorMean?, priorWeight?, priorSquaredDeviations?}` |
| `BanditPolicySpec` | `EpsilonGreedySpec` | `epsilonGreedy` | `{epsilon?, priorMean?, priorWeight?, priorSquaredDeviations?}` |
| `BanditPolicySpec` | `EpsilonDecreasingSpec` | `epsilonDecreasing` | `{epsilon?, decay?, priorMean?, priorWeight?, priorSquaredDeviations?}` |
| `BanditPolicySpec` | `UniformSelectionSpec` | `uniformSelection` | `{priorMean?, priorWeight?, priorSquaredDeviations?}` |
| `BanditPolicySpec` | `KlUcbSpec` | `klUcb` | `{c?, tolerance?, priorAlpha?, priorBeta?}` |
| `BanditPolicySpec` | `MossSpec` | `moss` | `{nbrArms, priorMean?, priorWeight?}` |
| `BanditPolicySpec` | `UcbVSpec` | `ucbV` | `{zeta?, c?, priorMean?, priorWeight?}` |
| `Arm` | `BernoulliArm` | `bernoulliArm` | `{priorAlpha?, priorBeta?}` |
| `Arm` | `MeanArm` | `meanArm` | `{priorMean?, priorWeight?}` |
| `Arm` | `NormalArm` | `normalArm` | `{priorMean?, priorWeight?, priorSquaredDeviations?}` |
| `Arm` | `LogNormalArm` | `logNormalArm` | `{priorMean?, priorWeight?, priorSquaredDeviations?}` |
| `Arm` | `MomentsArm` | `momentsArm` | `{priorMean?, priorWeight?}` |
| `Arm` | `CompositeArm` | `compositeArm` | `{subArms}` |
| `Posterior` | `BetaPosterior` | `betaPosterior` | `{}` |
| `Posterior` | `PoissonGammaPosterior` | `poissonGammaPosterior` | `{}` |
| `Posterior` | `GeometricBetaPosterior` | `geometricBetaPosterior` | `{}` |
| `Posterior` | `ExponentialGammaPosterior` | `exponentialGammaPosterior` | `{}` |
| `Posterior` | `NormalGammaPosterior` | `normalGammaPosterior` | `{}` |
| `Posterior` | `LogNormalGammaPosterior` | `logNormalGammaPosterior` | `{}` |
| `Posterior` | `GammaScalePosterior` | `gammaScalePosterior` | `{fixedShape}` |
| `Posterior` | `CompositePosterior` | `compositePosterior` | `{subPosteriors, combine}` |
| `LinearPosterior` | `PointPosterior` | `pointPosterior` | `{}` |
| `LinearPosterior` | `FactorisedGaussian` | `factorisedGaussian` | `{}` |
| `LinearPosterior` | `MultivariateGaussian` | `multivariateGaussian` | `{}` |
| `LinearPosterior` | `LinUcb` | `linUcb` | `{}` |
| `Link` | `Identity` | `identity` | `{}` |
| `Link` | `Logit` | `logit` | `{}` |
| `Link` | `Log` | `log` | `{}` |
| `Penalty` | `None` | `none` | `{}` |
| `Penalty` | `L1` | `l1` | `{lambda}` |
| `Penalty` | `L2` | `l2` | `{lambda}` |
| `SplitMetric` | `VarianceReduction` | `varianceReduction` | `{}` |
| `ClassificationSplitMetric` | `GiniReduction` | `giniReduction` | `{}` |
| `ClassificationSplitMetric` | `InformationGain` | `informationGain` | `{}` |

`RegressionPosterior` includes all four `LinearPosterior` shapes above and the
following tree and forest shapes. `TreePosterior` and `ForestPosterior` use the
same respective shapes through their own roots.

| Kotlin node | Operator | Payload fields |
| --- | --- | --- |
| `MeanTreePosterior` | `meanTreePosterior` | `{}` |
| `ThompsonTreePosterior` | `thompsonTreePosterior` | `{priorWeight?, priorVariance?}` |
| `UcbTreePosterior` | `ucbTreePosterior` | `{priorWeight?, priorVariance?}` |
| `MeanForestPosterior` | `meanForestPosterior` | `{}` |
| `ThompsonForestPosterior` | `thompsonForestPosterior` | `{priorWeight?, priorVariance?}` |
| `UcbForestPosterior` | `ucbForestPosterior` | `{priorWeight?, priorVariance?}` |

Known concrete configuration records are ordinary objects, with no extra
operator wrapper:

- `CompositeSubArm`: `{arm, valueExpr?, weightExpr?, filter?}`. `arm` is a canonical
  arm node; projections are scalar expressions and `filter` is a boolean expression.
- `TopTwoThompson.policy`: `{arm, posterior}`, the statically fixed Thompson policy
  record. `MultiArmed.policy` accepts any policy and carries its operator key.
- Tree growth `config`: the declared `RegressionTreeConfig` or
  `ClassificationTreeConfig` fields (`delta?`, `deltaDecay?`, `tau?`,
  `minSamplesSplit?`, `minSamplesLeaf?`, `splitPeriod?`, `maxDepth?`, `maxNodes?`,
  `metric?`, `mtry?`). An explicit `metric` uses the corresponding operator node.
- `StatSchemaDef`: `{stats: {name: spec, ...}}`.

For example, a contextual bandit can embed a composed regression directly:

```json
{"regressionContextual": {
  "nbrArms": 2,
  "regression": {"transformY": {
    "inner": {"bayesianRegression": {"featureSize": 2}},
    "expr": {"add": [{"input": "value"}, 1]}
  }},
  "posterior": {"linUcb": {}}
}}
```

## Migration and non-JSON formats

This is a deliberate breaking JSON wire migration. Class-discriminator JSON
(`type`, `$type`, or a caller-selected discriminator), class-name operators,
`Const` wrappers, arithmetic `l`/`r` records, and nested decay `weighting`
records are not accepted alternatives. Regenerate saved JSON by reading it
with the preceding Kumulant version and writing the resulting typed objects
with this version's root serializers. Deploy producers and consumers together.
There is no separate server schema or runtime legacy-expression parser.

Formats whose encoder/decoder is not a `JsonEncoder`/`JsonDecoder`, including
Protobuf and CBOR, retain the generated sealed-class representation: original
serial names, constructor field names, operand records, and polymorphic tags.
The custom serializers delegate to that representation for those formats;
the JSON migration does not require a binary migration. Generated concrete
serializers and result/snapshot serialization retain their own record layouts.
Any nested spec, expression, or configuration root inside a JSON snapshot uses
the canonical JSON grammar, so snapshots embedding these roots participate in
the JSON migration too.
