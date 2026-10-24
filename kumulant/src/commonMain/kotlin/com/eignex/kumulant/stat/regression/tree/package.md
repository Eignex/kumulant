# Package com.eignex.kumulant.stat.regression.tree

Online VFDT decision trees and random forests, plus the shared
machinery they're built on. The package covers both regression
(continuous `y`) and classification (`y` in `[0, numClasses)`) under
one consistent shape.

## Picking a tree stat

| Stat | Output | Leaf state |
|------|--------|-----------|
| [DecisionTreeRegressionStat] | Continuous `y` prediction | [com.eignex.kumulant.stat.summary.WeightedVarianceResult] |
| [RandomForestRegressionStat] | Bagged ensemble of regression trees | One [DecisionTreeRegressionStat] per tree |
| [DecisionTreeClassifierStat] | K-way class probabilities | [ClassCountsResult] |
| [RandomForestClassifierStat] | Bagged ensemble of classifier trees; predictions average per-class probabilities | One [DecisionTreeClassifierStat] per tree |

Each takes a list of [Split] candidates (axis-aligned thresholds or
arbitrary [ExprSplit] expressions) and a config;
[RegressionTreeConfig] for the regression side,
[ClassificationTreeConfig] for the classification side. Splits fire
when a candidate clears the Hoeffding bound on the configured metric:
[VarianceReduction] for regression, [GiniReduction] / [InformationGain]
for classification.

## Split candidates

[ThresholdSplit] is the standard axis-aligned predicate `x[i] <= t`.
[ExprSplit] takes any [com.eignex.kumulant.schema.expr.BoolExpr] for
non-axis-aligned splits; useful when you want a tree to split on a
derived feature (`x[0] + x[1] > 0`, `(x[0] - x[1]).abs() > 1`)
without materialising the feature in the input vector.

Pass an empty `splitCandidates` list to disable growth and degenerate
the stat into a single global accumulator. The Random forest's
`config.mtry` defaults to `ceil(sqrt(p))` Breiman-style.

## Batch fitting

[BatchRandomForest] fits independent CART forests from complete training datasets.
[RegressionObservation] supplies numeric vectors, scalar responses, and nonnegative
weights; [BatchForestConfig] controls bootstrap sampling, feature subsampling, and
leaf/size limits. [BatchSplitStrategy] selects greedy boundaries or randomized continuous
thresholds within the observed feature range. The result
is the same [ForestRegressionResult] snapshot shape used by streaming forests.

[predictDistribution] summarizes the equal-weight distribution of member predictions
for model uncertainty. [ForestPrediction.expectedImprovement] computes Gaussian
expected improvement for either response maximization or cost minimization.
For log-response models, [ForestPrediction.expectedImprovementLogNormal] integrates
improvement in original positive response units. This ensemble distribution differs from [ForestRegressionResult.findLeafMerged], which
pools leaf evidence with statistical weights for posterior-based online scoring.

[BatchRandomForest.fitModel] also estimates leaf-mean uncertainty from original observation
units, independently of internal bootstrap multiplicities. [ForestMoments] separates tree
disagreement, uncertainty in leaf means, and response dispersion. Known observation variances
propagate through normalized squared weights. [BatchForestModel.drawFunction] fixes one tree
and its Gaussian leaf offsets for coherent repeated queries. These are approximate sampling
moments: adaptive partition selection, unresolved within-leaf function variation, and correlated
trees prevent interpreting them as a calibrated posterior. Response dispersion is not latent
function uncertainty, and additional trees do not multiply independent evidence.

## Posteriors

[TreePosterior] (and its forest counterpart [ForestPosterior]) turn a
[TreeRegressionResult] / [ForestRegressionResult] into a scalar score:

- [MeanTreePosterior] / [MeanForestPosterior]: deterministic leaf
  mean.
- [ThompsonTreePosterior] / [ThompsonForestPosterior]: Normal-Gamma
  draw from the leaf's `(mean, variance, totalWeights)` triplet.
- [UcbTreePosterior] / [UcbForestPosterior]: UCB-style
  `mean + alpha * sqrt(variance / n)`.

These plug into
[com.eignex.kumulant.bandit.contextual.RegressionContextualBandit] for
non-linear contextual bandits with the same Thompson / UCB / mean
choice that the GLM side has via the
[com.eignex.kumulant.stat.regression.glm.LinearPosterior] family.

For constrained search or custom acquisition functions, [mergeForestEvidence]
pools arbitrary leaf or subtree summaries with the same correlation adjustment.
Its default averages the effective weight across members; `independent = true`
adds weights for disjoint observations. [RandomForestRegressionStat.findLeaves]
and [RandomForestRegressionStat.predict] read live leaf statistics without
freezing the full tree structure. Vector inputs can be ordinary dense or sparse
vectors, or read-only views supplied by a downstream feature adapter.

## Merge

All four merge approximately through `tree.mergeSnapshot()`, which combines
matching leaf accumulators: weighted Welford per leaf for the regression trees,
cell-wise class-count sums for the classifiers, per-tree for the forests. The
combine assumes the tree structures line up (same split history); divergent
trees merge only their shared leaves. For distributed training, prefer feeding
one stat the ordered stream and treat merge as a roll-up.

## Concurrency

The hot update path touches exactly one accumulator; the leaf the
observation routes to. Internal split nodes carry no live arm; subtree
aggregates are derived by combining descendants at snapshot time.
Each leaf arm honours the [com.eignex.kumulant.core.Concurrency] level
passed in, so multiple threads landing in different leaves never
contend. Split conversion takes a per-tree lock fired only at split
decisions. See [RegressionTree] / [ClassificationTree] for the full
concurrency design.
