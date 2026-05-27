# Package com.eignex.kumulant.stat.regression.tree

Online VFDT decision trees and random forests, plus the shared
machinery they're built on. The package covers both regression
(continuous `y`) and classification (`y` in `[0, numClasses)`) under
one consistent shape.

## The four user-facing stats

| Stat | Output | Leaf state |
|------|--------|-----------|
| [DecisionTreeRegressionStat] | Continuous `y` prediction | [com.eignex.kumulant.stat.summary.WeightedVarianceResult] |
| [RandomForestRegressionStat] | Bagged ensemble of regression trees | One [DecisionTreeRegressionStat] per tree |
| [DecisionTreeClassifierStat] | K-way class probabilities | [ClassCountsResult] |
| [RandomForestClassifierStat] | Bagged ensemble of classifier trees; predictions average per-class probabilities | One [DecisionTreeClassifierStat] per tree |

Each takes a list of [Split] candidates (axis-aligned thresholds or
arbitrary [ExprSplit] expressions) and a config —
[RegressionTreeConfig] for the regression side,
[ClassificationTreeConfig] for the classification side. Splits fire
when a candidate clears the Hoeffding bound on the configured metric:
[VarianceReduction] for regression, [GiniReduction] / [InformationGain]
for classification.

## Split candidates

[ThresholdSplit] is the standard axis-aligned predicate `x[i] <= t`.
[ExprSplit] takes any [com.eignex.kumulant.schema.BoolExpr] for
non-axis-aligned splits — useful when you want a tree to split on a
derived feature (`x[0] + x[1] > 0`, `(x[0] - x[1]).abs() > 1`)
without materialising the feature in the input vector.

Pass an empty `splitCandidates` list to disable growth and degenerate
the stat into a single global accumulator. The Random forest's
`config.mtry` defaults to `ceil(sqrt(p))` Breiman-style.

## Posteriors

[TreePosterior] (and its forest counterpart [ForestPosterior]) turn a
[TreeRegressionResult] / [ForestRegressionResult] into a scalar score:

- [MeanTreePosterior] / [MeanForestPosterior] — deterministic leaf
  mean.
- [ThompsonTreePosterior] / [ThompsonForestPosterior] — Normal-Gamma
  draw from the leaf's `(mean, variance, totalWeights)` triplet.
- [UcbTreePosterior] / [UcbForestPosterior] — UCB-style
  `mean + alpha * sqrt(variance / n)`.

These plug into
[com.eignex.kumulant.bandit.contextual.RegressionContextualBandit] for
non-linear contextual bandits with the same Thompson / UCB / mean
choice that the GLM side has via the
[com.eignex.kumulant.stat.regression.glm.LinearPosterior] family.

## Concurrency

The hot update path touches exactly one accumulator — the leaf the
observation routes to. Internal split nodes carry no live arm; subtree
aggregates are derived by combining descendants at snapshot time.
Each leaf arm honours the [com.eignex.kumulant.core.Concurrency] level
passed in, so multiple threads landing in different leaves never
contend. Split conversion takes a per-tree lock fired only at split
decisions. See [RegressionTree] / [ClassificationTree] for the full
concurrency design.
