# Package com.eignex.kumulant.stat.score

Online evaluation metrics. Inputs are paired `(prediction, truth)`
observations (or richer shapes for distributional metrics) and outputs
are accuracy / discrimination / calibration / distributional summaries.

The *calibration* diagnostic ([Reliability][com.eignex.kumulant.stat.calibration.ReliabilityStat])
and the calibration *fixes* live in [com.eignex.kumulant.stat.calibration].
This package is for the rest: regression errors, proper scoring rules,
discrimination metrics, classification metrics, and distributional
forecast diagnostics.

## Regression errors

| Stat | Result | Use |
|------|--------|-----|
| [mseLoss][com.eignex.kumulant.schema.ops.mseLoss] | [com.eignex.kumulant.stat.summary.WeightedMeanResult] | Mean squared error; weights large errors quadratically. |
| [maeLoss][com.eignex.kumulant.schema.ops.maeLoss] | [com.eignex.kumulant.stat.summary.WeightedMeanResult] | Mean absolute error; robust median-error-style alternative. |
| [pinballLoss][com.eignex.kumulant.schema.ops.pinballLoss] | [com.eignex.kumulant.stat.summary.WeightedMeanResult] | Quantile (pinball) loss at quantile `tau`. The right pick when the model emits a quantile rather than a mean. |

The losses are spec builders: each is `Mean.foldPaired(score)` for its per-pair score, so they
compose, window and serialize like any other paired spec.

## Binary proper scoring rules

| Stat | Result | Use |
|------|--------|-----|
| [logLoss][com.eignex.kumulant.schema.ops.logLoss] | [com.eignex.kumulant.stat.summary.WeightedMeanResult] | Cross-entropy / log-likelihood. Log-likelihood-shaped objectives. |
| [mseLoss][com.eignex.kumulant.schema.ops.mseLoss] | [com.eignex.kumulant.stat.summary.WeightedMeanResult] | Binary Brier score when inputs are probabilities and outcomes are in `{0, 1}`. |

LogLoss penalises confident-wrong predictions much more harshly than
Brier. Pick LogLoss when you want a likelihood-shaped objective; pick
MSE on binary probability forecasts when you want the Brier score. The `[0, 1]`
bound assumes probabilities and outcomes stay in that domain; MSE does not clamp inputs.

## Discrimination

[AucStat] reports streaming ROC AUC over a fixed-resolution score
histogram. AUC measures whether positives score higher than negatives
on average and is calibration-agnostic; a perfectly-discriminative
model can still be miscalibrated, and a perfectly-calibrated model can
have mediocre AUC.

## Classification

| Stat | Result | Use |
|------|--------|-----|
| [AccuracyStat] | [com.eignex.kumulant.stat.summary.WeightedMeanResult] | Weighted classification accuracy: fraction of `predicted == truth`. |
| [ConfusionMatrixStat] | [ConfusionMatrixResult] | K-by-K confusion matrix with per-class precision / recall / F1, macro / micro averages, multiclass MCC. |

[AccuracyStat] is the O(1) shortcut when only the scalar accuracy
matters. [ConfusionMatrixStat] is the full P/R/F1 surface with a
per-class breakdown; reach for it when accuracy alone hides
class-imbalance effects.

Both take a `numClasses` and resolve labels the same way: a pair is
scored only if both sides are exactly an integer in `[0, numClasses)`,
and is ignored otherwise. That is what makes
[ConfusionMatrixResult.accuracy] and [AccuracyStat]'s mean comparable on
the same stream. A fractional label such as `1.5`, an out-of-range one,
and `NaN` are all ignored by both rather than truncated into a class by
one and refused by the other.

## Distributional forecast diagnostics

The PIT (probability integral transform) family covers calibration of
distributional forecasts:

- `pitHistogram(numBins)` (factory in `PitHistogram.kt`): feeds PIT
  values into an equiprobable [BucketHistogramStat][com.eignex.kumulant.stat.quantile.BucketHistogramStat]
  over `[0, 1]`. Under correct distributional forecasts the histogram
  should be uniform; deviations diagnose under- or over-coverage and
  tail mis-specifications.
- The functions in `PitTests.kt` run the standard PIT uniformity tests
  on the histogram (Kolmogorov-Smirnov-style summary statistics).

Use these when the model emits a CDF (not just a point estimate) and
you want to check whether the predicted distribution matches the
empirical one.

## Compose patterns

- `mseLoss().windowed(window)` for windowed regression error.
- `mseLoss().transformX(...)` after a [Platt][com.eignex.kumulant.stat.calibration.PlattCalibratorStat]
  or [isotonic calibration][com.eignex.kumulant.stat.calibration.isotonicCalibration]
  step to score calibrated probabilities.
- `Auc` + [Reliability][com.eignex.kumulant.stat.calibration.ReliabilityStat]
  in parallel: AUC tells you discrimination, reliability tells you
  calibration. A pipeline that monitors both catches different failure
  modes.

## Merge

All paired-mean-shaped metrics (the losses and `Accuracy`) merge via the underlying
[MeanStat][com.eignex.kumulant.stat.summary.MeanStat]'s Chan-style
parallel formula; exact across replicas. [AucStat] and
[ConfusionMatrixStat] merge via cell-wise bin / matrix addition.

## Concurrency

The mean-shaped metrics ([AccuracyStat] and the losses) inherit
[MeanStat][com.eignex.kumulant.stat.summary.MeanStat]'s Welford-coupled model:
locked under [com.eignex.kumulant.core.Concurrency.Strict] /
[com.eignex.kumulant.core.Concurrency.HighWrite]. Under
[com.eignex.kumulant.core.Concurrency.Relaxed], coupled state can drift without
a numerical error bound. [AucStat] and
[ConfusionMatrixStat] use independent additive histogram / matrix cells;
concurrent modes preserve per-cell updates, with JVM striped adders under
HighWrite. Multi-cell reads are not atomic. Floating-point results depend on
update order. [com.eignex.kumulant.core.Concurrency.None] uses plain cells.
