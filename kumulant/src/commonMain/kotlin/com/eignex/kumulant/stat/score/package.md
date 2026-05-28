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
| `MseLoss` (in `Loss.kt`) | [com.eignex.kumulant.stat.summary.WeightedMeanResult] | Mean squared error; weights large errors quadratically. |
| `MaeLoss` (in `Loss.kt`) | [com.eignex.kumulant.stat.summary.WeightedMeanResult] | Mean absolute error; robust median-error-style alternative. |
| [PinballLossStat] | [com.eignex.kumulant.stat.summary.WeightedMeanResult] | Quantile (pinball) loss at quantile `tau`. The right pick when the model emits a quantile rather than a mean. |

## Binary proper scoring rules

| Stat | Result | Use |
|------|--------|-----|
| `LogLoss` (in `Loss.kt`) | [com.eignex.kumulant.stat.summary.WeightedMeanResult] | Cross-entropy / log-likelihood. Log-likelihood-shaped objectives. |
| [BrierScoreStat] | [com.eignex.kumulant.stat.summary.WeightedMeanResult] | Bounded squared-error counterpart of log loss. Reliability-decomposable. |

LogLoss penalises confident-wrong predictions much more harshly than
Brier. Pick LogLoss when you want a likelihood-shaped objective; pick
Brier when you want a bounded, calibration-decomposable error.

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

## Distributional forecast diagnostics

The PIT (probability integral transform) family covers calibration of
distributional forecasts:

- `pitHistogram(numBins)` (factory in `PitHistogram.kt`); feeds PIT
  values into an equiprobable [LinearHistogramStat][com.eignex.kumulant.stat.quantile.LinearHistogramStat]
  over `[0, 1]`. Under correct distributional forecasts the histogram
  should be uniform; deviations diagnose under- or over-coverage and
  tail mis-specifications.
- The functions in `PitTests.kt` run the standard PIT uniformity tests
  on the histogram (Kolmogorov-Smirnov-style summary statistics).

Use these when the model emits a CDF (not just a point estimate) and
you want to check whether the predicted distribution matches the
empirical one.

## Compose patterns

- `MseLoss.windowed(window)` for windowed regression error.
- `BrierScore.transform(...)` after a [Platt][com.eignex.kumulant.stat.calibration.PlattCalibratorStat]
  or [Isotonic][com.eignex.kumulant.stat.calibration.IsotonicCalibratorStat]
  step to score calibrated probabilities.
- `Auc` + [Reliability][com.eignex.kumulant.stat.calibration.ReliabilityStat]
  in parallel: AUC tells you discrimination, reliability tells you
  calibration. A pipeline that monitors both catches different failure
  modes.

## Merge

All paired-mean-shaped metrics (`MseLoss`, `MaeLoss`, `LogLoss`,
`BrierScore`, `PinballLoss`, `Accuracy`) merge via the underlying
[MeanStat][com.eignex.kumulant.stat.summary.MeanStat]'s Chan-style
parallel formula; exact across replicas. [AucStat] and
[ConfusionMatrixStat] merge via cell-wise bin / matrix addition.
