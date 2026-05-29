# Package com.eignex.kumulant.stat.calibration

Probability calibration: diagnosing when a classifier's predicted
probabilities are out of step with the empirical positive rate, and
two complementary mappings that fix it.

## Picking a calibration stat

| Stat | Role | Mapping |
|------|------|---------|
| [ReliabilityStat] | **Diagnostic.** Bins `(predicted, observed)` pairs into per-bin reliability tiers; result exposes expected calibration error and the per-bin gap. | No mapping; read the result, plot, or pipe into an alarm. |
| [PlattCalibratorStat] | **Parametric fix.** Fits `sigmoid(slope * x + intercept)` over `(rawScore, label)`. Two parameters. | Smooth global sigmoid. Right when miscalibration is roughly sigmoidal; the classic SVM / boosted-tree pattern. |
| [IsotonicCalibratorStat] | **Non-parametric fix.** Bins raw scores; runs Pool Adjacent Violators at read time to produce a monotone step function. | Arbitrary monotone shape with linear interpolation between bin midpoints. Right when miscalibration has a non-sigmoidal pattern (kinks, plateaus, asymmetric tails). |

## The diagnostic → fix workflow

Standard pipeline:

1. Train a classifier (anything producing a probability or score in
   `[0, 1]`).
2. Feed `(prediction, label)` to a [ReliabilityStat] alongside training
   (or on held-out evaluation data) and read the per-bin gap. If
   `expectedCalibrationError()` is acceptable, stop.
3. Otherwise plumb the same `(prediction, label)` stream into a
   [PlattCalibratorStat] or [IsotonicCalibratorStat]. At inference,
   replace `rawProb` with `calibrator.calibrate(rawProb)`.
4. Continue feeding [ReliabilityStat] from the calibrated outputs to
   verify the fix held.

## Reuse with ReliabilityStat

[IsotonicCalibratorStat] is built on top of [ReliabilityStat]; the
binned `(positives, total)` book-keeping is exactly what
[ReliabilityResult] already exposes. The PAV pass is purely a `read`
post-process; updates remain `O(1)`. If you already have a
[ReliabilityStat] in the pipeline for diagnostics, attaching an
[IsotonicCalibratorStat] costs only the second instance's per-bin cells.

## Merge

[ReliabilityStat] merges per-bin sums element-wise, safe across
parallel workers. [PlattCalibratorStat] merges sample-weighted weight
vectors via the inner
[com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat]:
same approximation as any SGD merge. [IsotonicCalibratorStat] does
not support merge directly: the result only carries the threshold step
function, not the bin layout. To pool isotonic calibration across
workers, merge the underlying [ReliabilityStat] and re-derive.

## Concurrency

[ReliabilityStat] applies independent striped atomic adds to its per-bin
counters; lock-free and exact under every [com.eignex.kumulant.core.Concurrency]
level. [IsotonicCalibratorStat] inherits that model, with the read-time Pool
Adjacent Violators pass running single-threaded. [PlattCalibratorStat] inherits
the [com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat] model:
the update body is locked under [com.eignex.kumulant.core.Concurrency.Strict] /
[com.eignex.kumulant.core.Concurrency.HighWrite] and runs lock-free (Hogwild)
under [com.eignex.kumulant.core.Concurrency.Relaxed].
