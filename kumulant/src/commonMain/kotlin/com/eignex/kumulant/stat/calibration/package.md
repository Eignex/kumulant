# Package com.eignex.kumulant.stat.calibration

Probability calibration diagnoses how predicted probabilities differ from
observed positive rates and fits a mapping to correct them. The live stats
consume [com.eignex.kumulant.core.PairedStat] observations `(prediction, label)`;
isotonic calibration derives a curve from a reliability snapshot.

## Picking a calibration method

| Stat or projection | Mapping | Reach for it when |
|--------------------|---------|-------------------|
| [ReliabilityStat] | Per-bin predicted and observed rates, plus calibration error | Diagnose calibration, plot the gaps, or retain mergeable evidence for isotonic fitting. |
| [PlattCalibratorStat] | `sigmoid(slope * x + intercept)` | Fit a smooth sigmoid with two parameters, especially when observations are scarce. |
| [isotonicCalibration] | Monotone probabilities with linear interpolation between bin midpoints | Fit an arbitrary monotone shape from a [ReliabilityResult] without a second accumulator. |

## The diagnostic and correction workflow

1. Feed held-out `(prediction, label)` pairs to [ReliabilityStat].
2. Inspect [ReliabilityResult.expectedCalibrationError] and the per-bin gap.
3. Call [ReliabilityResult.isotonicCalibration] to fit a monotone curve, or
   train [PlattCalibratorStat] on the pairs for a sigmoid mapping.
4. Apply the fitted result's `calibrate(rawProbability)` at inference and
   monitor the calibrated outputs with a reliability stat.

The isotonic projection runs weighted Pool Adjacent Violators in O(numBins)
time and memory. It reads the existing per-bin evidence and needs no update
path or separate lifecycle.

## Merge

[ReliabilityStat] adds per-bin sums, retaining the evidence across workers.
Merge those snapshots before calling [isotonicCalibration]; fitting each
shard and averaging curves loses the bin weights and pooling constraints.
[PlattCalibratorStat] merges sample-weighted parameter vectors through its
[com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat], an
approximation to training on the combined observations.

## Concurrency

[ReliabilityStat] uses independent atomic adds for its per-bin cells and
bounds each snapshot's outcome sum by its total weight. Isotonic fitting
operates on that detached snapshot. [PlattCalibratorStat] inherits the
[com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat] model:
locked updates under [com.eignex.kumulant.core.Concurrency.Strict] and
[com.eignex.kumulant.core.Concurrency.HighWrite], asynchronous SGD under
[com.eignex.kumulant.core.Concurrency.Relaxed].
