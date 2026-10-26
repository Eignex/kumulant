# Backend-identified Native comparison

Each process repetition retains five measurement samples after three warmups.
Errors are the existing Native engine errors; process-to-process and host drift
can exceed them. Ratios below one favor views. The order-32 control is unchanged
numerical code. Inspect both repetitions and the raw samples before drawing a conclusion.

## Process repetition 0

| Method and fixture | Copy us/op | View us/op | View/copy |
| --- | ---: | ---: | ---: |
| factor precision:128 | 254.223 ± 15.867 | 375.021 ± 5.531 | 1.475 |
| factor precision:32 | 14.662 ± 1.330 | 14.362 ± 1.143 | 0.980 |
| factor rbf:1024 | 44678.309 ± 1634.100 | 89120.945 ± 3352.513 | 1.995 |
| factor rbf:512 | 6338.452 ± 401.993 | 12560.081 ± 500.273 | 1.982 |
| factorWithWorkspace precision:128 | 226.984 ± 13.108 | 382.441 ± 14.671 | 1.685 |
| factorWithWorkspace precision:32 | 15.068 ± 1.565 | 13.076 ± 0.664 | 0.868 |
| factorWithWorkspace rbf:1024 | 34809.062 ± 1414.766 | 84178.476 ± 3329.298 | 2.418 |
| factorWithWorkspace rbf:512 | 5662.704 ± 299.931 | 12715.650 ± 296.117 | 2.246 |
| factorSolveLogDeterminant 512 | 6496.429 ± 254.151 | 13355.359 ± 862.883 | 2.056 |
| factorSolveLogDeterminantWithWorkspace 512 | 6458.111 ± 204.339 | 12402.014 ± 960.954 | 1.920 |
| fittingRound 512 | 162735.144 ± 6925.337 | 313841.782 ± 16754.260 | 1.929 |
| fittingRoundWithWorkspace 512 | 162968.059 ± 5311.459 | 307374.815 ± 15886.444 | 1.886 |

## Process repetition 1

| Method and fixture | Copy us/op | View us/op | View/copy |
| --- | ---: | ---: | ---: |
| factor precision:128 | 246.399 ± 5.476 | 431.980 ± 15.056 | 1.753 |
| factor precision:32 | 14.929 ± 0.760 | 23.278 ± 1.048 | 1.559 |
| factor rbf:1024 | 37281.431 ± 1065.419 | 92264.503 ± 4136.508 | 2.475 |
| factor rbf:512 | 6864.266 ± 235.135 | 16081.153 ± 573.143 | 2.343 |
| factorWithWorkspace precision:128 | 215.084 ± 10.124 | 417.562 ± 24.988 | 1.941 |
| factorWithWorkspace precision:32 | 14.279 ± 0.793 | 15.943 ± 1.721 | 1.117 |
| factorWithWorkspace rbf:1024 | 38279.832 ± 948.155 | 88701.763 ± 3577.235 | 2.317 |
| factorWithWorkspace rbf:512 | 6364.181 ± 398.203 | 13210.024 ± 862.706 | 2.076 |
| factorSolveLogDeterminant 512 | 6287.931 ± 315.287 | 13121.488 ± 744.328 | 2.087 |
| factorSolveLogDeterminantWithWorkspace 512 | 6797.547 ± 270.561 | 15581.148 ± 555.814 | 2.292 |
| fittingRound 512 | 161122.516 ± 6323.389 | 321400.356 ± 19836.212 | 1.995 |
| fittingRoundWithWorkspace 512 | 153259.281 ± 5950.505 | 314697.387 ± 16715.859 | 2.053 |
