# Backend-identified Native comparison

Each process repetition retains five measurement samples after three warmups.
Errors are the existing Native engine errors; process-to-process and host drift
can exceed them. Ratios below one favor views. The order-32 control is unchanged
numerical code. Inspect both repetitions and the raw samples before drawing a conclusion.

## Process repetition 0

| Method and fixture | Copy us/op | View us/op | View/copy |
| --- | ---: | ---: | ---: |
| factor precision:128 | 330.896 ± 2.513 | 290.893 ± 1.115 | 0.879 |
| factor precision:32 | 16.878 ± 0.078 | 17.622 ± 0.138 | 1.044 |
| factor rbf:1024 | 55140.039 ± 390.486 | 24499.814 ± 133.741 | 0.444 |
| factor rbf:512 | 9298.375 ± 88.428 | 5818.360 ± 11.134 | 0.626 |
| factorWithWorkspace precision:128 | 297.382 ± 0.590 | 287.596 ± 1.270 | 0.967 |
| factorWithWorkspace precision:32 | 17.631 ± 0.150 | 16.956 ± 0.075 | 0.962 |
| factorWithWorkspace rbf:1024 | 55182.689 ± 472.128 | 24858.868 ± 97.808 | 0.450 |
| factorWithWorkspace rbf:512 | 9322.022 ± 100.611 | 5780.529 ± 15.741 | 0.620 |
| factorSolveLogDeterminant 512 | 9332.496 ± 75.880 | 5851.708 ± 23.175 | 0.627 |
| factorSolveLogDeterminantWithWorkspace 512 | 9290.992 ± 56.650 | 5817.627 ± 24.317 | 0.626 |
| fittingRound 512 | 230033.148 ± 2624.738 | 147173.373 ± 543.795 | 0.640 |
| fittingRoundWithWorkspace 512 | 230014.396 ± 1231.032 | 149835.788 ± 708.896 | 0.651 |

## Process repetition 1

| Method and fixture | Copy us/op | View us/op | View/copy |
| --- | ---: | ---: | ---: |
| factor precision:128 | 333.986 ± 1.699 | 290.150 ± 0.997 | 0.869 |
| factor precision:32 | 16.399 ± 0.180 | 16.992 ± 0.074 | 1.036 |
| factor rbf:1024 | 54817.388 ± 463.882 | 24566.827 ± 34.576 | 0.448 |
| factor rbf:512 | 9332.691 ± 53.499 | 5746.051 ± 32.308 | 0.616 |
| factorWithWorkspace precision:128 | 295.862 ± 1.475 | 286.410 ± 0.929 | 0.968 |
| factorWithWorkspace precision:32 | 16.668 ± 0.154 | 16.960 ± 0.113 | 1.018 |
| factorWithWorkspace rbf:1024 | 54236.041 ± 606.249 | 24590.205 ± 47.480 | 0.453 |
| factorWithWorkspace rbf:512 | 9048.715 ± 98.211 | 5756.657 ± 37.751 | 0.636 |
| factorSolveLogDeterminant 512 | 9214.285 ± 68.533 | 7474.359 ± 33.511 | 0.811 |
| factorSolveLogDeterminantWithWorkspace 512 | 9146.954 ± 56.500 | 5821.215 ± 29.597 | 0.636 |
| fittingRound 512 | 231907.112 ± 867.333 | 152212.563 ± 534.014 | 0.656 |
| fittingRoundWithWorkspace 512 | 229406.183 ± 1528.928 | 147194.521 ± 610.826 | 0.642 |
