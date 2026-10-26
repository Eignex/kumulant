# Matched copying/view measurements

JVM uncertainty is the JMH score error from two forks; Native rows retain each
process separately with its engine error. Ratios below one favor views. Small
differences within uncertainty do not establish a performance change.
Native view rows describe the rejected trial; production retains the copying route.

## scalar

| Method and fixture | Copy µs/op | View µs/op | View/copy | Copy B/op | View B/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| factor precision:8 | 0.230 ± 0.042 | 0.245 ± 0.031 | 1.068 | 568 | 568 |
| factor precision:128 | 179.507 ± 8.170 | 201.901 ± 42.308 | 1.125 | 410425 | 246537 |
| factor precision:512 | 7321.859 ± 508.690 | 7404.312 ± 866.520 | 1.011 | 8783717 | 3278590 |
| factorWithWorkspace precision:8 | 0.220 ± 0.022 | 0.250 ± 0.049 | 1.137 | 568 | 568 |
| factorWithWorkspace precision:128 | 171.544 ± 23.835 | 186.045 ± 35.391 | 1.085 | 131273 | 131321 |
| factorWithWorkspace precision:512 | 8996.431 ± 1075.718 | 7601.235 ± 888.291 | 0.845 | 7537360 | 2097713 |
| factor rbf:128 | 210.965 ± 24.530 | 156.812 ± 5.732 | 0.743 | 410425 | 246537 |
| factor rbf:512 | 8768.746 ± 823.576 | 6319.195 ± 434.147 | 0.721 | 8783744 | 3278575 |
| factor rbf:1024 | 65225.564 ± 8512.977 | 44712.330 ± 3859.201 | 0.686 | 55710015 | 11145131 |
| factorWithWorkspace rbf:128 | 182.086 ± 23.962 | 146.486 ± 5.625 | 0.804 | 131273 | 131321 |
| factorWithWorkspace rbf:512 | 9065.154 ± 1658.284 | 6361.115 ± 569.089 | 0.702 | 7537360 | 2097699 |
| factorWithWorkspace rbf:1024 | 70536.210 ± 9163.080 | 44427.601 ± 3186.324 | 0.630 | 52889595 | 8389984 |
| factorSolveLogDeterminant 128 | 222.020 ± 23.160 | 213.196 ± 19.097 | 0.960 | 411449 | 247561 |
| factorSolveLogDeterminant 512 | 8855.300 ± 691.800 | 7906.908 ± 939.233 | 0.893 | 8787859 | 3282726 |
| factorSolveLogDeterminantWithWorkspace 128 | 203.727 ± 27.112 | 185.441 ± 13.326 | 0.910 | 131257 | 131305 |
| factorSolveLogDeterminantWithWorkspace 512 | 8537.332 ± 1553.727 | 8023.196 ± 1960.782 | 0.940 | 7537362 | 2097737 |
| fittingRound 128 | 5862.592 ± 507.959 | 5182.041 ± 421.220 | 0.884 | 10286250 | 6189043 |
| fittingRound 512 | 231890.521 ± 24189.242 | 201200.944 ± 20778.974 | 0.868 | 219696114 | 82067618 |
| fittingRoundWithWorkspace 128 | 4782.344 ± 493.884 | 4610.992 ± 957.471 | 0.964 | 3281595 | 3282772 |
| fittingRoundWithWorkspace 512 | 250315.344 ± 38116.190 | 212947.351 ± 36304.816 | 0.851 | 188434134 | 52443138 |
| factor precision:32 | 5.705 ± 0.701 | 5.445 ± 0.587 | 0.955 | 8248 | 8248 |
| factorWithWorkspace precision:32 | 6.610 ± 1.155 | 7.010 ± 1.112 | 1.061 | 8248 | 8248 |

## simd

| Method and fixture | Copy µs/op | View µs/op | View/copy | Copy B/op | View B/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| factor precision:8 | 0.338 ± 0.024 | 0.258 ± 0.063 | 0.764 | 568 | 568 |
| factor precision:128 | 191.433 ± 11.095 | 150.981 ± 10.513 | 0.789 | 411577 | 247689 |
| factor precision:512 | 5461.686 ± 377.893 | 4236.834 ± 708.551 | 0.776 | 8786393 | 3281250 |
| factorWithWorkspace precision:8 | 0.332 ± 0.033 | 0.302 ± 0.042 | 0.910 | 568 | 568 |
| factorWithWorkspace precision:128 | 165.208 ± 12.143 | 160.939 ± 28.159 | 0.974 | 131273 | 131321 |
| factorWithWorkspace precision:512 | 5542.741 ± 425.401 | 3973.739 ± 237.896 | 0.717 | 7537328 | 2097681 |
| factor rbf:128 | 155.329 ± 16.802 | 117.522 ± 9.656 | 0.757 | 411577 | 247689 |
| factor rbf:512 | 4838.027 ± 382.268 | 3202.722 ± 196.427 | 0.662 | 8786389 | 3281244 |
| factor rbf:1024 | 32425.708 ± 2898.609 | 16660.434 ± 1091.163 | 0.514 | 55715482 | 11150655 |
| factorWithWorkspace rbf:128 | 150.426 ± 24.432 | 103.530 ± 8.906 | 0.688 | 131273 | 131321 |
| factorWithWorkspace rbf:512 | 5295.230 ± 453.121 | 3116.629 ± 199.405 | 0.589 | 7537319 | 2097675 |
| factorWithWorkspace rbf:1024 | 36653.267 ± 7005.407 | 16678.040 ± 1345.584 | 0.455 | 52889174 | 8389742 |
| factorSolveLogDeterminant 128 | 178.898 ± 15.662 | 158.121 ± 17.251 | 0.884 | 412609 | 248713 |
| factorSolveLogDeterminant 512 | 5538.390 ± 555.110 | 4435.624 ± 729.652 | 0.801 | 8790512 | 3285367 |
| factorSolveLogDeterminantWithWorkspace 128 | 170.890 ± 24.907 | 144.707 ± 13.991 | 0.847 | 131265 | 131305 |
| factorSolveLogDeterminantWithWorkspace 512 | 5336.811 ± 469.410 | 4232.609 ± 516.053 | 0.793 | 7537333 | 2097685 |
| fittingRound 128 | 4728.472 ± 499.688 | 4231.854 ± 461.447 | 0.895 | 10315241 | 6217836 |
| fittingRound 512 | 133261.666 ± 11204.664 | 102225.337 ± 6864.972 | 0.767 | 219762623 | 82134037 |
| fittingRoundWithWorkspace 128 | 3932.956 ± 419.660 | 3489.867 ± 466.279 | 0.887 | 3281707 | 3282723 |
| fittingRoundWithWorkspace 512 | 126688.438 ± 17471.172 | 107202.637 ± 18451.496 | 0.846 | 188432969 | 52442071 |
| factor precision:32 | 7.151 ± 0.526 | 7.161 ± 0.334 | 1.001 | 8248 | 8248 |
| factorWithWorkspace precision:32 | 6.083 ± 1.633 | 5.957 ± 1.815 | 0.979 | 8248 | 8248 |

## native-0

| Method and fixture | Copy µs/op | View µs/op | View/copy | Copy B/op | View B/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| factorWithWorkspace precision:8 | 0.948 ± 0.031 | 1.008 ± 0.051 | 1.063 | — | — |
| factorWithWorkspace precision:128 | 234.154 ± 18.555 | 475.718 ± 14.602 | 2.032 | — | — |
| factorWithWorkspace precision:512 | 7041.423 ± 191.185 | 14035.153 ± 475.227 | 1.993 | — | — |
| factor precision:8 | 0.987 ± 0.069 | 0.981 ± 0.056 | 0.994 | — | — |
| factor precision:128 | 276.272 ± 44.455 | 453.817 ± 17.982 | 1.643 | — | — |
| factor precision:512 | 7522.204 ± 1485.080 | 13405.364 ± 651.188 | 1.782 | — | — |
| factorWithWorkspace rbf:128 | 212.324 ± 14.833 | 468.847 ± 20.891 | 2.208 | — | — |
| factorWithWorkspace rbf:512 | 6469.932 ± 45.456 | 14055.428 ± 748.365 | 2.172 | — | — |
| factorWithWorkspace rbf:1024 | 35958.286 ± 969.990 | 96305.232 ± 5154.597 | 2.678 | — | — |
| factor rbf:128 | 259.623 ± 6.208 | 509.288 ± 31.357 | 1.962 | — | — |
| factor rbf:512 | 6226.666 ± 351.202 | 14242.277 ± 439.190 | 2.287 | — | — |
| factor rbf:1024 | 38069.656 ± 772.121 | 96294.403 ± 3257.086 | 2.529 | — | — |
| factorSolveLogDeterminantWithWorkspace 128 | 258.372 ± 15.182 | 458.971 ± 26.744 | 1.776 | — | — |
| factorSolveLogDeterminantWithWorkspace 512 | 6660.194 ± 190.894 | 14256.250 ± 626.020 | 2.141 | — | — |
| factorSolveLogDeterminant 128 | 291.788 ± 12.032 | 531.952 ± 7.398 | 1.823 | — | — |
| factorSolveLogDeterminant 512 | 6897.083 ± 524.498 | 14257.654 ± 1182.511 | 2.067 | — | — |
| fittingRoundWithWorkspace 128 | 6622.982 ± 244.524 | 12290.306 ± 641.830 | 1.856 | — | — |
| fittingRoundWithWorkspace 512 | 174656.246 ± 8018.026 | 362217.347 ± 8978.706 | 2.074 | — | — |
| fittingRound 128 | 7206.590 ± 280.187 | 12654.120 ± 1026.496 | 1.756 | — | — |
| fittingRound 512 | 177485.720 ± 9441.342 | 425920.903 ± 11610.530 | 2.400 | — | — |
| factorWithWorkspace precision:32 | 15.176 ± 0.819 | 19.318 ± 0.332 | 1.273 | — | — |
| factor precision:32 | 15.676 ± 0.486 | 24.407 ± 1.322 | 1.557 | — | — |

## native-1

| Method and fixture | Copy µs/op | View µs/op | View/copy | Copy B/op | View B/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| factorWithWorkspace precision:8 | 0.712 ± 0.014 | 0.846 ± 0.145 | 1.189 | — | — |
| factorWithWorkspace precision:128 | 207.022 ± 6.183 | 387.156 ± 14.566 | 1.870 | — | — |
| factorWithWorkspace precision:512 | 5871.476 ± 214.406 | 12164.850 ± 602.046 | 2.072 | — | — |
| factor precision:8 | 0.743 ± 0.021 | 0.776 ± 0.058 | 1.045 | — | — |
| factor precision:128 | 218.262 ± 6.978 | 387.600 ± 14.139 | 1.776 | — | — |
| factor precision:512 | 6242.512 ± 340.666 | 11455.156 ± 155.922 | 1.835 | — | — |
| factorWithWorkspace rbf:128 | 203.828 ± 9.157 | 385.606 ± 13.116 | 1.892 | — | — |
| factorWithWorkspace rbf:512 | 5581.476 ± 75.653 | 12349.157 ± 367.729 | 2.213 | — | — |
| factorWithWorkspace rbf:1024 | 34676.953 ± 565.427 | 84247.014 ± 2097.605 | 2.429 | — | — |
| factor rbf:128 | 216.241 ± 3.582 | 434.159 ± 4.933 | 2.008 | — | — |
| factor rbf:512 | 5714.843 ± 171.241 | 13581.222 ± 487.474 | 2.376 | — | — |
| factor rbf:1024 | 33704.784 ± 478.714 | 96543.339 ± 2917.960 | 2.864 | — | — |
| factorSolveLogDeterminantWithWorkspace 128 | 202.804 ± 5.557 | 418.671 ± 46.523 | 2.064 | — | — |
| factorSolveLogDeterminantWithWorkspace 512 | 5908.849 ± 148.167 | 12791.999 ± 182.307 | 2.165 | — | — |
| factorSolveLogDeterminant 128 | 237.865 ± 5.093 | 471.774 ± 18.914 | 1.983 | — | — |
| factorSolveLogDeterminant 512 | 6319.925 ± 294.036 | 12815.403 ± 498.657 | 2.028 | — | — |
| fittingRoundWithWorkspace 128 | 6028.673 ± 380.549 | 9747.301 ± 495.919 | 1.617 | — | — |
| fittingRoundWithWorkspace 512 | 160770.718 ± 2103.469 | 359415.893 ± 1807.606 | 2.236 | — | — |
| fittingRound 128 | 6297.975 ± 249.773 | 9607.953 ± 99.322 | 1.526 | — | — |
| fittingRound 512 | 156838.700 ± 7355.510 | 281060.489 ± 1295.573 | 1.792 | — | — |
| factorWithWorkspace precision:32 | 19.929 ± 0.753 | 16.845 ± 0.518 | 0.845 | — | — |
| factor precision:32 | 19.463 ± 0.627 | 16.191 ± 0.810 | 0.832 | — | — |
