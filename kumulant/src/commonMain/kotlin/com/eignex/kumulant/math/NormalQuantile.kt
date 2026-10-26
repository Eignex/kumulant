package com.eignex.kumulant.math

import kotlin.math.abs
import kotlin.math.ln
import kotlin.math.sqrt

/*
ASA241 normal quantile coefficients
Original algorithm: Michael Wichura, Applied Statistics 37(3), 477-484 (1988).
Fortran90 transcription: John Burkardt.
Source: https://people.sc.fsu.edu/~jburkardt/f_src/asa241/asa241.f90
Distributed by the author under the MIT license:
https://people.sc.fsu.edu/~jburkardt/f_src/asa241/asa241.html

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above attribution and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

// Wichura, AS 241 (1988); coefficients from John Burkardt's MIT-licensed ASA241 distribution.
// https://people.sc.fsu.edu/~jburkardt/f_src/asa241/asa241.f90
internal fun standardNormalQuantile(probability: Double): Double {
    require(probability > 0.0 && probability < 1.0)
    val centered = probability - 0.5
    if (abs(centered) <= 0.425) {
        val coordinate = 0.180625 - centered * centered
        return centered * polynomial(
            normalCentralNumerator,
            coordinate,
        ) / polynomial(normalCentralDenominator, coordinate)
    }
    val tail = sqrt(-ln(if (centered < 0.0) probability else 1.0 - probability))
    val value = if (tail <= 5.0) {
        polynomial(normalTailNumerator, tail - 1.6) / polynomial(normalTailDenominator, tail - 1.6)
    } else {
        polynomial(normalExtremeNumerator, tail - 5.0) / polynomial(normalExtremeDenominator, tail - 5.0)
    }
    return if (centered < 0.0) -value else value
}

private fun polynomial(coefficients: DoubleArray, coordinate: Double): Double {
    var value = coefficients.last()
    for (index in coefficients.lastIndex - 1 downTo 0) value = value * coordinate + coefficients[index]
    return value
}

private val normalCentralNumerator = doubleArrayOf(
    3.3871328727963666080e+00,
    1.3314166789178437745e+02,
    1.9715909503065514427e+03,
    1.3731693765509461125e+04,
    4.5921953931549871457e+04,
    6.7265770927008700853e+04,
    3.3430575583588128105e+04,
    2.5090809287301226727e+03,
)

private val normalCentralDenominator = doubleArrayOf(
    1.0e+00,
    4.2313330701600911252e+01,
    6.8718700749205790830e+02,
    5.3941960214247511077e+03,
    2.1213794301586595867e+04,
    3.9307895800092710610e+04,
    2.8729085735721942674e+04,
    5.2264952788528545610e+03,
)

private val normalTailNumerator = doubleArrayOf(
    1.42343711074968357734e+00,
    4.63033784615654529590e+00,
    5.76949722146069140550e+00,
    3.64784832476320460504e+00,
    1.27045825245236838258e+00,
    2.41780725177450611770e-01,
    2.27238449892691845833e-02,
    7.74545014278341407640e-04,
)

private val normalTailDenominator = doubleArrayOf(
    1.0e+00,
    2.05319162663775882187e+00,
    1.67638483018380384940e+00,
    6.89767334985100004550e-01,
    1.48103976427480074590e-01,
    1.51986665636164571966e-02,
    5.47593808499534494600e-04,
    1.05075007164441684324e-09,
)

private val normalExtremeNumerator = doubleArrayOf(
    6.65790464350110377720e+00,
    5.46378491116411436990e+00,
    1.78482653991729133580e+00,
    2.96560571828504891230e-01,
    2.65321895265761230930e-02,
    1.24266094738807843860e-03,
    2.71155556874348757815e-05,
    2.01033439929228813265e-07,
)

private val normalExtremeDenominator = doubleArrayOf(
    1.0e+00,
    5.99832206555887937690e-01,
    1.36929880922735805310e-01,
    1.48753612908506148525e-02,
    7.86869131145613259100e-04,
    1.84631831751005468180e-05,
    1.42151175831644588870e-07,
    2.04426310338993978564e-15,
)
