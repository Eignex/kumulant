package com.eignex.kumulant.schema.expr

import kotlin.math.floor

// IDs past 2^53 have no exact binary64 image, so the bare identity and comparisons of the ID
// against a literal run on the Long itself. Every other node evaluates in binary64.

private const val TWO_POW_63 = 9.223372036854775808E18

internal fun ScalarExpr.isDiscreteIdentity(): Boolean = when (this) {
    X -> true
    is BoundScalarExpr -> resolved.isDiscreteIdentity()
    else -> false
}

internal fun BoolExpr.discretePredicate(): (Long) -> Boolean = when (this) {
    is BoundBoolExpr -> resolved.discretePredicate()
    is And -> conjunction(l.discretePredicate(), r.discretePredicate())
    is Or -> disjunction(l.discretePredicate(), r.discretePredicate())
    is Not -> negation(a.discretePredicate())
    is Gt -> comparison(l, r) { it > 0 }
    is Ge -> comparison(l, r) { it >= 0 }
    is Lt -> comparison(l, r) { it < 0 }
    is Le -> comparison(l, r) { it <= 0 }
    is Eq -> comparison(l, r) { it == 0 }
    is In -> if (of.isDiscreteIdentity()) membership(values.toDoubleArray()) else binary64Predicate()
    else -> binary64Predicate()
}

private fun conjunction(left: (Long) -> Boolean, right: (Long) -> Boolean): (Long) -> Boolean =
    { id -> left(id) && right(id) }

private fun disjunction(left: (Long) -> Boolean, right: (Long) -> Boolean): (Long) -> Boolean =
    { id -> left(id) || right(id) }

private fun negation(inner: (Long) -> Boolean): (Long) -> Boolean = { id -> !inner(id) }

private fun membership(candidates: DoubleArray): (Long) -> Boolean =
    { id -> candidates.any { compareExact(id, it) == 0 } }

private fun BoolExpr.binary64Predicate(): (Long) -> Boolean = { id -> eval(id.toDouble()) }

private fun ScalarExpr.constantOrNull(): Double? = when (this) {
    is Const -> v
    is BoundScalarExpr -> resolved.constantOrNull()
    else -> null
}

// An unordered comparison (NaN literal) is false for every operator, as in binary64.
private inline fun BoolExpr.comparison(
    left: ScalarExpr,
    right: ScalarExpr,
    crossinline holds: (Int) -> Boolean,
): (Long) -> Boolean {
    val rightConstant = right.constantOrNull()
    if (left.isDiscreteIdentity() && rightConstant != null) {
        return { id -> compareExact(id, rightConstant).let { it != null && holds(it) } }
    }
    val leftConstant = left.constantOrNull()
    if (leftConstant != null && right.isDiscreteIdentity()) {
        return { id -> compareExact(id, leftConstant).let { it != null && holds(-it) } }
    }
    return binary64Predicate()
}

private fun compareExact(id: Long, literal: Double): Int? = when {
    literal.isNaN() -> null

    literal >= TWO_POW_63 -> -1

    literal < -TWO_POW_63 -> 1

    else -> {
        val whole = floor(literal)
        val wholeId = whole.toLong()
        when {
            id != wholeId -> id.compareTo(wholeId)
            literal > whole -> -1
            else -> 0
        }
    }
}
