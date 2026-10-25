# Package com.eignex.kumulant.schema.expr

The serializable expression AST used by the input operations in
[com.eignex.kumulant.schema]. When an operation needs a projection or a
predicate that must travel on the wire rather than as a live Kotlin
lambda, it carries one of these trees instead. The whole tree is
serializable, so a filter or a transform round-trips as data and rebuilds
an identical closure on the far side.

## The three trees

There are three sealed hierarchies, one per result shape. [ScalarExpr]
evaluates to a single number from an update's value, its paired second
component, and the current vector. [VectorExpr] evaluates to a whole
vector and backs the per-feature projection and scaling operations.
[BoolExpr] evaluates to a boolean and drives filter predicates and the
guards on conditional nodes. JSON uses operator keys, numeric literals, and
named input references, such as `{"add":["x",1]}`.

## Leaves and operators

The scalar leaves are the update value [X], the paired second component
[Y], a vector coordinate [V], and a literal [Const]. Arithmetic composes
them with the ordinary operators and with dedicated nodes for the
transcendental functions: powers, logs, exponentials, square roots, and
absolute value. Comparisons between scalars produce a [BoolExpr], and the
boolean combinators join several predicates into one.

## Sugar nodes

Common shapes have dedicated nodes so a serialized tree stays readable
rather than ballooning into nested arithmetic. Standardizing, min-max
scaling, centering, and range tests each have their own node, as does the
multi-branch [Switch] and the conditional [IfExpr]. Vector projection has
nodes for dot products, element selection, single-coordinate access, and
a fold whose operator picks sum, min, max, or mean.

Filters, transforms, computed weights, and feedback projections use these
expression trees through [com.eignex.kumulant.schema.ops]. The live adapters
that evaluate them are internal implementation details.
