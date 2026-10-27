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
named references, such as `{"add":[{"ref":"value"},1]}`.

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

## Portable expression profile

The portable subset matches Klause's expression JSON without sharing an AST. A named
scalar is `{"ref":"value"}`, a constant is a bare number, and an object contains exactly
one operator. String expressions are not parsed.

| Payload | Operators and operand order |
| --- | --- |
| Single bare operand | `abs`, `not` |
| Exactly two operands | `add`, `mul`, `min`, `max`, `pow`, `and`, `or`, `lt`, `le`, `eq`, `ge`, `gt` |
| Exactly three operands | `if`: condition, then branch, else branch |

The common numeric domain consists of finite integral literals in Klause's signed
32-bit range, with nonnegative integral literal exponents for `pow`. `0^0` is one.
Klause evaluates integer expressions exactly; Kumulant evaluates each node in binary64.
Integers through magnitude `2^53` are exactly representable in binary64. Beyond that,
rounding can change a value. Both environments encode integral literal values with no
fraction part. Kumulant preserves negative zero as `-0`.

Kumulant retains every binary grouping and evaluates operands in their written order.
For example, adding two small numbers before a large one can produce a different
result from adding each small number to the large one in turn. Variadic `add`, `min`,
`max`, `and`, and `or` are Klause integer extensions; Kumulant rejects those shapes.
Klause writes its own chains nested to the left, subtraction as `add` of `mul` by -1,
and constant multiplication with the constant first, so its DSL output stays portable.

`div` is binary64 division, including IEEE 754 infinities and NaN from division by
zero. Klause's integer operations use `truncDiv` and `truncMod`; Kumulant rejects them.
Fractional literals, fractional or expression-valued exponents, negative exponents,
`sub`, `neg`, and transcendental functions are Kumulant extensions. Negative bases
with fractional exponents can produce NaN. JSON does not carry non-finite literals
under the default settings; an evaluation may still produce a non-finite result.

The shared `portable-expression/conformance.json` JVM resource is a byte-identical
copy of Klause's fixture. Each case records accepted shapes, canonical output,
evaluation results for both numeric environments, and explicit rejections.

## Explicit reference bindings

[Ref] names have no implicit relationship to an observation field or a feedback stat.
Bind the fragment to its sources before using it in a stat:

```kotlin
val predicate = (Ref("value") ge 0.0).bind(mapOf("value" to X))
val statistic = Mean.filter(predicate)
```

The predicate serializes with its scope, so a consumer can decode and materialize the
stat directly:

```json
{"bind":{"expr":{"ge":[{"ref":"value"},0]},"bindings":{"value":{"input":"x"}}}}
```

| Kotlin source | Kumulant extension JSON | Meaning |
| --- | --- | --- |
| [X] | `{"input":"x"}` | Primary scalar input, or the current element in an element-wise operation |
| [Y] | `{"input":"y"}` | Paired secondary input or regression response |
| [V] | `{"element":0}` | Explicit vector coordinate |
| [Center], [Scale] | `{"feedback":"center"}`, `{"feedback":"scale"}` | Current primary's center or scale |
| [Low], [High] | `{"feedback":"low"}`, `{"feedback":"high"}` | Current primary's minimum or maximum |
| [VIndex] | `{"feedback":"index"}` | Coordinate of an element-wise feedback primary |

Bindings can refer to other named bindings or to scalar expressions. Binding checks
all branches eagerly, rejects unresolved names and cycles, and preserves grouping.
Nested binding scopes are independent. Feedback sources retain their primary-result
requirements, and vector coordinates retain bounds checks. A name such as `x` remains
a user-defined reference even when a built-in input source occurs in the same tree.
Raw fragments can be decoded for a host to bind; statistical recipes with unresolved
references are rejected when materialized.

## Embedding and JSON Schema

[com.eignex.kumulant.schema.KumulantJson.serializers] exposes every vocabulary for
composition with a host library's own compact serializers.
[com.eignex.kumulant.schema.KumulantJson.catalog] describes nested stats, expressions,
and their custom payload layouts. Use Skema's document-root decoder to retain error
paths through the host model's records, maps, and lists:

```kotlin
import com.eignex.skema.serialization.decodeFromJsonElement
import com.eignex.skema.serialization.toJsonSchema

val tree = SchemaJson.parseToJsonElement(payload)
val model = SchemaJson.decodeFromJsonElement(Model.serializer(), tree, KumulantJson.catalog)
val schema = Model.serializer().toJsonSchema(KumulantJson.catalog)
```

A host with its own compact vocabulary can build a `JsonNodeCatalog` whose supplier
returns its serializers plus `KumulantJson.serializers`. The upstream document-root
decoder validates unknown fields even when JSON is configured to ignore them.

## Migration

Replace bare input strings such as `"x"` with `{"input":"x"}` and bare feedback
strings such as `"center"` with `{"feedback":"center"}`. Use `{"ref":"name"}`
with an explicit binding scope for portable fragments. Replace a named-field `if`
payload with `[condition, then, otherwise]`. Integral constants encode as integer
tokens. The previous spellings are rejected; there is one canonical expression format.
