# Package com.eignex.kumulant.schema.ops

The composition operators for the wire specs in
[com.eignex.kumulant.schema]. Each operator is an extension on a
modality-specific spec that returns another spec, so compositions stay
pure data and round-trip on the wire exactly like the leaf specs they
wrap. They are the spec-side mirror of the live operators in
[com.eignex.kumulant.operation]: where the live form wraps a running
stat, the form here wraps its spec, and the two produce the same
behaviour once materialized.

## What you can compose

The operators cover the same ground as the live package. Input-side
wrappers change what a stat sees: weighting and fixed-value injection,
windowing, sampling and throttling, lagging and feedback, and the
adapters that move a stat between modalities (a series stat reading the
x of a pair, a vector coordinate, and so on). Output-side wrappers change
what it reports: folding and projecting results, the scalers, and the
band around a center-scale result.

## AST-backed operators

The operators that take a projection or a predicate, such as filtering,
transforming, and weighting by a computed value, accept an expression
tree from [com.eignex.kumulant.schema.expr] rather than a live lambda, so
the whole composition still serializes. When a projection cannot be
expressed as that AST, the operation has no spec form and stays a
live-only lambda on the [com.eignex.kumulant.operation] side.
