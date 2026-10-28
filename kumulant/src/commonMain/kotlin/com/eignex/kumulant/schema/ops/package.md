# Package com.eignex.kumulant.schema.ops

The composition operators for the wire specs in
[com.eignex.kumulant.schema]. Each operator is an extension on a
modality-specific spec that returns another spec, so compositions stay
pure data and round-trip on the wire exactly like the leaf specs they
wrap. Materialization builds the internal live adapters from the composed
spec; callers author compositions through this package.

## What you can compose

Input-side wrappers change what a stat sees: weighting and fixed-value injection,
windowing, sampling and throttling, lagging and feedback, and the
adapters that move a stat between modalities (a series stat reading the
x of a pair, a vector coordinate, and so on). Output-side wrappers change
what it reports: folding and projecting results, the scalers, and the
band around a center-scale result.

## AST-backed operators

The operators that take a projection or a predicate, such as filtering,
transforming, and weighting by a computed value, accept an expression
tree from [com.eignex.kumulant.schema.expr] rather than a live lambda, so
the whole composition still serializes. Live adapters compile these trees
into internal update closures during materialization.
