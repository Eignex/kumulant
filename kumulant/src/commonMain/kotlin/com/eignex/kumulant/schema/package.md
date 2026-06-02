# Package com.eignex.kumulant.schema

The schema layer lets you declare a named, typed bag of stats once and
treat it as a single unit: every update fans out to all of them, results
read back by typed key, and the declaration itself travels on the wire so
another process can stand up the same bag and merge snapshots back.

## When you need it

Reach for the schema layer when you have more than one stat to run
together, when the choice of stats has to travel as data, a remote
worker, a stored configuration, a UI that assembles stats from user
input, or when you want many workers to run the same bag and fold their
partial results into one. If you only have a single stat in a single
process, you don't need any of this; construct the stat directly and call
it. The schema layer earns its keep the moment configuration has to be
named, grouped, or serialized.

The two types that tie it together live in this root package. A
[GroupResult] is the aggregated snapshot, a map from key to per-stat
result, and [StatKey] (with [GroupStatKey] for nested groups) is the
typed handle you read it back with, so a lookup returns the stat's own
result type rather than an untyped value.

## How it's organized

The rest of the layer is split into focused subpackages:

- [com.eignex.kumulant.schema.spec] is the spec catalog: a pure-data
  recipe for every stat in the library. This is the vocabulary you author
  and serialize, the sealed `StatSpec` tree and all its variants.
- [com.eignex.kumulant.schema.ops] holds the composition operators that
  wrap one spec into another, filtering, windowing, weighting, scaling,
  and the modality adapters, each returning a spec so compositions stay
  serializable.
- [com.eignex.kumulant.schema.expr] is the serializable expression AST
  those operators carry when a projection or predicate has to travel on
  the wire instead of as a live lambda.
- [com.eignex.kumulant.schema.optimizer] and
  [com.eignex.kumulant.schema.decay] are the optimizer and
  decay-weighting strategy configurations that the regression and decay
  specs reference.
- [com.eignex.kumulant.schema.runtime] is the materialization and
  grouping layer: the `StatSchema` you subclass to declare a bag, the
  `StatGroup` it materializes into, and the materialize functions that
  turn any spec into a live stat.

## The typical flow

Declare a schema of specs, composed with operators where you need them,
in a [com.eignex.kumulant.schema.runtime.StatSchema] subclass. Materialize
it into a [com.eignex.kumulant.schema.runtime.StatGroup], feed it updates,
and read a [GroupResult] back by [StatKey]. To cross a process boundary,
encode the schema's flat
[com.eignex.kumulant.schema.runtime.StatSchemaDef] form, rebuild the same
group on the far side, and merge each worker's `GroupResult` into a
coordinator running the same shape.
