# Package com.eignex.kumulant.schema.runtime

Turns the pure-data specs of [com.eignex.kumulant.schema] into live,
running accumulators. The specs in the parent package say what to build;
this package builds it, groups the results, and fans every update out to
the right stat. Nothing here goes on the wire: it is the materialization
and grouping layer that sits between a decoded schema and the live stats
it stands up.

## Materialization

Every spec materializes through one of the modality-specific extension
functions, each of which takes a
[Concurrency][com.eignex.kumulant.core.Concurrency] mode and returns a
live stat of the matching modality. The concurrency mode is the only
input that isn't already on the spec; it is the deployment knob passed in
at build time, so the same decoded payload can run unsynchronized in a
test and strictly synchronized in a contended hot loop.

## Declaring and reading a group

[StatSchema] is the declarative entry point. A schema subclass registers
its stats through the series, paired, vector, discrete, and regression delegates, and
each delegate hands back a [com.eignex.kumulant.schema.StatKey] carrying
the result type. The schema materializes into a [StatGroup], which is
itself a series stat over [com.eignex.kumulant.schema.GroupResult]: it
forwards every update to all of its entries and collects their snapshots
into one result, read back by typed key rather than by string. The
[PairedStatGroup], [VectorStatGroup], [DiscreteStatGroup], and [RegressionStatGroup] variants
fan updates only to entries of their own modality.

The group constructor is where [Concurrency][com.eignex.kumulant.core.Concurrency]
is chosen. A schema describes stats and nothing else, so the level passed
there is what every stat it describes is built with, nested groups
included, and a group never reports a level its members were not built at.

A schema flattens to its pure-data form as a [StatSchemaDef], which is
the part that crosses the wire. A coordinator can stand up the same group
from a definition, run it independently, and fold workers' snapshots back
in through merge.

## Building groups by hand

Groups can also be assembled directly from [BoundStat] entries. The same
built-in accumulators and compositions are available as serializable specs;
use [StatSchema] or [StatSchemaDef] when the configuration must travel on the wire.
