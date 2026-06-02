# Package com.eignex.kumulant.schema.spec

The spec catalog: a pure-data recipe for every stat in the library. Each
spec is a small serializable value carrying only configuration, with no
live cells, locks, or concurrency mode. This is the vocabulary that
travels on the wire; the live stats it describes are built from it in
[com.eignex.kumulant.schema.runtime].

## The sealed tree

[StatSpec] is the sealed root. Below it sit one interface per modality,
[SeriesStatSpec], [PairedStatSpec], [VectorStatSpec], [DiscreteStatSpec],
and [RegressionStatSpec], each carrying its result type as a phantom
marker that threads through schema declaration and materialization
without appearing on the wire. Every concrete spec is a data class, or a
data object for the parameter-less ones, sitting under the modality it
materializes into. Because the hierarchy is sealed, the whole catalog
lives in this one package; that is a deliberate constraint, not an
accident of layout.

## Wire shape

Polymorphism is by name: each spec carries a serial discriminator matching
its Kotlin class name, so any format with open polymorphism, JSON, CBOR,
or Protobuf, puts the same type string on the wire. Defaults on each spec
mirror the underlying stat's constructor defaults, so an encoded payload
stays terse when the format is configured to omit defaults. Construction
lives elsewhere on purpose: a spec is inert data, and turning it into a
live stat happens through the materialize functions in
[com.eignex.kumulant.schema.runtime], with the concurrency mode supplied
at that point rather than carried on the wire.

## Beyond the leaf specs

The catalog also holds the composed specs. The wrappers behind the
operators in [com.eignex.kumulant.schema.ops] are spec variants too, so a
filtered or windowed stat serializes as one tree, and
[GroupStatSpec] nests a whole sub-schema as a single entry. Weighting
strategies live in [com.eignex.kumulant.schema.decay] and optimizer
strategies in [com.eignex.kumulant.schema.optimizer]; the specs here
reference those as configuration.
