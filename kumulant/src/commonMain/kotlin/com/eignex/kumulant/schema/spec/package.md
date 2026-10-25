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

JSON uses canonical operator-key nodes such as `{"mean":{}}` and
`{"ewma":{"alpha":0.1}}`. Expressions use structured operators and numeric
literals; decorators retain an explicit inner tree. Serialize through [StatSpec]
or a modality root, or embed specs in a schema or bandit. The
[JSON reference](https://github.com/Eignex/kumulant/blob/main/docs/JSON_FORMAT.md)
lists every shape, default-field rules, and migration details.

Non-JSON formats, including Protobuf and CBOR, retain the generated sealed-class
representation and serial names. Defaults match the underlying stat constructors
and follow the format's `encodeDefaults` setting. Materialization supplies
concurrency separately from the wire payload.

## Beyond the leaf specs

The catalog also holds the composed specs. The wrappers behind the
operators in [com.eignex.kumulant.schema.ops] are spec variants too, so a
filtered or windowed stat serializes as one tree, and
[GroupStatSpec] nests a whole sub-schema as a single entry. Weighting
strategies live in [com.eignex.kumulant.schema.decay] and optimizer
strategies in [com.eignex.kumulant.schema.optimizer]; the specs here
reference those as configuration.
