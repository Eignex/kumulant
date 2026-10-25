# Benchmark cleanup plan

Keep deterministic fixtures in Kotlin and reuse JMH/kotlinx-benchmark and Lab's
existing profile interface. The complete PR must reduce maintained code.

- Remove handwritten throughput loops and duplicated report/task dispatch.
- Reuse stat and bandit registries for selection; prepare regression and bandit
  inputs outside timing and advance repeated timestamped streams.
- Keep serial stat, bandit regret and Cholesky checks. Consolidate concurrent
  stat checks with bounded cleanup and propagated worker failures.
- Add a small Lab/local launcher retaining existing outputs and run settings.
- Remove custom capture schemas, catalogue, fingerprinting, Native protocol
  adapter and their tests. Native uses its existing public Gradle task.
- Withdraw the custom Lab viewer; existing retained job files need no service change.
- Update instructions and documentation, run `check lintDocs --rerun-tasks`, and
  verify JVM/Native smoke runs, quality, failure retention and output protection.
