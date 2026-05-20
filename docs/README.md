# Kumulant docs

Kumulant is a Kotlin Multiplatform library for streaming statistics, online
learning, and bandits. This folder is the prose reference for how the
pieces fit together. The per-symbol reference lives in the KDocs.

Read in order:

1. [Overview](01-overview.md): mental model and lifecycle.
2. [Stats](02-stats.md): the four Stat modalities, the Result hierarchy,
   the family catalog.
3. [Concurrency](03-concurrency.md): the four concurrency levels and what
   they guarantee.
4. [Operations](04-operations.md): composing stats with weighting,
   filtering, windowing, transforms, and vectorisation.
5. [Schemas and the wire](05-schemas.md): declaring named bags of stats
   and shipping them between processes.
6. [Bandits](06-bandits.md): the bandit hierarchy, univariate and
   contextual families, policies, and arms.
