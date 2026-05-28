# Kumulant style guide

## Bandit KDoc template

Every bandit class (anything implementing `UnivariateBandit`, `ContextualBandit`,
or built on top of `PerArmBandit` / `Scorable` / `ContextualScorable`) should
have a KDoc that follows this template.

```kotlin
/**
 * <One-sentence summary: what family this bandit belongs to and how it picks
 * arms. Name the reward model (Bernoulli/Gaussian/weighted-mean/regression) if
 * it's load-bearing.>
 *
 * <Algorithm paragraph when non-trivial; name the policy (Thompson sampling,
 * UCB1, EXP3, Boltzmann, LinUCB, k-NN scoring, ...), cite the paper if
 * relevant, call out the exploration/exploitation knob and any regret bound.>
 *
 * <Configuration paragraph when the constructor has non-obvious knobs; what
 * each tunable does, defaults, exploration tradeoffs, history caps.>
 *
 * **Use cases:** <when to reach for this bandit over its peers; reward shape,
 * context shape, stationarity assumptions.>
 *
 * **Arms:** <indexless univariate / contextual with feature dim D / composite
 * over sub-arms; whether arm count is fixed at construction.>
 *
 * **Memory:** <O(...) total, broken out as per-arm × arms when that's the
 * dominant term. Examples: `O(nbrArms)`, `O(nbrArms · D^2)` (linear models),
 * `O(nbrArms · maxHistoryPerArm · D)` (k-NN reservoirs).>
 *
 * **Choose:** <O(...) per `choose()` call. Examples: `O(nbrArms)` (argmax over
 * scores), `O(nbrArms · D)` (linear scoring), `O(nbrArms · history · D)`
 * (k-NN), `O(nbrArms · log nbrArms)` (sort or wheel build).>
 *
 * **Update:** <O(...) per `update()` call. Examples: `O(1)` (single-arm
 * accumulator), `O(D)` (linear gradient), `O(D^2)` (Sherman-Morrison),
 * `O(history)` (reservoir trim).>
 *
 * **Randomness:** <where the bandit pulls from; always [Bandit.random]; and
 * which calls are deterministic given a fixed seed (choose, internal
 * sampling, tie-breaks).>
 *
 * **Concurrency:** <mechanism, what's safe to call concurrently from multiple
 * threads, what isn't. See *Concurrency clauses* below.>
 */
class FooBandit(...) : UnivariateBandit, PerArmBandit<FooArmResult> { ... }
```

### Required and optional sections

Always present, in this order:

1. **Summary**; one sentence, no header. The family, the selection rule, the
   reward model if non-obvious.
2. **`**Use cases:**`**; one to three sentences. Which problem shape this
   bandit fits, what its peers are.
3. **`**Arms:**`**; arm model in one line: indexless vs contextual, feature
   dimension if relevant, whether `nbrArms` is fixed at construction.
4. **`**Memory:**`**; O-bound, total. Break out the per-arm factor when arms
   dominate (e.g. `O(nbrArms · D^2)` rather than `O(D^2)`).
5. **`**Choose:**`**; O-bound per `choose()` call.
6. **`**Update:**`**; O-bound per `update()` call.
7. **`**Randomness:**`**; single source clause; what is reproducible under a
   fixed seed.
8. **`**Concurrency:**`**; mechanism first, then what's safe across threads,
   then caveats. See *Concurrency clauses* below.

Optional, between the summary and the bold sections:

- **Algorithm paragraph**: the policy, the paper, the regret story, the
  exploration knob. Skip when the bandit is a thin wrapper or a direct
  re-export of an existing policy.
- **Configuration paragraph**: non-obvious constructor knobs and their
  tradeoffs. Skip when the bandit takes only `nbrArms`, a policy, and
  `random`.

### Section style

- Use `**Label:**` inline bold, **not** `#` markdown headings. Heading-style
  blocks render too heavy inside per-class KDocs.
- Each `**Section:**` body should be one to three sentences. If you need more,
  the algorithm/configuration paragraph above is the place.
- Declarative, not narrative: *"Per-arm `SeriesStat`s update independently"*
  beats *"We use a per-arm `SeriesStat` because…"*.
- Per-arm result data classes keep their existing per-field `/** ... */` docs
  unchanged; the template applies only to the bandit class itself.

### Concurrency clauses

State what's safe across threads in 3–6 words, then the caveats. Reuse these
standard phrasings so the matrix is grep-able:

| Mechanism                                                         | Behaviour                                                                                                                          |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| *Per-arm `SeriesStat` carries its own concurrency*                | `choose` and `update` are as safe as the arm stat allows. Cross-arm consistency is best-effort; racing updates on different arms never block. |
| *Single atomic counter for round index*                           | `choose` increments a shared step counter atomically; concurrent choosers all see distinct `t` values.                              |
| *Body locked under any concurrent level*                          | All public calls serialise on one lock. Exact, but throughput bound by lock contention; shard for higher write rates.              |
| *Lock-free with racing reads of arm snapshots*                    | `choose` reads a fresh snapshot per arm without locking; a concurrent `update` may interleave and the chosen arm reflects either pre- or post-update state. |
| *Caller-supplied `random` is the synchronisation boundary*        | Thread-safety of `choose` reduces to thread-safety of [Bandit.random]; pass a thread-local or synchronised wrapper for multi-thread choose. |

Cite measured drift or contention numbers from `:kumulant-bench:` when known;
drop them if not.

When a bandit is a thin wrapper over another, each section can collapse to one
line: `**Concurrency:** Inherits [MultiArmedBandit]'s concurrency model.`

### Worked example

```kotlin
/**
 * Univariate bandit with a fixed number of independent arms, each backed by a
 * kumulant [SeriesStat]; on every `choose` the bandit asks the [policy] to
 * score a fresh snapshot per arm and picks the argmax.
 *
 * The selection rule and the arm accumulator both live in [BanditPolicy], so
 * swapping Thompson sampling for UCB1 is a policy swap, not a bandit swap.
 *
 * **Use cases:** stationary multi-armed problems with scalar rewards; any
 * policy expressible as "score each arm independently, pick the max".
 *
 * **Arms:** indexless, `nbrArms` fixed at construction; each arm owns one
 * [SeriesStat] from `policy.createArm()`.
 *
 * **Memory:** O(nbrArms · arm-state); per-arm `SeriesStat` plus a shared
 * step counter.
 *
 * **Choose:** O(nbrArms); one `policy.evaluate` per arm, argmax.
 *
 * **Update:** O(1) on the targeted arm, delegated to `policy.update`.
 *
 * **Randomness:** every `policy.evaluate` and `policy.update` receives the
 * caller-supplied [random]; reproducible under a fixed seed if the policy is.
 *
 * **Concurrency:** per-arm `SeriesStat` carries its own concurrency. Racing
 * `update`s on different arms never block; concurrent `choose`s share the
 * atomic step counter so each sees a distinct `t`. Cross-arm snapshot
 * consistency is best-effort.
 */
class MultiArmedBandit<R : Result>(...) : UnivariateBandit, PerArmBandit<R>, Scorable
```
