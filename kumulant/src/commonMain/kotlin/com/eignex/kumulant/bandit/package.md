# Package com.eignex.kumulant.bandit

Multi-armed and contextual bandits built on the same
[Stat][com.eignex.kumulant.core.Stat] /
[Result][com.eignex.kumulant.core.Result] foundation as the rest of the
library.

## Why bandits live here

A multi-armed bandit is the simplest reinforcement-learning shape: on
each round you pick one of K actions ("arms"), receive a reward, and
the only feedback you get is the reward for the arm you actually
played. You never learn what would have happened for the arms you did
not pick. The bandit's job is to balance exploitation (play the arm
that looks best) against exploration (play a different arm to learn
whether it might be better), so cumulative reward converges toward what
an oracle who knew the best arm in advance would have collected.

In a **contextual** bandit each round also comes with a feature vector,
and the reward depends on both the chosen arm and the context. Linear,
nearest-neighbour, and tree-based contextual bandits all fit that shape
and live in the contextual family.

Typical use cases:

- A/B-test-style optimisation under a budget (picking which creative,
  variant, or arm to show next).
- Online recommendation (which item to show this user given their
  feature vector).
- Operator selection inside meta-heuristics (which neighbourhood move
  to try inside an LNS solver).
- Any sequential decision problem where the data collection itself
  should adapt as evidence accumulates rather than running a
  fixed-size experiment.

Bandits fit kumulant naturally because they are themselves streaming
problems. Observations arrive one at a time, the per-arm state has to
stay bounded no matter how long the run goes, and the same evidence
that drives `choose` is the evidence the rest of the library is already
tracking. A bandit arm is a kumulant accumulator viewed through a
scoring rule, so the same Welford means, exponential-weight cells, and
regression posteriors that power streaming summaries also power
Thompson sampling, UCB, EXP3, and LinUCB. Two replicas of a bandit can
train in parallel and stitch their snapshots back together with `merge`,
the same way two parallel mean estimators do.

## Interface hierarchy

The action surface and the state surface are orthogonal, so each bandit
family implements exactly the pieces that fit.

- `Bandit`; common root: `nbrArms`, `random`, `reset`.
- `UnivariateBandit`; `choose()` and `update(arm, value, weight)` for
  indexless arms.
- `ContextualBandit`; `choose(x)` and `update(arm, x, reward, weight)`
  for per-round context vectors.
- `Snapshotable`; `snapshot`, `merge`, `create(random)`. State shape
  is whatever the bandit family needs.
- `PerArmBandit`; convenience for the common case where state is one
  [Result][com.eignex.kumulant.core.Result] per arm; extends
  `Snapshotable` over a list of results and adds a per-arm `armResult`
  accessor.
- `Scorable`; opt-in: exposes `evaluate(armIndex)` when selection is
  an argmax over independent per-arm scores. `ContextualScorable` is
  the contextual analogue.

Joint-sampling bandits (Boltzmann, top-two Thompson) do not expose
`Scorable` because no per-arm score is meaningful in isolation. Exp3 /
Exp4 do not fit `PerArmBandit` because their state is not per-arm. Each
concrete bandit's KDoc states which interfaces it implements and why.

## Subpackages

- `bandit.univariate`; Indexless arms: epsilon-greedy / decreasing,
  UCB1 / KL-UCB / MOSS, Thompson, Boltzmann, EXP3, multi-armed shells,
  roulette-wheel selection.
- `bandit.contextual`; Per-arm regression bandits over the
  [com.eignex.kumulant.stat.regression] family: linear (Bayesian,
  diagonal, stochastic), kNN, tree- and forest-based.
- `bandit.policy`; Pluggable scoring policies (Greedy, EpsilonGreedy,
  UCB1, ThompsonSampling, KLUcb, MOSS, etc.) shared by univariate
  bandits.

## Wire portability

A bandit's `BanditSpec` round-trips through the same skema-based
mechanism as [com.eignex.kumulant.schema.StatSpec]: declare a spec,
encode to JSON / CBOR, ship, decode, materialise. The same data classes
parameterise both Bandit construction in code and Bandit construction
from the wire.
