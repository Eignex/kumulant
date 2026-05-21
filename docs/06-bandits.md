# Bandits

A multi-armed bandit is the simplest reinforcement-learning shape: on
each round you pick one of K actions ("arms"), receive a reward, and
the only feedback you ever get is the reward for the arm you actually
played. You never learn what would have happened for the arms you did
not pick. The bandit's job is to balance exploitation (play the arm
that looks best) against exploration (play a different arm to learn
whether it might be better), so the running total reward converges
toward what an oracle who knew the best arm in advance would have
collected.

In a contextual bandit each round also comes with a feature vector,
and the reward depends on both the chosen arm and the context. The
problem becomes "learn a per-arm reward model and pick the arm whose
model gives the best score at this context". Linear contextual
bandits, k-nearest-neighbour bandits, and tree-based bandits all fit
that shape and live in the contextual family.

Typical use cases include A/B-test-style optimisation under a budget
(say, picking which of several creatives to show next), online
recommendation (which item to show this user given their feature
vector), operator selection inside meta-heuristics (which neighbourhood
move to try next inside an LNS solver), and any sequential decision
problem where you want the data collection itself to adapt as evidence
accumulates rather than running a fixed-size experiment.

Bandits fit kumulant naturally because they are themselves streaming
problems. Observations arrive one at a time, the per-arm state has to
stay bounded no matter how long the run goes, and the same evidence
that drives the choose call is the evidence the rest of the library is
already tracking. A bandit arm is just a kumulant accumulator viewed
through a scoring rule, so the same Welford means, the same
exponential-weight cells, and the same regression posteriors that
power streaming summaries also power Thompson sampling, UCB, EXP3, and
LinUCB. Each arm owns a kumulant accumulator, the bandit picks arms by
scoring snapshots, and the per-arm state inherits the same concurrency
contract, wire-portable result types, and merge semantics as any other
stat. Two replicas of a bandit can train in parallel and stitch their
snapshots back together with merge, the same way two parallel mean
estimators do.

## The interface hierarchy

The action surface and the state surface are orthogonal, so each bandit
family implements exactly the pieces that fit.

Bandit is the common root with nbrArms, random, and reset.
UnivariateBandit adds choose and update(arm, value, weight) for
indexless arms. ContextualBandit adds choose(x) and
update(arm, x, reward, weight) for per-round context vectors.

Snapshotable adds snapshot, merge, and create(random). The state shape
is whatever the bandit family needs. PerArmBandit is the convenience for
the common case where state is one Result per arm: it extends
Snapshotable over a list of results and adds a per-arm armResult
accessor.

Scorable is opt-in: a bandit exposes evaluate(armIndex) when its
selection is an argmax over independent per-arm scores.
ContextualScorable is the contextual analogue: evaluate(armIndex, x).

Joint-sampling bandits (Boltzmann, Top-Two Thompson) do not expose
Scorable because no per-arm score is meaningful in isolation. Exp3 and
Exp4 do not fit PerArmBandit because their state is not per-arm. Each
concrete bandit's KDoc states which interfaces it implements and why.

## Univariate bandits

All live in com.eignex.kumulant.bandit.univariate.

MultiArmedBandit is the workhorse. It takes a BanditPolicy that owns the
per-arm accumulator (a SeriesStat) and the per-arm scoring rule, so
swapping Thompson sampling for UCB1 is a policy swap rather than a
bandit swap.

BoltzmannBandit samples from a softmax over per-arm means with a
cooling temperature schedule. Exp3Bandit is the adversarial bandit with
an exponential-weights update and a regret bound under non-stationary
reward distributions. RouletteWheelBandit is the operator-selection
bandit from Ropke-Pisinger ALNS, batch-rebalancing weights every
segmentLength updates. TopTwoThompsonBandit is the best-arm
identification variant of Thompson sampling.

## Contextual bandits

All live in com.eignex.kumulant.bandit.contextual.

RegressionContextualBandit holds one RegressionStat per arm scored by a
shared RegressionPosterior. It covers Linear Thompson Sampling
(BayesianRegressionStat with MultivariateGaussian), LinUCB (any linear
regressor with LinUcb), greedy SGD (StochasticRegressionStat with a
PointPosterior), and tree and forest bandits when paired with the
corresponding posteriors. An optional continuous-pooling mode adds a
global regressor that absorbs every (x, reward) and lets per-arm
regressors fit residuals against its mean prediction.

KnnContextualBandit is non-parametric: each arm keeps a bounded history
of (context, reward, weight) triples and is scored by k-NN mean reward
plus a UCB-style bonus. Exp4Bandit is the adversarial contextual bandit
over a fixed pool of experts; per-expert exponential weights update from
IPS-corrected gain.

## Policies for MultiArmedBandit

A BanditPolicy is the (arm cumulator, scoring rule) pair. It constructs
per-arm SeriesStats via createArm and scores arm snapshots via
evaluate(snapshot, t, random).

The built-ins are ThompsonSampling over any (Arm, Posterior) combo,
with a BetaBernoulliTS shortcut for Bernoulli rewards plus the
Normal-Gamma, Log-Normal-Gamma, Poisson-Gamma, Geometric-Beta, and
Exponential-Gamma posteriors over the matching arm shapes; UCB1,
UCB1Normal, and UCB1Tuned for Auer's deterministic confidence-bound
rules; Greedy, EpsilonGreedy, and EpsilonDecreasing for exploit-only
and epsilon-exploration baselines; UniformSelection as a pure-exploration
control; and KlUcb, Moss, and UcbV for finer-grained confidence bounds.

### Arms and posteriors

An Arm describes the per-arm prior and reward model. The built-ins
include BernoulliArm, MeanArm, NormalArm, LogNormalArm, and MomentsArm.

A Posterior is the conjugate posterior for an Arm, sampled by
ThompsonSampling. The built-ins include BetaPosterior (for BernoulliArm),
PoissonGammaPosterior, GeometricBetaPosterior, ExponentialGammaPosterior,
NormalGammaPosterior, LogNormalGammaPosterior, and GammaScalePosterior.

### Composite arms

CompositeArm and CompositePosterior model multi-component rewards such
as zero-inflated lognormal revenue without one class per shape. Routing
and score combination travel as the same expression ASTs used elsewhere
in the library, so the whole composite serialises with the bandit's
spec.

## Wire-portable specs

Every bandit has a serializable Spec, so a whole bandit configuration
travels as data:

```kotlin
val spec: UnivariateBanditSpec = MultiArmedSpec(
    nbrArms = 4,
    policy = Ucb1Spec(alpha = 1.5),
)
val live: Bandit = spec.materialize(Random(0))
```

The univariate specs are sealed under UnivariateBanditSpec
(MultiArmedSpec, RouletteWheelSpec, BoltzmannSpec, Exp3Spec,
TopTwoThompsonSpec); the contextual ones under ContextualBanditSpec
(RegressionContextualSpec, KnnContextualSpec). Policies are sealed under
BanditPolicySpec (ThompsonSamplingSpec, Ucb1Spec, Ucb1NormalSpec,
Ucb1TunedSpec, GreedySpec, EpsilonGreedySpec, EpsilonDecreasingSpec,
UniformSelectionSpec, KlUcbSpec, MossSpec, UcbVSpec).

## Lifecycle and merging

For per-arm bandits the pattern is: each replica runs the same bandit
spec materialised at its own seed, each replica snapshots periodically
to produce a list of per-arm Results (the same Results the underlying
SeriesStat would produce), and the coordinator's bandit calls merge to
fold the foreign snapshots in.

For Snapshotable bandits whose state is not per-arm (Exp4, the roulette
wheel's segment state), the snapshot type is the wire format and merge
does the principled or best-effort combine documented in the bandit's
KDoc.

The create(random) call returns a fresh bandit with the same
configuration, state reset to the prior seed, and a swappable random.
Use it to spin up clean replicas for parallel training without
reserialising the spec.

## Observability wrappers

Bandits answer "which arm to play" but production deployments want richer
introspection: what policy is the bandit learning, how does reward depend
on context, which arms correlate with which rewards. `TrackedContextualBandit`
and `TrackedUnivariateBandit` wrap any bandit and route every `choose` /
`update` event into a small set of aggregate side stats. Arm-level
bucketing is a planned `stratify` op; encode the arm into the observation
(the joint template does this automatically) until that lands.

`TrackedContextualBandit` has four optional template slots:

- `chooseTemplate: RegressionStat<*>?`. Sees `update(x = context, y =
  armIndex.toDouble(), weight = 1.0)` at every `choose`. Models the
  bandit's **policy**: arm selections as a function of context. The
  template's `featureSize` must equal `contextFeatureSize`.
- `updateJointTemplate: RegressionStat<*>?`. Sees `update(x =
  [armIndex.toDouble()] ++ context, y = reward, weight)` at every
  `update`. **Joint reward model** with the chosen arm prepended as an
  extra feature; the coefficient on the arm dimension gives the
  arm-conditional effect. The template's `featureSize` must equal
  `1 + contextFeatureSize`.
- `updateMarginalTemplate: RegressionStat<*>?`. Sees `update(x = context,
  y = reward, weight)` at every `update`. **Marginal reward-given-context
  model**, agnostic to which arm was played. The template's `featureSize`
  must equal `contextFeatureSize`.
- `updateArmRewardTemplate: PairedStat<*>?`. Sees `update(x =
  armIndex.toDouble(), y = reward, weight)` at every `update`.
  **Arm-versus-reward**: slope, correlation, or covariance between arm
  and observed reward.

`TrackedUnivariateBandit` (no context) has two slots:

- `chooseTemplate: SeriesStat<*>?`. Sees `update(value =
  armIndex.toDouble(), weight = 1.0)` at every `choose`. Arm-pick
  distribution over time; a `CountStat` here gives total pulls, a
  `MomentsStat` gives the empirical arm distribution.
- `updateArmRewardTemplate: PairedStat<*>?`. Sees `update(x =
  armIndex.toDouble(), y = reward, weight)` at every `update`. Per-arm
  reward distribution via the paired modality.

Every slot is independent and may be null to disable.

The wrapper itself only satisfies the action interface (`ContextualBandit`
or `UnivariateBandit`). To reach `PerArmBandit.snapshot()`,
`ContextualScorable.evaluate(i, x)`, or any other interface the inner
bandit implements, dot through `tracked.inner.<method>`. The inner is
typed as the generic parameter `B`, so no casts are needed.

```kotlin
val bandit = TrackedContextualBandit(
    inner = RegressionContextualBandit(nbrArms, BayesianRegressionStat(d), MultivariateGaussian),
    contextFeatureSize = d,
    chooseTemplate = BayesianRegressionStat(featureSize = d),       // policy
    updateJointTemplate = BayesianRegressionStat(featureSize = d + 1), // arm + context
    updateMarginalTemplate = BayesianRegressionStat(featureSize = d),  // marginal
    updateArmRewardTemplate = CovarianceStat(),                        // arm-vs-reward
)
val armIndex = bandit.choose(x)
bandit.update(armIndex, x, reward)
bandit.chooseResult()              // policy regressor snapshot
bandit.updateJointResult()         // joint model snapshot
bandit.updateMarginalResult()      // marginal model snapshot
bandit.updateArmRewardResult()     // arm-vs-reward covariance snapshot
bandit.inner.armResult(armIndex)   // PerArmBandit access via .inner
```

For non-bandit regressors that want marginal-y observability alongside
the model, compose the regressor with a side `SeriesStat` via
`RegressionListStats` and `foldRegression(Y)`. See the "RegressionStat
decorators" section in [04-operations.md](04-operations.md).

## Single source of randomness

Every bandit takes a Random at construction and routes every draw,
sample, and tie-break through it. Pass Random(seed) for full
reproducibility, Random.Default for shared global state, or a custom
implementation such as a thread-local wrapper or a secure-random
adapter. Thread-safety of the Random instance is the caller's
responsibility; the rest is covered by each bandit's concurrency clause.
