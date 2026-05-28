# Package com.eignex.kumulant.bandit.univariate

Indexless multi-armed bandits. Each round, the bandit picks one of K
arms via `choose()`, the caller observes a reward, and `update(arm,
value, weight)` folds it back into that arm's accumulator. No per-round
feature vector; for that, see [com.eignex.kumulant.bandit.contextual].

## Bandit shells

[MultiArmedBandit] is the workhorse. It carries a [BanditPolicy] and a
list of [Arm]s; choose delegates to the policy's scoring rule. Most
named bandits in the literature (UCB1, Thompson, epsilon-greedy, etc.)
are just a policy swap on this shell.

| Bandit | Selection rule |
|--------|----------------|
| [MultiArmedBandit] | Argmax over per-arm policy scores (or whatever the policy does; joint sampling, etc.). |
| [BoltzmannBandit] | Softmax over per-arm means with a cooling temperature schedule. |
| [Exp3Bandit] | Adversarial bandit with exponential-weights updates and a regret bound under non-stationary reward distributions. |
| [RouletteWheelBandit] | Operator-selection roulette where arm probability is proportional to score. Used in meta-heuristics where each "arm" is a neighbourhood move and the score is the recent improvement rate. |
| [TopTwoThompsonBandit] | Top-two Thompson sampling: draw two samples, play the second-best with probability `1 - beta`. Identifies the best arm faster than vanilla Thompson when many arms are competitive. |

## Policies

[BanditPolicy] is the scoring strategy plugged into [MultiArmedBandit].
The library ships every commonly-cited one:

| Policy | Family |
|--------|--------|
| [Greedy] | Pure exploitation; argmax of point estimates. The baseline. |
| [EpsilonGreedy], [EpsilonDecreasing] | Mix of exploitation and uniform-random exploration; epsilon either fixed or annealed. |
| [UniformSelection] | Pure exploration, used as a baseline. |
| [UCB1], [UCB1Normal], [UCB1Tuned] | Upper-confidence-bound family; variants differ in confidence-interval shape. |
| [KlUcb] | KL-UCB; tighter bound than UCB1 for Bernoulli arms. |
| [Moss] | MOSS bound; near-optimal regret across stationary settings. |
| [UcbV] | UCB-V; UCB with variance-aware confidence width. |
| [ThompsonSampling] | Posterior sampling: draw from each arm's posterior, play argmax. |

Each policy reads its per-arm state through a [Posterior] adapter that
projects the arm's [com.eignex.kumulant.core.Result] to whatever the
scoring rule needs (a mean, a Beta posterior, a normal-gamma posterior,
etc.). The [GammaScalePosterior] is the canonical example used by
[BoltzmannBandit] for variance-scaled softmax temperatures.

## Arms

[Arm] is the per-arm state contract. Each arm pairs a stat with the
[com.eignex.kumulant.core.Concurrency] and reset story it needs:

| Arm | Backing stat | Suits |
|-----|--------------|-------|
| [BernoulliArm] | [com.eignex.kumulant.stat.summary.BernoulliSumStat] + count | Binary rewards (click / no click, pass / fail). |
| [MeanArm] | [com.eignex.kumulant.stat.summary.MeanStat] | Continuous rewards where mean suffices. |
| [NormalArm] | [com.eignex.kumulant.stat.summary.VarianceStat] | Continuous rewards with normally-distributed noise; carries enough state for UCB-V and Thompson with normal-gamma. |
| [LogNormalArm] | Welford over `log(value)` | Multiplicative rewards (revenue, latency). |
| [MomentsArm] | [com.eignex.kumulant.stat.summary.MomentsStat] | Higher-order shape matters (skewness / kurtosis aware scoring). |

[CompositeArm] (and [CompositeSubArm]) model multi-component rewards;
e.g. zero-inflated lognormal revenue; without writing a per-shape arm
class. Routing and score combination travel as
[com.eignex.kumulant.schema.ScalarExpr] expressions, so the whole
composite round-trips on the wire alongside the rest of the bandit
config.

## Wire portability

[UnivariateBanditSpec] is the sealed root of wire-portable bandit
configs:

- [MultiArmedSpec]; bandit + policy + arm list.
- [RouletteWheelSpec]; roulette-wheel variant.
- Other family-specific specs co-located here.

Configurations and policies round-trip through skema-based JSON / CBOR
just like the [com.eignex.kumulant.schema.StatSpec] family. The
materializer in [com.eignex.kumulant.bandit] takes a spec and a
`Random` and returns the live bandit; pass the same seed across replicas
for reproducible exploration.

## Interface hierarchy

See [com.eignex.kumulant.bandit] for the action/state interface split;
which bandits expose [com.eignex.kumulant.bandit.Scorable], which
implement [com.eignex.kumulant.bandit.PerArmBandit], and where joint-
sampling bandits diverge from the per-arm-score path.
