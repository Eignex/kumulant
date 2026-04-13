# kumulant

TODO:

Find a way to unify DecayingStats and EwmaStats
Make Covariance DerivedStats
Implement Ridge/Lasso 
Implement DefaultStreamMode in a way that does not require expected
Make a simplified grouping: ListStats

histogram algos
- [X] ddsketch
- [X] frugal streaming
- [ ] T digest
- [X] HDR
- [ ] reservoir
- [ ] linear

cardinality
- hyperloglog
- linear counting (bitset)

pgbm stuff
- vector of variance
- gradient sum (sumGrad+sumHess). this is probably just a paired sum
- crps (continuous ranked probability score)
