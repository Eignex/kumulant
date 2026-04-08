# katom

TODO:

make constructors that accept derived stuff so we can minimize state in stats (added vals for copy)

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
