# Incremental exact LIFO tracing under reclustering

Pure Python 3 standard library (`psutil` only for the benchmark's memory
readings; `z3-solver` only for `verify/smt_checks.py`).  After any merge, split or regroup of clusters, `Engine` holds exactly what a
fresh LIFO trace of the new clustering produces, without retracing the network.

| File | What it is |
| --- | --- |
| `lifo.py` | `Ledger`, the reference tracer `fresh_trace`, and `Engine` |
| `toy.py` | The 15-transaction challenge ledger: baseline, +B–E, +D–F, −B–E, checked against fresh traces |
| `test_engine.py` | Differential tests: random ledgers and operations, each result checked against a fresh trace and a from-scratch rebuild; on small-amount ledgers the fresh trace is also checked against an independent unit-by-unit tracer; input-validation tests |
| `mutate.py` | Injects 11 bugs into `lifo.py` one at a time and checks that `test_engine.py` (and, with `--exhaustive`, `verify/exhaustive.py` on its quick scopes) catches each |
| `gen.py` | Synthetic laundering networks: hacks, peel chains, exchanges with thousands of deposit addresses, change addresses, internal sweeps |
| `bench.py` | Scaling benchmark: 14 reclustering operations, each timed against a fresh trace and checked for exact equality; peak memory; ablations |
| `verify/smt_checks.py` | Z3 proofs: every feasible path of the sweep's event code with symbolic ids, block sizes and amounts (stacks of up to 12 blocks), the induction-step arithmetic for conservation and holdings, and push order; self-test (9 injected bugs) and fidelity check against `lifo.sweep` |
| `verify/exhaustive.py` | Bounded exhaustive check of the shipped engine against the unit-level specification: every valid ledger in scope, every clustering, every single-block regroup, merge and split with undo, and every ordered pair of clusterings, under 5 engine settings; full state compared after every operation, lemmas asserted on the specification |
| `results/` | Logs and JSON of the test and benchmark runs |

## Reproduce

```
python3 toy.py                        # challenge ledger, all three events
python3 test_engine.py 2000 11        # 2,000 random ledgers, ~12k operations (also seeds 12, 13)
python3 mutate.py --exhaustive        # 11 injected bugs, each must be caught by both harnesses
python3 verify/smt_checks.py          # Z3 proofs, about 10 s
python3 verify/exhaustive.py          # every case in scope, about 50 min on 2 cores (--quick: 1 min)
python3 bench.py 10000   --mem --ablate --out results/n10000.json
python3 bench.py 100000  --mem --ablate --out results/n100000.json
python3 bench.py 1000000 --mem --ablate --out results/n1000000.json   # about 8 min on one core
```

## Using the engine

```python
from lifo import Ledger, Engine
L = Ledger(src, dst, amt, sources={STOLEN: 1, CLEAN: 0}, n_wallets=W)
eng = Engine(L, clusters)          # disjoint wallet sets; other wallets are singletons
eng.merge([b, e])                  # merge the clusters containing wallets b and e
eng.split([e])                     # split {e} out of its cluster
eng.regroup([{d, f}, {b}])         # any regrouping in one call; blocks must be disjoint
eng.labels[t]                      # funds moved by tx t: (label, units, label, units, ...)
eng.cluster_of()                   # wallet -> cluster key, and stolen holdings per cluster
eng.reset_stats()                  # work done since the last call
```

Semantics: one LIFO stack per cluster; an external transfer pops the top `amt` units and
pushes them onto the receiver in the same order; an internal transfer changes nothing and
carries the top `amt` units.
