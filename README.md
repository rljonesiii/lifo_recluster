# Incremental exact LIFO tracing under reclustering

After any merge, split or regroup of clusters, the engine holds exactly what a fresh LIFO
trace of the new clustering produces, without retracing the network.

Two engines with one interface and identical results:

* `lifo_nb.Engine`: compiled with Numba; state in NumPy arrays. The one to use.
* `lifo.Engine`: pure standard-library Python; the readable reference and the fallback.

`from engine import Engine` picks the compiled engine when NumPy and Numba are installed
and falls back to the reference otherwise. Requirements: `numpy` and `numba` for the
compiled engine, `z3-solver` for `verify/smt_checks.py`, `psutil` for the benchmark's
memory readings. Numba compiles the kernels on first use (about 15 s) and caches them on disk.

| File | What it is |
| --- | --- |
| `lifo.py` | `Ledger`, the reference tracer `fresh_trace`, and the reference `Engine` |
| `lifo_nb.py` | The compiled `Engine` and compiled `fresh_trace`: the same algorithm, kernel by kernel |
| `engine.py` | Picks the compiled engine when available, the reference otherwise |
| `toy.py` | The 15-transaction challenge ledger: baseline, +B–E, +D–F, −B–E, checked against fresh traces |
| `test_engine.py` | Differential tests: random ledgers and operations, each result checked against a fresh trace and a from-scratch rebuild; on small-amount ledgers the fresh trace is also checked against an independent unit-by-unit tracer; input-validation tests. With `--nb`, the compiled engine runs in lockstep with the reference and must match its whole state after every step |
| `mutate.py` | Injects bugs into an engine one at a time (11 into `lifo.py`, or 15 into `lifo_nb.py` with `--nb`) and checks that `test_engine.py` and, with `--exhaustive`, `verify/exhaustive.py` catch each |
| `gen.py` | Synthetic laundering networks: hacks, peel chains, exchanges with thousands of deposit addresses, change addresses, internal sweeps |
| `bench.py` | Scaling benchmark: 14 reclustering operations, each timed against a fresh trace and checked for exact equality; memory; ablations. `--nb` runs the compiled engine against the compiled fresh trace |
| `verify/smt_checks.py` | Z3 proofs: every feasible path of the sweep's event code with symbolic ids, block sizes and amounts (stacks of up to 12 blocks), the induction-step arithmetic for conservation and holdings, and push order; self-test (9 injected bugs) and a fidelity check of the transcription against `lifo.sweep` and both compiled code paths |
| `verify/exhaustive.py` | Bounded exhaustive check of an engine against the unit-level specification: every valid ledger in scope, every clustering, every single-block regroup, merge and split with undo, and every ordered pair of clusterings, under 5 engine settings; full state compared after every operation, lemmas asserted on the specification. `--nb` checks the compiled engine |
| `results/` | Logs and JSON of the test, verification and benchmark runs |

## Reproduce

```
python3 toy.py                              # challenge ledger, all three events
python3 test_engine.py 2000 11              # reference engine, 2,000 random ledgers (also seeds 12, 13)
python3 test_engine.py 2000 11 --nb         # compiled engine in lockstep with the reference
python3 mutate.py --exhaustive              # 11 bugs in lifo.py, each caught by both harnesses
python3 mutate.py --exhaustive --nb         # 15 bugs in lifo_nb.py
python3 verify/smt_checks.py                # Z3 proofs, about 10 s, plus the fidelity check
python3 verify/exhaustive.py                # reference engine, every case in scope, about 50 min on 2 cores
python3 verify/exhaustive.py --nb S1        # compiled engine; S2 in parts: --nb S2 0/2, --nb S2 1/2
python3 bench.py 1000000 --nb --mem --ablate --out results/nb_n1000000.json   # compiled, about 3 min
python3 bench.py 1000000 --mem --ablate --out results/n1000000.json           # reference, about 8 min
```

`NUMBA_BOUNDSCHECK=1` (with a separate `NUMBA_CACHE_DIR`) compiles the kernels with array
bounds checks; `results/tests_nb_boundscheck.log` is the lockstep suite run that way.

## Using the engine

```python
from engine import Ledger, Engine
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
