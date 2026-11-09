# Incremental exact LIFO tracing under reclustering

After any merge, split or regroup of clusters, the engine holds exactly what a fresh LIFO
trace of the new clustering produces. It re-processes only the clusters a change creates and
the labels that change; a change that reaches a quarter of the network finishes with one
linear pass instead.

**Start here:** [the documentation](docs/README.md) walks through the code page by page;
[the report](docs/report.md) is the full write-up, with its [proofs appendix](docs/proofs.md);
[the challenge](docs/challenge.pdf) is the Heights Labs research challenge this answers.

Two engines with one interface and identical results:

* `lifo_nb.Engine`: compiled with Numba; state in NumPy arrays. The one to use.
* `lifo.Engine`: pure standard-library Python; the readable reference and the fallback.

`from engine import Engine` picks the compiled engine when NumPy and Numba are installed
and the ledger fits its int64 arithmetic (total volume below 2**62, labels below 2**63),
and the reference otherwise.

## Setup

Dependencies are managed with [uv](https://docs.astral.sh/uv/): `pyproject.toml` lists them,
`uv.lock` pins them, and `.python-version` selects Python 3.12. From the repository root:

```
uv sync                       # numpy, numba, z3-solver, psutil into .venv/
uv run python toy.py          # the challenge ledger, all three events
```

`numpy` and `numba` run the compiled engine, `z3-solver` the proofs in `verify/smt_checks.py`,
and `psutil` the benchmark's memory readings; `uv sync --group docs` adds `matplotlib` for
`docs/figures/make_figures.py`. Numba compiles the kernels on first use (about 20 s) and caches them on disk; later runs
load them in well under a second.

| File | What it is |
| --- | --- |
| `lifo.py` | `Ledger`, the reference tracer `fresh_trace`, and the reference `Engine` |
| `lifo_nb.py` | The compiled `Engine` and compiled `fresh_trace`: the same algorithm, kernel by kernel |
| `engine.py` | Picks the compiled engine when available, the reference otherwise |
| `toy.py` | The 15-transaction challenge ledger: baseline, +B–E, +D–F, −B–E, checked against fresh traces |
| `test_engine.py` | Differential tests: random ledgers and operations, each result checked against a fresh trace and a from-scratch rebuild; on small-amount ledgers the fresh trace is also checked against an independent unit-by-unit tracer. With `--nb`, the compiled engine runs in lockstep with the reference and must match its whole state after every step, and its hidden state must stay consistent. Then targeted tests (amounts near and past the int64 bound, thresholds, holdings-array growth, negative tests of the self-checks) and input validation |
| `mutate.py` | Injects bugs into an engine one at a time (12 into `lifo.py`, or 22 into `lifo_nb.py` with `--nb`) and checks that `test_engine.py` catches each and, with `--exhaustive`, that `verify/exhaustive.py` catches each its scope can reach (bugs that weaken a self-check are left to the negative tests) |
| `experiments.py` | The micro-benchmarks behind "compile rather than vectorize" (the hot paths in plain Python and through NumPy calls, and the sweep also as a functional fold and compiled), the worst case of an update against a fresh trace, and the route-stability check behind the future-work item |
| `gen.py` | Synthetic laundering networks: hacks, peel chains, exchanges with thousands of deposit addresses, change addresses, internal sweeps |
| `bench.py` | Scaling benchmark: 14 reclustering operations, each timed against a fresh trace and checked for exact equality; memory; ablations. `--nb` runs the compiled engine against the compiled fresh trace |
| `verify/smt_checks.py` | Z3 proofs: every feasible path of the sweep's event code with symbolic ids, block sizes and amounts (stacks of up to 12 blocks) and push order; self-test (9 injected bugs) and a fidelity check of the transcription against `lifo.sweep` and both compiled code paths |
| `verify/exhaustive.py` | Bounded exhaustive check of an engine against the unit-level specification: every valid ledger in scope (amounts 1–2, one stolen and one clean source), every clustering, every single-block regroup with undo and every ordered pair of clusterings under 5 engine settings, and every merge and split with undo under the defaults; full state compared after every operation, lemmas asserted on the specification. `--nb` checks the compiled engine |
| `verify/toy_independent.py` | The challenge ledger traced by a separate implementation written from the rules alone, then compared with the package |
| `results/` | Raw logs and JSON of every run reported; `results/README.md` lists the command behind each |
| `docs/` | The documentation, the report, its proofs appendix and the challenge |
| `spark/` | A sketch of the batch build for a full ledger on PySpark and Kubernetes (Kubeflow Spark Operator, Delta Lake on S3-compatible storage), checked in local mode against `lifo.sweep` and `fresh_trace`; `spark/README.md` explains it |
| `pyproject.toml`, `uv.lock` | Dependencies, managed with uv |

## Reproduce

```
uv run python toy.py                              # challenge ledger, all three events
uv run python verify/toy_independent.py           # the same ledger, independent implementation
uv run python test_engine.py 2000 11              # reference engine, 2,000 random ledgers (also seeds 12, 13)
uv run python test_engine.py 2000 11 --nb         # compiled engine in lockstep with the reference
uv run python mutate.py --exhaustive              # 12 bugs in lifo.py; see results/mutation.log
uv run python mutate.py --exhaustive --nb         # 22 bugs in lifo_nb.py
uv run python verify/smt_checks.py                # Z3 proofs, about 10 s, plus the fidelity check
uv run python verify/exhaustive.py                # reference engine, every case in scope, about 65 min on 2 cores
uv run python verify/exhaustive.py --nb S1        # compiled engine; S2 in parts: --nb S2 0/4 ... --nb S2 3/4
uv run python bench.py 1000000 --nb --repeat 5 --mem --ablate --out results/nb_n1000000.json   # compiled
uv run python bench.py 1000000 --repeat 5 --mem --ablate --out results/n1000000.json           # reference
uv run python experiments.py                      # compile vs vectorize, worst case, route stability
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

Inputs: wallet ids, amounts and labels must be integers (NumPy integers are accepted and
stored as Python ints); anything else, an unknown wallet, an overlap, a source in a
cluster or an overdraft raises `ValueError` before anything changes. `eng.clusters()`
returns snapshots: a `ClusterView`'s holdings do not follow later updates. The compiled
engine computes in int64 and refuses a ledger whose total volume reaches 2**62 (for
example, large wei-denominated ledgers); `engine.Engine` then returns the reference engine,
which uses Python's unbounded integers.
