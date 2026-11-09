# Documentation

This folder explains how `lifo_recluster` works, in the order you would want to learn it. Each page is short and links to the exact lines of code it describes.

**The story in five sentences.** Heights Labs traces stolen funds through a transaction network with LIFO accounting, one stack per cluster of wallets, and asks how to update the trace when clusters merge or split without retracing everything ([the challenge](challenge.pdf)). The answer here is to split tracing into two parts: *routing*, which units each transfer carries, and *labels*, whether those units are stolen. A cluster's routing depends only on its own members, so a reclustering re-routes only the clusters it creates. Labels are then pushed forward in time order and stop as soon as a recomputed label matches the old one. The result is always identical to a fresh trace, and local changes on a 1M-transaction network take under half a millisecond instead of a quarter of a second.

## Read in this order

| | Page | What it covers |
| --- | --- | --- |
| 1 | [The problem](01-the-problem.md) | LIFO tracing, clusters, the 15-transaction challenge ledger, and the exact rules |
| 2 | [The idea: routes and labels](02-routes-and-labels.md) | Why a cluster's routing depends only on its members, and how labels flow through it |
| 3 | [The update](03-the-update.md) | `regroup`, step by step: re-route, find what changed, propagate, stop early |
| 4 | [Reading the reference engine](04-reference-engine.md) | A guided tour of [`lifo.py`](../lifo.py), the readable implementation |
| 5 | [The compiled engine](05-compiled-engine.md) | [`lifo_nb.py`](../lifo_nb.py): the same algorithm on NumPy arrays, compiled with Numba |
| 6 | [How we know it is right](06-verification.md) | Differential tests, Z3 proofs, the bounded exhaustive check and mutation tests |
| 7 | [Benchmarks](07-benchmarks.md) | The synthetic networks, the 14-operation script and what the numbers mean |
| 8 | [Running it](08-running.md) | Setting up with uv and reproducing every result |

## The deliverables

- **[The report](report.md)**: the full write-up, from Introduction to Conclusions and Future Work, with references.
- **[Appendix: proofs](proofs.md)**: the nine theorems behind the correctness argument, and the machine checks behind each.
- **[The challenge](challenge.pdf)**: the Heights Labs research challenge this work answers.

## Repository map

```
lifo.py              the reference engine and the reference fresh trace (pure Python)
lifo_nb.py           the compiled engine and compiled fresh trace (NumPy + Numba)
engine.py            picks the compiled engine when it can, the reference otherwise
toy.py               the challenge's 15-transaction ledger through all three events
gen.py               synthetic laundering networks for the scaling test
bench.py             the 14-operation scaling benchmark
experiments.py       micro-benchmarks, the worst case, route stability
test_engine.py       randomized differential tests
mutate.py            mutation tests: plant bugs, make sure the tests catch them
verify/              Z3 proofs, the bounded exhaustive check, an independent toy tracer
results/             the raw logs and JSON of every reported run
docs/                this documentation
pyproject.toml       dependencies, managed with uv (uv.lock pins them)
```

---
Next: [1. The problem →](01-the-problem.md)
