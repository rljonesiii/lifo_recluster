# 5. The compiled engine

[← 4. Reading the reference engine](04-reference-engine.md) · [Index](README.md)

[`lifo_nb.py`](../lifo_nb.py) is the engine to use. It runs the same algorithm as [`lifo.py`](../lifo.py), kernel for kernel, but keeps its state in NumPy arrays and compiles every loop to machine code with Numba. Its results are identical to the reference, and the tests check that after every operation ([page 6](06-verification.md)).

## Why compile instead of vectorize

The loops that do the work are sequential. A sweep reads the stack the previous event left behind. A label is built from the labels of earlier transactions. Propagation pops from a heap whose next entry depends on which labels just changed. NumPy speeds up whole-array operations, not loops like these, and on the engine's small objects it is slower: one label recomputation (about 1.4 pieces) costs 0.89 µs with Python tuples and 21 µs through NumPy calls. Numba instead compiles ordinary loops over arrays. On the busiest cluster of the 1M network (69,805 events over 11,498 addresses), the compiled sweep runs 14× faster than the Python one, with identical results. [`experiments.py`](../experiments.py) measures all of this; its output is [`results/experiments.log`](../results/experiments.log).

## How the state is stored

Compiled code works on flat integer arrays, so every per-transaction structure becomes rows in an **arena**: one big array, with a start and a length per transaction pointing at that transaction's rows ([the module docstring](../lifo_nb.py#L9-L17)).

| Structure | Reference engine | Compiled engine |
| --- | --- | --- |
| Routes | `route[t]`, a tuple of pieces | arena `P`, rows `(inflow, offset, length)`; `t`'s rows are `P[rs[t] : rs[t] + rl[t]]` |
| Labels | `labels[t]`, a tuple of runs | arena `LB`, rows `(label, units)`; located by `ls[t]`, `ll[t]` |
| Readers | `readers[s]`, a list | arena `RD`; located by `ds[s]`, `dl[s]` |
| Stolen counts, net flows | lists of ints | int64 arrays `stol`, `net` |
| Clusters | `Cluster` objects | `owner[w]`, a cluster id per wallet, and a `hold` array per id; member sets stay in Python |
| Routing cache | member set → routes and readers | the same, held as arrays |

The arenas are **append-only**. An update writes new rows at the end and repoints the transaction; the old rows become dead. When dead rows outnumber live ones by more than 4,096, [`_compact_if_needed`](../lifo_nb.py#L1016) copies the live rows into a fresh arena ([`_compact2`](../lifo_nb.py#L627), [`_compact1`](../lifo_nb.py#L644)).

## The kernels

Each compiled kernel mirrors a piece of the reference:

| Kernel | Mirrors | What it does |
| --- | --- | --- |
| [`_events`](../lifo_nb.py#L85) | the event merge in `lifo.sweep` | A cluster's events in time order; a large cluster marks its events and scans once instead of sorting |
| [`_sweep`](../lifo_nb.py#L155) | [`lifo.sweep`](../lifo.py#L214) | One cluster's routes, with exactly the reference's five cases |
| [`_invert`](../lifo_nb.py#L124) | the readers part of `lifo.sweep` | Readers from routes, by counting rather than sorting |
| [`_build`](../lifo_nb.py#L275) | [`lifo.Engine.__init__`](../lifo.py#L319) | Every cluster's routes in one pass over the ledger, then readers, then labels in time order |
| [`_label_runs`](../lifo_nb.py#L229) | [`_label_from_pieces`](../lifo.py#L277) | A label read off a route |
| [`_snapshot`](../lifo_nb.py#L394) | step 2 of `regroup` | An outgoing cluster's routes and readers, for the cache |
| [`_install`](../lifo_nb.py#L430) | step 3 of `regroup` | Write a new cluster's routes; return the ones that changed |
| [`_propagate`](../lifo_nb.py#L542) | [`Engine._propagate`](../lifo.py#L466) | The heap, the cutoff and the linear pass |
| [`_fresh`](../lifo_nb.py#L661) | [`lifo.fresh_trace`](../lifo.py#L149) | The compiled fresh trace the benchmark compares against |

Smaller helpers grow arrays, run the heap ([`_hpush`](../lifo_nb.py#L509), [`_hpop`](../lifo_nb.py#L522)), and drop and install readers ([`_drop_readers`](../lifo_nb.py#L479), [`_put_readers`](../lifo_nb.py#L488)).

## What stays in Python

The [`Engine`](../lifo_nb.py#L827) class keeps only the bookkeeping: validating input, working out which clusters an operation replaces, the member sets, the routing cache, and calling the kernels. Its [`regroup`](../lifo_nb.py#L922) takes the same steps as the reference's, in the same order, down to the cache's LRU order, so the two engines' states can be compared field by field.

For the tests it also offers [`export`](../lifo_nb.py#L1107), the whole state in the reference's Python form, and [`check_invariants`](../lifo_nb.py#L1064), which checks the state that `export` does not show: live-row counters, arena bounds, scratch arrays left clean, cluster ids.

One small difference: the compiled engine's [`clusters()`](../lifo_nb.py#L1042) returns snapshots, so a `ClusterView`'s holdings do not follow later updates. Call `clusters()` again after each update.

## Choosing an engine

[`engine.py`](../engine.py) picks for you. `Engine(L, ...)` returns the compiled engine when NumPy and Numba are installed and the ledger fits in 64-bit integers, and the reference engine otherwise; both behave the same.

The 64-bit limit comes from [`fits`](../lifo_nb.py#L40). Every number the kernels form (a block, a run, a stack height, a net flow, a holding) is at most twice the ledger's total volume, so a total volume below 2⁶² is always safe. A ledger kept in wei passes that bound at about 4.6 ether of total volume, and then runs on the reference engine, which uses Python's unbounded integers.

Numba compiles the kernels the first time they run (about 20 seconds, [`compile.log`](../results/compile.log)) and caches the machine code on disk, so later runs start in under a second.

---
[← 4. Reading the reference engine](04-reference-engine.md) · Next: [6. How we know it is right →](06-verification.md)
