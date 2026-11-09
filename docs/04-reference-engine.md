# 4. Reading the reference engine

[← 3. The update](03-the-update.md) · [Index](README.md)

[`lifo.py`](../lifo.py) is the whole algorithm in 543 lines of standard-library Python. It is written to be read: the compiled engine on [page 5](05-compiled-engine.md) does the same thing step for step, and the tests compare the two after every operation. This page walks through the file from top to bottom.

## Using it

```python
from lifo import Ledger, Engine, fresh_trace

# Four wallets: 0 = stolen source, 1 = clean source, 2 and 3 ordinary wallets.
L = Ledger(src=[0, 1, 2], dst=[2, 2, 3], amt=[100, 40, 70],
           sources={0: 1, 1: 0}, n_wallets=4)   # label 1 = stolen, 0 = clean
eng = Engine(L)            # every wallet starts as its own cluster
eng.labels[2]              # (1, 30, 0, 40): $30 stolen under $40 clean
eng.merge([2, 3])          # wallets 2 and 3 are one entity
eng.labels[2]              # the same funds, now an internal transfer
eng.reset_stats()          # the work the merge did
```

In practice, import `Engine` from [`engine.py`](../engine.py) instead: it returns the compiled engine when it can and this one otherwise, with the same interface ([page 5](05-compiled-engine.md#choosing-an-engine)).

## The file, top to bottom

| Lines | What is there |
| --- | --- |
| [1–40](../lifo.py#L1-L40) | The model, the representation and the update, in brief. A good first read. |
| [49–64](../lifo.py#L49-L64) | Input checks: [`_integer`](../lifo.py#L49) and [`check_wallets`](../lifo.py#L56) reject anything that is not an integer wallet id. |
| [67–112](../lifo.py#L67-L112) | [`Ledger`](../lifo.py#L67): validates the transactions and builds `wev`, each wallet's list of transactions in time order. |
| [115–145](../lifo.py#L115-L145) | Small helpers: [`stolen_of`](../lifo.py#L115) counts stolen units in a label; [`check_threshold`](../lifo.py#L124) and [`check_partition`](../lifo.py#L131) validate engine settings and wallet sets. |
| [149–210](../lifo.py#L149-L210) | [`fresh_trace`](../lifo.py#L149): the from-scratch trace every result is compared with. |
| [214–274](../lifo.py#L214-L274) | [`sweep`](../lifo.py#L214): one cluster's routes and readers ([page 2](02-routes-and-labels.md#how-a-stack-is-stored-blocks-by-inflow)). |
| [277–306](../lifo.py#L277-L306) | [`_label_from_pieces`](../lifo.py#L277): a label read off a route. |
| [310–315](../lifo.py#L310-L315) | [`Cluster`](../lifo.py#L310): a member set and its stolen holdings. |
| [318–364](../lifo.py#L318-L364) | [`Engine.__init__`](../lifo.py#L319): the full build. |
| [376–446](../lifo.py#L376-L446) | [`merge`](../lifo.py#L376), [`split`](../lifo.py#L387) and [`regroup`](../lifo.py#L396) ([page 3](03-the-update.md)). |
| [449–463](../lifo.py#L449-L463) | The routing cache: [`_cache_get`](../lifo.py#L449) and [`_cache_put`](../lifo.py#L455). |
| [466–526](../lifo.py#L466-L526) | [`_propagate`](../lifo.py#L466): labels in time order, the cutoff and the linear pass. |
| [529–543](../lifo.py#L529-L543) | Queries: [`clusters`](../lifo.py#L529), [`cluster_of`](../lifo.py#L532) and [`check_against_fresh`](../lifo.py#L539). |

## The ledger

A [`Ledger`](../lifo.py#L67) holds three parallel lists, `src`, `dst` and `amt`, one entry per transaction in time order, plus `source_label[w]`: the label a source wallet emits (0 clean, 1 or more stolen), or −1 for an ordinary wallet. The constructor rejects self-transfers, non-positive amounts, payments into a source and any payment that would overdraw a wallet. If any check fails, the constructor raises and no ledger is created. If no wallet can overdraw, no cluster can either, under any clustering ([Theorem 1](proofs.md#the-formulation)).

`L.wev[w]` lists the transactions that touch wallet `w`. A cluster's events are the merged lists of its members, which is all [`sweep`](../lifo.py#L214) needs to read.

## The engine's state

| Field | Type | Meaning |
| --- | --- | --- |
| `route[t]` | tuple of `(inflow, offset, length)` triples, flattened | The pieces transaction `t` carries; `None` for a deposit |
| `readers[s]` | list of transactions | Who has a piece in inflow `s`, in time order |
| `labels[t]` | tuple `(label, units, label, units, …)` | What `t` carries, bottom to top, equal neighbours merged |
| `stol[t]` | int | Stolen units in `labels[t]` |
| `net[w]` | int | Stolen units wallet `w` received minus sent |
| `owner[w]` | `Cluster` | The cluster containing `w`; `None` for a source |
| `cache` | `OrderedDict` | Member set → (routes, readers), least recently used first |
| `stats` | dict | Work counters since the last [`reset_stats`](../lifo.py#L371) |

The work counters are what the benchmarks report:

- `swept`: events re-swept;
- `route_changed`: routes that differ after the change (the seeds);
- `relabeled`: labels recomputed;
- `label_changed`: labels that actually changed;
- `cache_hits`: new clusters whose routing came from the cache;
- `linear_pass`: how often propagation switched to the linear pass.

`relabeled − label_changed` is the cutoff at work: labels that were checked, found unchanged, and stopped the wave.

## Settings

[`Engine(L, clusters, sweep_after, cache_budget, cutoff)`](../lifo.py#L318):

- `clusters`: disjoint wallet sets. Every wallet not listed starts on its own.
- `sweep_after`: how many recomputations before the linear pass takes over (default n/4).
- `cache_budget`: routes kept in the cache (default n; 0 turns the cache off).
- `cutoff`: `False` turns off the equality cutoff. This exists only to measure what the cutoff buys.

Any setting gives the same results; only the work changes. The tests rotate through all of them.

---
[← 3. The update](03-the-update.md) · Next: [5. The compiled engine →](05-compiled-engine.md)
