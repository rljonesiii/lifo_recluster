"""Micro-benchmarks behind "Why compile rather than vectorize" in the report: the engine's
hot paths written as plain Python, as a pure functional fold, through NumPy calls, and
compiled with Numba, on the largest cluster of the 1M-transaction benchmark network.

python3 experiments.py [n_tx]      (default 1,000,000; results/experiments.log)

Each row is the best of several runs; every variant's result is checked equal to the
reference's.  The compiled sweep is the shipped kernel lifo_nb._sweep, which, like
lifo.sweep, also builds the cluster's event list and inverts the routes into readers.
"""
import sys
import time
from collections import defaultdict
from itertools import chain

import numpy as np

import lifo
import lifo_nb
from gen import generate
from lifo import Ledger, sweep, _label_from_pieces


def best(f, reps=5):
    ts = []
    for _ in range(reps):
        t0 = time.perf_counter()
        r = f()
        ts.append(time.perf_counter() - t0)
    return min(ts), r


# ----------------------------------------------------- the sweep as a pure fold
def take(stack, a):
    """The top a units of a cons-list block stack (inflow, units, below), as pieces bottom
    first, and the stack that remains.  Nothing is mutated."""
    out = []
    while True:
        s, r, below = stack
        if r > a:
            out.append((s, r - a, a))
            rest = (s, r - a, below)
            break
        out.append((s, 0, r))
        if r == a:
            rest = below
            break
        a -= r
        stack = below
    return tuple(chain.from_iterable(reversed(out))), rest


def sweep_functional(L, members):
    src, dst, amt = L.src, L.dst, L.amt
    events = sorted(set(chain.from_iterable(L.wev[w] for w in members)))
    stack, route = None, {}
    for t in events:                       # a fold of a pure step over the events
        if src[t] not in members:
            stack = (t, amt[t], stack)
        else:
            route[t], rest = take(stack, amt[t])
            if dst[t] not in members:
                stack = rest
    readers = defaultdict(list)
    for t, p in route.items():
        for s in p[0::3]:
            readers[s].append(t)
    return route, dict(readers)


def label_numpy(p, labels):
    """One label recomputation through NumPy calls, on runs (never expanded to units):
    clip each source label's runs to the piece, then merge neighbouring equal labels."""
    labs, lens = [], []
    for i in range(0, len(p), 3):
        runs = np.asarray(labels[p[i]], np.int64).reshape(-1, 2)
        end = np.cumsum(runs[:, 1])
        lo = np.maximum(end - runs[:, 1], p[i + 1])
        hi = np.minimum(end, p[i + 1] + p[i + 2])
        keep = hi > lo
        labs.append(runs[keep, 0])
        lens.append((hi - lo)[keep])
    lab, ln = np.concatenate(labs), np.concatenate(lens)
    first = np.concatenate(([0], np.flatnonzero(np.diff(lab)) + 1))
    return tuple(int(x) for x in np.column_stack((lab[first], np.add.reduceat(ln, first))).ravel())


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    t0 = time.perf_counter()
    (src, dst, amt, sources, W), clusters, meta = generate(n, seed=1)
    L = Ledger(src, dst, amt, sources, W)
    A = lifo_nb.LedgerArrays(L)
    print(f"network: {n:,} transactions, {W:,} wallets, built in {time.perf_counter() - t0:.1f} s")
    M = frozenset(max(clusters, key=lambda c: len(set(chain.from_iterable(L.wev[w] for w in c)))))
    ne = len(set(chain.from_iterable(L.wev[w] for w in M)))
    print(f"largest cluster by events: {len(M):,} wallets, {ne:,} events\n")

    # 1. one cluster's sweep, events and readers included
    ti, (r_i, rd_i, _) = best(lambda: sweep(L, M))
    tf, (r_f, rd_f) = best(lambda: sweep_functional(L, M))
    assert r_f == r_i and rd_f == rd_i
    ma = np.fromiter(M, np.int64, len(M))
    mask = np.zeros(W, np.bool_)
    mark = np.zeros(n, np.bool_)
    slot = np.full(n, -1, np.int64)
    run = lambda: lifo_nb._sweep(ma, mask, A.wptr, A.widx, A.src, A.dst, A.amt, mark, slot)
    t1 = time.perf_counter(); e = run(); first = time.perf_counter() - t1
    tn, e = best(run, 9)
    assert lifo_nb._routes_of(e) == r_i and lifo_nb._readers_of(e) == rd_i
    print("one cluster's sweep (event list, LIFO pass, readers)")
    print(f"  lifo.sweep, plain Python          {ti * 1e3:8.2f} ms")
    print(f"  pure functional fold              {tf * 1e3:8.2f} ms   {tf / ti:.2f}x the plain loop's time")
    print(f"  lifo_nb._sweep, compiled          {tn * 1e3:8.2f} ms   {ti / tn:.0f}x faster than plain Python "
          f"(first call {first * 1e3:.1f} ms, compiled code loaded from the cache); routes and readers identical\n")

    # 2. the event list alone
    wev_np = [np.asarray(x, np.int64) for x in L.wev]
    tp, ev_p = best(lambda: sorted(set(chain.from_iterable(L.wev[w] for w in M))))
    tu, ev_u = best(lambda: np.unique(np.concatenate([wev_np[w] for w in M])))
    big = np.zeros(W, np.bool_); big[ma] = True
    ts, ev_s = best(lambda: np.flatnonzero(big[A.src] | big[A.dst]))
    assert ev_u.tolist() == ev_p and ev_s.tolist() == ev_p
    print("the cluster's event list")
    print(f"  Python set + sort                 {tp * 1e3:8.2f} ms")
    print(f"  NumPy unique(concatenate)         {tu * 1e3:8.2f} ms   {tp / tu:.1f}x faster")
    print(f"  NumPy mask over the whole ledger  {ts * 1e3:8.2f} ms   {tp / ts:.1f}x faster (scans all {n:,})\n")

    # 3. one label recomputation, the unit of work of propagation
    eng = lifo.Engine(L, clusters)
    sample = [t for t in range(L.n) if eng.route[t] is not None][:20000]
    pieces = sum(len(eng.route[t]) // 3 for t in sample) / len(sample)
    labels = eng.labels
    tl, out = best(lambda: [_label_from_pieces(eng.route[t], labels) for t in sample], 3)
    k = 2000
    tl2, out2 = best(lambda: [label_numpy(eng.route[t], labels) for t in sample[:k]], 3)
    assert out2 == out[:k] and out == [labels[t] for t in sample]
    print(f"one label recomputation ({len(sample):,} sending transactions, {pieces:.2f} pieces on average)")
    print(f"  Python tuples                     {tl / len(sample) * 1e6:8.2f} us")
    print(f"  NumPy calls                       {tl2 / k * 1e6:8.2f} us   {tl2 / k / (tl / len(sample)):.0f}x slower\n")

    # 4. per-wallet net stolen flows at build time
    stol = np.asarray(eng.stol, np.int64)
    dep = np.asarray([L.source_label[s] >= 0 for s in src])

    def net_py():
        net = [0] * W
        st = eng.stol
        for t in range(L.n):
            k = st[t]
            if k:
                net[dst[t]] += k
                if L.source_label[src[t]] < 0:
                    net[src[t]] -= k
        return net

    def net_np():
        return np.bincount(A.dst, stol, W) - np.bincount(A.src[~dep], stol[~dep], W)

    ta, a = best(net_py, 3)
    tb, b = best(net_np, 3)
    assert a == [int(x) for x in b] == list(eng.net)
    print("per-wallet net stolen flows over the whole ledger")
    print(f"  Python loop                       {ta * 1e3:8.2f} ms")
    print(f"  NumPy bincount                    {tb * 1e3:8.2f} ms   {ta / tb:.1f}x faster")


if __name__ == "__main__":
    main()
