"""Experiments behind the report, on the 1M-transaction benchmark network.

python3 experiments.py [n_tx]      (default 1,000,000; results/experiments.log)

1-4. "Why compile rather than vectorize": the engine's hot paths written as plain Python, as a
     pure functional fold, through NumPy calls, and compiled with Numba, on the network's
     largest cluster.  Each row is the best of several runs, and every variant's result is
     checked equal to the reference's.  The compiled sweep is the shipped kernel
     lifo_nb._sweep, which, like lifo.sweep, also builds the event list and the readers.
5.   The worst case: a ledger whose routes hold K*K pieces while its labels hold O(K) runs,
     so an update costs far more than a fresh trace (both engines, results checked exact).
6.   Route stability (future work): which sends a merge or split can re-route, by a rule
     that needs no sweep, against the routes the engine actually changes on the benchmark's
     operations.
"""
import gc
import sys
import time
from collections import defaultdict
from itertools import chain

import numpy as np

import lifo
import lifo_nb
from bench import pick_ops
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


    # 5. the worst case: routes far larger than labels
    print("\nworst case: K one-unit deposits into A, then K transfers of the whole balance between A and B,")
    print("then A and B merged (each transfer becomes an internal read of all K deposits)")
    for K in (300, 1000):
        worst_case(K)

    # 6. route stability: which routes can a merge or split change?
    route_stability(L, eng, pick_ops(L, eng, meta, 1))


def worst_case(K):
    L = Ledger([0] * K + [1 + j % 2 for j in range(K)], [1] * K + [2 - j % 2 for j in range(K)],
               [1] * K + [K] * K, sources={0: 0}, n_wallets=3)

    def reference():
        e = lifo.Engine(L)
        gc.collect()
        t0 = time.perf_counter(); e.merge([1, 2]); t1 = time.perf_counter()
        co = e.cluster_of()[0]
        gc.collect()
        t2 = time.perf_counter(); lifo.fresh_trace(L, co); t3 = time.perf_counter()
        return t1 - t0, t3 - t2, e

    def compiled():
        e = lifo_nb.Engine(L)
        gc.collect()
        t0 = time.perf_counter(); e.merge([1, 2]); t1 = time.perf_counter()
        gc.collect()
        t2 = time.perf_counter(); lifo_nb.fresh_trace_arrays(e.A, e.owner, e.hold.shape[0]); t3 = time.perf_counter()
        return t1 - t0, t3 - t2, e

    for name, run in (("reference", reference), ("compiled", compiled)):
        runs = [run() for _ in range(3)]
        upd, fr = min(r[0] for r in runs), min(r[1] for r in runs)
        e = runs[-1][2]
        assert e.check_against_fresh()
        if name == "reference":
            pieces = sum(len(p) // 3 for p in e.route if p)
            label_runs = sum(len(x) // 2 for x in e.labels if x)
            print(f"  K = {K:,}: {pieces:,} route pieces, {label_runs:,} label runs over {L.n:,} transactions")
        print(f"    {name:<9} update {upd * 1e3:9.2f} ms   fresh trace {fr * 1e3:7.3f} ms   "
              f"{upd / fr:6.0f}x the fresh trace; exact")


def route_stability(L, eng, ops):
    """A send of part X of a union can route differently in the union than in X alone only
    if it crosses parts, or if it reads or pops below X's lowest balance since the last event
    of any other part: in between, the union's balance is X's plus a constant, so X's sends
    above that low take the same units.  The rule needs no sweep; check it against what the
    engine re-sweeps and changes on each of the benchmark's operations."""
    print("\nroute stability: sends a merge or split can re-route, by a rule that needs no sweep")
    print("  (d: events of the parts that join or leave the largest part)")
    print(f"  {'operation':<42}{'re-swept':>10}{'d':>8}{'flagged':>10}{'changed':>10}  all changes flagged")
    for name, kind, ws in ops:
        if kind == "merge":
            parts = list({id(eng.owner[w]): eng.owner[w].members for w in ws}.values())
        else:
            whole = eng.owner[ws[0]].members
            parts = [frozenset(ws), whole - frozenset(ws)]
        events, flagged = flag(L, parts)
        sizes = [len({t for w in P for t in L.wev[w]}) for P in parts]
        d = sum(sizes) - max(sizes)  # events of the parts that join or leave the largest one
        sends = [t for t in events if L.source_label[L.src[t]] < 0]
        before = {t: eng.route[t] for t in sends}
        eng.reset_stats()
        (eng.merge if kind == "merge" else eng.split)(ws)
        swept = eng.reset_stats()["swept"]
        changed = {t for t in sends if eng.route[t] != before[t]}
        assert changed <= flagged, name
        print(f"  {name:<42}{swept:>10,}{d:>8,}{len(flagged):>10,}{len(changed):>10,}  yes")
    assert eng.check_against_fresh()


def flag(L, parts):
    """The union's events, and the sends the rule flags."""
    part = {w: i for i, P in enumerate(parts) for w in P}
    events = sorted({t for P in parts for w in P for t in L.wev[w]})
    height = [0] * len(parts)
    low = [None] * len(parts)  # lowest balance since another part's last event (None: no such event yet)
    flagged = set()
    for t in events:
        ps, pd, a = part.get(L.src[t]), part.get(L.dst[t]), L.amt[t]
        if ps is not None and (pd not in (None, ps) or low[ps] is not None and height[ps] - a < low[ps]):
            flagged.add(t)
        if ps != pd:
            if ps is not None:
                height[ps] -= a
            if pd is not None:
                height[pd] += a
        acting = {ps, pd} - {None}
        low = [height[i] if acting - {i} else low[i] if low[i] is None else min(low[i], height[i])
               for i in range(len(parts))]
    return events, flagged


if __name__ == "__main__":
    main()
