"""Differential tests for lifo.Engine and the compiled lifo_nb.Engine.

After every merge, split or regroup on random ledgers and clusterings:
  * labels and holdings equal a fresh trace (lifo.fresh_trace);
  * on small-amount ledgers, fresh_trace itself equals an independent unit-by-unit tracer;
  * routing, readers and per-wallet net flows equal an engine rebuilt from scratch;
  * readers are the exact inverse of routing.
Engine settings rotate through: defaults, an aggressive linear-pass threshold, a tiny or
zero routing cache, and no equality cutoff.  Invalid input must be rejected atomically.
With --nb the compiled engine is tested, and the reference lifo.Engine also runs every
operation in lockstep: the two must agree on the whole state after every step (routes,
readers, labels, stolen counts, net flows, partition, holdings, owners, the routing
cache in LRU order) and on the work counters.

Usage: python3 test_engine.py [n_cases] [seed] [--nb]
"""
import random
import sys

import lifo
from lifo import Ledger, fresh_trace

Engine = lifo.Engine


# ------------------------------------------------------------------- generators
def random_ledger(rng, W, n, n_labels=3, max_amt=9):
    """Wallets 0..n_labels-1 are sources (wallet i emits label i); balances stay >= 0."""
    sources = {w: w for w in range(n_labels)}
    first = n_labels
    Wtot = first + W
    bal = [0] * Wtot
    src, dst, amt = [], [], []
    while len(src) < n:
        funded = [w for w in range(first, Wtot) if bal[w] > 0]
        if not funded or rng.random() < 0.25:
            s = rng.randrange(n_labels)
            d = rng.randrange(first, Wtot)
            a = rng.randint(1, max_amt)
        else:
            s = rng.choice(funded)
            d = rng.randrange(first, Wtot)
            if d == s:
                continue
            a = bal[s] if rng.random() < 0.3 else rng.randint(1, bal[s])
            bal[s] -= a
        bal[d] += a
        src.append(s); dst.append(d); amt.append(a)
    return Ledger(src, dst, amt, sources, Wtot)


def random_partition(rng, wallets, p_join=0.4):
    blocks = []
    for w in wallets:
        if blocks and rng.random() < p_join:
            rng.choice(blocks).append(w)
        else:
            blocks.append([w])
    return blocks


# ------------------------------------------------ independent unit-by-unit tracer
def unit_trace(L, cluster_of):
    """Every unit is a list element.  Slow, obviously correct, independent code."""
    stacks = {}
    out = []
    for t in range(L.n):
        s, d, a = L.src[t], L.dst[t], L.amt[t]
        lab = L.source_label[s]
        if lab >= 0:
            moved = [lab] * a
            stacks.setdefault(cluster_of[d], []).extend(moved)
        else:
            st = stacks[cluster_of[s]]
            moved = st[len(st) - a:]
            if cluster_of[s] != cluster_of[d]:
                del st[len(st) - a:]
                stacks.setdefault(cluster_of[d], []).extend(moved)
        runs = []
        for x in moved:
            if runs and runs[-2] == x:
                runs[-1] += 1
            else:
                runs += [x, 1]
        out.append(tuple(runs))
    return out, {c: sum(1 for x in st if x) for c, st in stacks.items()}


# ------------------------------------------------------------------------ checks
def nz(d):
    return {k: v for k, v in d.items() if v}


def check(eng, L, where, units=False):
    co, hold = eng.cluster_of()
    out, fhold = fresh_trace(L, co)
    assert out == eng.labels, f"labels differ from fresh trace {where}"
    assert nz(hold) == nz(fhold), f"holdings differ from fresh trace {where}"
    if Engine is not lifo.Engine:  # the compiled fresh trace too
        import lifo_nb
        nout, nhold = lifo_nb.fresh_trace(L, co)
        assert nout == out and nz(nhold) == nz(fhold), f"compiled fresh trace differs {where}"
    if units:
        uout, uhold = unit_trace(L, co)
        assert uout == out and nz(uhold) == nz(fhold), f"fresh_trace differs from unit tracer {where}"
    ref = Engine(L, [c.members for c in eng.clusters()])
    assert ref.route == eng.route, f"routing differs from rebuild {where}"
    assert list(ref.net) == list(eng.net), f"net flows differ from rebuild {where}"
    norm = lambda rd: {k: sorted(v) for k, v in rd.items() if v}
    assert norm(ref.readers) == norm(eng.readers), f"readers differ from rebuild {where}"
    inv = {}
    for t, p in enumerate(eng.route):
        if p is not None:
            for i in range(0, len(p), 3):
                inv.setdefault(p[i], []).append(t)
    assert norm(eng.readers) == inv, f"readers are not the inverse of routing {where}"


MODES = [dict(), dict(sweep_after=2), dict(cache_budget=5), dict(cache_budget=0),
         dict(cutoff=False)]


COMPACTIONS = [0]


def lockstep(eng, twin, where):
    if twin is None:
        return
    from lifo_nb import state_of
    a, b = state_of(eng), state_of(twin)
    for k in a:
        assert a[k] == b[k], f"compiled engine differs from lifo.Engine in {k} {where}"
    s = dict(eng.stats)
    COMPACTIONS[0] += s.pop("compactions")
    eng.stats["compactions"] = 0
    assert s == twin.stats, f"work counters differ {where}"


def run(n_cases=1000, seed=1, compiled=False):
    """Returns (operations, operations on ledgers also checked by the unit tracer)."""
    global Engine
    if compiled:
        import lifo_nb
        Engine = lifo_nb.Engine
    rng = random.Random(seed)
    ops = uops = 0
    for case in range(n_cases):
        big = case % 10 == 0
        W = rng.randint(20, 60) if big else rng.randint(2, 12)
        n = rng.randint(800, 2500) if big else rng.randint(5, 160)
        max_amt = 5 if big else rng.choice([3, 9, 50])
        L = random_ledger(rng, W, n, n_labels=rng.choice([2, 3]), max_amt=max_amt)
        units = max_amt <= 9 and not big
        wallets = L.wallets()
        init = random_partition(rng, wallets, p_join=rng.choice([0.0, 0.3, 0.6]))
        eng = Engine(L, init, **MODES[case % len(MODES)])
        twin = lifo.Engine(L, init, **MODES[case % len(MODES)]) if compiled else None
        if compiled and case % 2:
            eng.compact_slack = 0  # compact the arenas whenever dead rows exceed live ones
        check(eng, L, f"case {case} init", units)
        lockstep(eng, twin, f"case {case} init")
        for step in range(rng.randint(1, 12)):
            cl = eng.clusters()
            r = rng.random()
            if r < 0.35 and len(cl) >= 2:
                pick = rng.sample(cl, 3 if len(cl) >= 3 and rng.random() < 0.3 else 2)
                ws = [min(c.members) for c in pick]
                eng.merge(ws)
                twin and twin.merge(ws)
                what = ("merge", ws)
            elif r < 0.7:
                multi = [c for c in cl if len(c.members) >= 2]
                if not multi:
                    continue
                mem = sorted(rng.choice(multi).members)
                part = rng.sample(mem, rng.randint(1, len(mem) - 1))
                eng.split(part)
                twin and twin.split(part)
                what = ("split", part)
            else:
                k = rng.randint(1, 3)
                pool = rng.sample(wallets, min(len(wallets), rng.randint(2, 8)))
                blocks = [pool[i::k] for i in range(k) if pool[i::k]]
                eng.regroup(blocks)
                twin and twin.regroup(blocks)
                what = ("regroup", blocks)
            ops += 1
            uops += units
            check(eng, L, f"case {case} step {step} {what}", units)
            lockstep(eng, twin, f"case {case} step {step} {what}")
    return ops, uops


def validation():
    """Invalid input is rejected before anything changes."""
    rng = random.Random(5)
    L = random_ledger(rng, 6, 80, n_labels=2, max_amt=9)
    for bad in ([[2, 3], [3, 4]], [[0, 3]], [[2, 99]]):
        try:
            Engine(L, bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"accepted clusters {bad}")
    eng = Engine(L, [[2, 3, 4]])
    before = list(eng.labels)
    for bad in ([[2, 3], [3, 4]], [[2], [0, 3]]):
        try:
            eng.regroup(bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"accepted blocks {bad}")
    for bad in ([2, 5], [0]):
        try:
            eng.split(bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"split accepted {bad}")
    eng.split([])
    eng.merge([2])
    assert eng.labels == before
    assert sorted(sorted(c.members) for c in eng.clusters() if len(c.members) > 1) == [[2, 3, 4]]
    try:
        Ledger([2], [3], [5], {0: 1}, 4)  # wallet 2 spends money it never received
    except ValueError:
        pass
    else:
        raise AssertionError("ledger accepted an overdraft")


if __name__ == "__main__":
    compiled = "--nb" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--nb"]
    n_cases = int(args[0]) if len(args) > 0 else 1000
    seed = int(args[1]) if len(args) > 1 else 1
    ops, uops = run(n_cases, seed, compiled)
    validation()
    which = (f"lifo_nb.Engine (compiled; {COMPACTIONS[0]} arena compactions), in lockstep with "
             f"lifo.Engine," if compiled else "lifo.Engine")
    print(f"OK (seed {seed}): {which} on {n_cases} random ledgers, {ops} operations, every result "
          f"identical to a fresh trace and to a rebuild; on the {uops} operations over "
          f"small-amount ledgers the fresh trace also matched the unit-by-unit tracer")
