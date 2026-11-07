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
cache in LRU order) and on the work counters, and the compiled engine's hidden state
(live-row counters, arena bounds, scratch arrays, cluster ids, holdings) must stay
consistent.  Its arena compaction threshold rotates through the default 4096, 0 and 64.
Then targeted tests: amounts near the compiled engine's int64 bound and labels near
2**63 (and amounts far beyond int64 for the reference), ledgers past the bound (refused
by the compiled engine, run on the reference through engine.Engine), linear-pass
thresholds of every kind (also changed between operations), a compaction slack lowered
between operations, holdings-array growth, and checks that the self-checks
(check_against_fresh, check_invariants) notice a corrupted label or holding.

Usage: python3 test_engine.py [n_cases] [seed] [--nb]
"""
import math
import random
import sys
import zlib

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


def scaled(L, k, labels=None):
    """The same ledger with every amount multiplied by k and labels renamed by `labels`."""
    labels = labels or {}
    return Ledger(L.src, L.dst, [a * k for a in L.amt],
                  {w: labels.get(l, l) for w, l in enumerate(L.source_label) if l >= 0}, L.W)


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
    if type(eng) is not lifo.Engine:  # the compiled fresh trace too, and the query views
        import lifo_nb
        nout, nhold = lifo_nb.fresh_trace(L, co)
        assert nout == out and nhold == fhold, f"compiled fresh trace differs {where}"
        assert eng.check_against_fresh(), f"check_against_fresh failed {where}"
        h = zlib.crc32(where.encode())
        a, b = sorted((h % (L.n + 1), (h >> 12) % (L.n + 1)))
        assert eng.labels[a:b] == out[a:b] and eng.labels[::-3] == out[::-3], f"label slice {where}"
        routes = [eng.route[t] for t in range(L.n)]
        assert eng.route[a:b] == routes[a:b] and eng.route[::-2] == routes[::-2], f"route slice {where}"
        if h % 3 == 0:  # some of the time: other cluster keys, None among them
            co2 = [None if c is not None and c % 2 else c for c in co]
            assert lifo_nb.fresh_trace(L, co2) == fresh_trace(L, co2), f"None keys {where}"
    if units:
        uout, uhold = unit_trace(L, co)
        assert uout == out and nz(uhold) == nz(fhold), f"fresh_trace differs from unit tracer {where}"
    ref = type(eng)(L, [c.members for c in eng.clusters()])
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
SLACKS = [4096, 0, 64]  # compiled engine: dead arena rows tolerated beyond the live ones
COMPACTIONS = [0]


def lockstep(eng, twin, where):
    if twin is None:
        return
    from lifo_nb import state_of
    a, b = state_of(eng), state_of(twin)
    for k in a:
        assert a[k] == b[k], f"compiled engine differs from lifo.Engine in {k} {where}"
    assert [c.members for c in eng.clusters()] == [c.members for c in twin.clusters()], \
        f"clusters() lists the clusters in another order {where}"
    bad = eng.check_invariants()
    assert not bad, f"compiled engine's hidden state: {bad} {where}"
    s = dict(eng.stats)
    COMPACTIONS[0] += s.pop("compactions")
    eng.stats["compactions"] = 0
    assert s == twin.stats, f"work counters differ {where}"


def operate(rng, eng, twin, L, where, units, steps):
    """Random merges, splits and regroups on eng (and twin), each result checked."""
    wallets = L.wallets()
    ops = 0
    for step in range(steps):
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
        check(eng, L, f"{where} step {step} {what}", units)
        lockstep(eng, twin, f"{where} step {step} {what}")
    return ops


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
        init = random_partition(rng, L.wallets(), p_join=rng.choice([0.0, 0.3, 0.6]))
        eng = Engine(L, init, **MODES[case % len(MODES)])
        twin = lifo.Engine(L, init, **MODES[case % len(MODES)]) if compiled else None
        if compiled:
            eng.compact_slack = SLACKS[(case // len(MODES)) % len(SLACKS)]
        check(eng, L, f"case {case} init", units)
        lockstep(eng, twin, f"case {case} init")
        k = operate(rng, eng, twin, L, f"case {case}", units, rng.randint(1, 12))
        ops += k
        uops += k * units
    return ops, uops


def expect_error(f, what):
    try:
        f()
    except ValueError:
        return
    raise AssertionError(f"accepted {what}")


def targeted(seed=1, compiled=False):
    """Large values, the int64 bound, thresholds, holdings growth, and the self-checks.
    Returns the number of operations checked."""
    import engine as factory
    rng = random.Random(seed + 1000)
    twin_of = (lambda L, init, **kw: lifo.Engine(L, init, **kw)) if compiled else (lambda *a, **k: None)
    ops = 0
    # amounts close to the compiled engine's bound (total volume just under 2**62) and labels
    # near 2**63; for the reference, half the cases use amounts around 10**30 instead
    for case in range(40):
        L0 = random_ledger(rng, rng.randint(3, 12), rng.randint(20, 300), n_labels=3,
                           max_amt=rng.choice([3, 50, 1000]))
        k = (2 ** 62 - 1) // sum(L0.amt) if compiled or case % 2 else 10 ** 30 + rng.randrange(10 ** 6)
        L = scaled(L0, k, {1: 2 ** 63 - 1, 2: 2 ** 63 - 2} if case % 3 else None)
        init = random_partition(rng, L.wallets(), p_join=0.4)
        eng = Engine(L, init, **MODES[case % len(MODES)])
        twin = twin_of(L, init, **MODES[case % len(MODES)])
        if compiled:
            eng.compact_slack = SLACKS[case % len(SLACKS)]
        check(eng, L, f"large values {case} init")
        lockstep(eng, twin, f"large values {case} init")
        ops += operate(rng, eng, twin, L, f"large values {case}", False, 8)
    # past the bound: the compiled engine refuses the ledger and engine.Engine falls back
    L0 = random_ledger(rng, 8, 200, n_labels=3, max_amt=9)
    for L in (scaled(L0, 2 ** 62 // sum(L0.amt) + 1), scaled(L0, 1, {1: 2 ** 63})):
        init = random_partition(rng, L.wallets())
        if compiled:
            import lifo_nb
            expect_error(lambda: lifo_nb.Engine(L), "a ledger past the int64 bound (compiled)")
            co = lifo.Engine(L, init).cluster_of()[0]
            assert lifo_nb.fresh_trace(L, co) == fresh_trace(L, co), "compiled fresh_trace past the bound"
        eng = factory.Engine(L, init)
        assert type(eng).__module__ == "lifo", "engine.Engine did not fall back to the reference"
        assert isinstance(eng, factory.Engine) and eng.check_against_fresh()
        ops += operate(rng, eng, None, L, "past the bound", False, 6)
    if compiled:
        e0 = factory.Engine(L0)
        assert type(e0).__module__ == "lifo_nb" and isinstance(e0, factory.Engine), \
            "engine.Engine did not pick the compiled engine"
    # linear-pass thresholds of every kind, at build and changed later, as in the reference
    L = random_ledger(rng, 10, 400, n_labels=3, max_amt=9)
    for sa in (10 ** 30, math.inf, math.nan, -math.inf, -5, -0.5, 0, 0.5, 1, 2.5, 3):
        init = random_partition(rng, L.wallets())
        eng, twin = Engine(L, init, sweep_after=sa), twin_of(L, init, sweep_after=sa)
        ops += operate(rng, eng, twin, L, f"sweep_after={sa}", True, 3)
        eng.sweep_after = 10 ** 9 if sa != sa or sa > 3 else 0
        if twin:
            twin.sweep_after = eng.sweep_after
        ops += operate(rng, eng, twin, L, f"sweep_after={sa}, then {eng.sweep_after}", True, 3)
    # lowering the compaction slack between operations is legal, and is honoured next time
    if compiled:
        init = random_partition(rng, L.wallets(), 0.5)
        eng, twin = Engine(L, init, cache_budget=0), twin_of(L, init, cache_budget=0)
        ops += operate(rng, eng, twin, L, "slack 4096", True, 10)
        eng.compact_slack = 0
        assert not eng.check_invariants(), "check_invariants flagged a legal state after the slack fell"
        ops += operate(rng, eng, twin, L, "slack 0", True, 4)
    # holdings array growth: one cluster of 80 wallets split into singletons, merged back
    L = random_ledger(rng, 80, 600, n_labels=2, max_amt=9)
    ws = L.wallets()
    eng, twin = Engine(L, [ws]), twin_of(L, [ws])
    order = ws[:]
    rng.shuffle(order)
    for i, w in enumerate(order[:-1]):
        eng.split([w]); twin and twin.split([w])
        lockstep(eng, twin, f"growth: split {w}")
        if i % 9 == 0:
            check(eng, L, f"growth: split {w}")
    check(eng, L, "growth: singletons")
    for i in range(1, len(order)):
        eng.merge(order[:i + 1]); twin and twin.merge(order[:i + 1])
        lockstep(eng, twin, f"growth: merge {i}")
    check(eng, L, "growth: merged back")
    ops += 2 * len(order) - 2
    # the self-checks must notice a wrong label (either column of a run) or a wrong holding
    L = random_ledger(rng, 10, 300, n_labels=3, max_amt=9)
    eng = Engine(L, random_partition(rng, L.wallets(), 0.5))
    assert eng.check_against_fresh()
    t = max(range(L.n), key=lambda t: len(eng.labels[t]))
    if compiled:
        row = int(eng.ls[t] + eng.ll[t] - 1)
        for col in (0, 1):
            eng.LB[row, col] += 1
            assert not eng.check_against_fresh(), f"check_against_fresh missed a wrong label (column {col})"
            eng.LB[row, col] -= 1
        c = next(iter(eng.members))
        eng.hold[c] += 1
        assert not eng.check_against_fresh(), "check_against_fresh missed a wrong holding"
        assert eng.check_invariants(), "check_invariants missed a wrong holding"
        eng.hold[c] -= 1
        eng.live_p += 1
        assert eng.check_invariants(), "check_invariants missed a wrong live counter"
        eng.live_p -= 1
        eng.mark[0] = True
        assert eng.check_invariants(), "check_invariants missed a dirty scratch array"
        eng.mark[0] = False
        assert eng.check_against_fresh() and not eng.check_invariants()
    else:
        good = eng.labels[t]
        for i in (len(good) - 2, len(good) - 1):
            eng.labels[t] = good[:i] + (good[i] + 1,) + good[i + 1:]
            assert not eng.check_against_fresh(), "check_against_fresh missed a wrong label"
        eng.labels[t] = good
        c = eng.clusters()[0]
        c.hold += 1
        assert not eng.check_against_fresh(), "check_against_fresh missed a wrong holding"
        c.hold -= 1
        assert eng.check_against_fresh()
    return ops


def validation():
    """Invalid input is rejected, with ValueError, before anything changes."""
    rng = random.Random(5)
    L = random_ledger(rng, 6, 80, n_labels=2, max_amt=9)
    for bad in ([[2, 3], [3, 4]], [[0, 3]], [[2, 99]], [[2, -1]], [[2.0, 3]], [[2.5]]):
        expect_error(lambda: Engine(L, bad), f"clusters {bad}")
    eng = Engine(L, [[2, 3, 4]])
    before = list(eng.labels)
    for bad in ([[2, 3], [3, 4]], [[2], [0, 3]], [[2, -1]], [[7, 2.5]]):
        expect_error(lambda: eng.regroup(bad), f"blocks {bad}")
    for bad in ([2, 5], [0], [-1], [99], [2.0], [3, 4.5]):
        expect_error(lambda: eng.split(bad), f"split {bad}")
    for bad in ([0, 2], [-1, 2], [2, 99], [2.0, 5], [5, None]):
        expect_error(lambda: eng.merge(bad), f"merge {bad}")
    for bad in ("4", True, [3]):  # None means the default, n // 4
        expect_error(lambda: Engine(L, sweep_after=bad), f"sweep_after={bad!r}")
    eng.split([])
    eng.merge([2])
    assert eng.labels == before
    assert sorted(sorted(c.members) for c in eng.clusters() if len(c.members) > 1) == [[2, 3, 4]]
    # a bool is not a wallet id, even where it would name an ordinary wallet (True == 1)
    Lb = Ledger([0, 0], [1, 2], [5, 5], {0: 1}, 3)
    expect_error(lambda: Engine(Lb, [[True, 2]]), "a bool wallet id in clusters")
    eb = Engine(Lb)
    for f in (lambda: eb.merge([True, 2]), lambda: eb.split([True]), lambda: eb.regroup([[True, 2]])):
        expect_error(f, "a bool wallet id")
    assert len(eb.clusters()) == 2
    # ledgers: overdrafts, non-integers (floats, bools), out-of-range ids, bad labels
    for args, what in ((([2], [3], [5], {0: 1}, 4), "an overdraft"),
                       (([0], [3], [2.5], {0: 1}, 4), "a fractional amount"),
                       (([0], [3], [2.0], {0: 1}, 4), "a float amount"),
                       (([0], [3], [True], {0: 1}, 4), "a bool amount"),
                       (([0.0], [3], [2], {0: 1}, 4), "a float wallet id"),
                       (([0], [-1], [2], {0: 1}, 4), "a negative wallet id"),
                       (([0], [4], [2], {0: 1}, 4), "a wallet id past n_wallets"),
                       (([0], [3], [2], {0: 0.5}, 4), "a fractional label"),
                       (([0], [3], [2], {0: -1}, 4), "a negative label"),
                       (([0], [3, 2], [2], {0: 1}, 4), "columns of different lengths"),
                       (([0], [3], [0], {0: 1}, 4), "a zero amount"),
                       (([], [], [], {}, -3), "a negative n_wallets")):
        expect_error(lambda: Ledger(*args), f"a ledger with {what}")
    try:
        import numpy as np
    except ImportError:
        return
    # NumPy integer columns are taken as Python ints: no int32 wraparound in sums
    big = np.array([2 ** 31 - 1] * 2, np.int32)
    L = Ledger(np.array([0, 0], np.int32), np.array([3, 3], np.int32), big, {0: 1}, 4)
    assert all(type(x) is int for x in L.src + L.dst + L.amt)
    assert Engine(L).cluster_of()[1][3] == 2 ** 32 - 2, "int32 amounts wrapped"


if __name__ == "__main__":
    compiled = "--nb" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--nb"]
    n_cases = int(args[0]) if len(args) > 0 else 1000
    seed = int(args[1]) if len(args) > 1 else 1
    ops, uops = run(n_cases, seed, compiled)
    tops = targeted(seed, compiled)
    validation()
    which = (f"lifo_nb.Engine (compiled; {COMPACTIONS[0]} arena compactions), in lockstep with "
             f"lifo.Engine," if compiled else "lifo.Engine")
    print(f"OK (seed {seed}): {which} on {n_cases} random ledgers, {ops} operations, every result "
          f"identical to a fresh trace and to a rebuild; on the {uops} operations over "
          f"small-amount ledgers the fresh trace also matched the unit-by-unit tracer; "
          f"targeted tests ({tops} operations: values near and past the int64 bound, "
          f"thresholds, holdings growth, self-checks) and input validation passed")
