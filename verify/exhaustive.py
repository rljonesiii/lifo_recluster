"""Bounded exhaustive verification of lifo.Engine against the unit-level specification.

python3 verify/exhaustive.py [--quick] [--nb] [S1] [S2] [--part i/k]

--nb checks the compiled engine (lifo_nb.Engine) instead of lifo.Engine.

Scope (every case, no sampling; the two scopes share no ledger):
  S1: every valid ledger of 1-5 transactions using at most 3 wallets, amounts 1-2
  S2: every valid ledger of 1-5 transactions using exactly 4 wallets, amounts 1-2
Two sources (stolen, clean).  Ledgers are enumerated up to renaming of wallets (wallets
appear in order of first use) and must keep every wallet balance non-negative.

For every ledger and every clustering P of its wallets, the specification is executed
unit by unit (spec_trace: units are atoms (t, x), stacks are lists of atoms) and the
lemmas of the Proofs tab are asserted on it:
  T1  every pop is defined; each cluster's stack size equals its wallets' balances
  T2  every unit is removed by the first later event that takes its cluster's stack
      height to or below the unit's index (first passage)
  T4  the units a cluster's transactions carry are the same in every clustering that
      contains that cluster
  T5  each cluster's holdings equal the sum of its wallets' net stolen flows
  T7  every piece of a route points into an inflow received by the sending cluster
From it the reference state is built without the engine: route[t] = the units t carries,
as maximal runs (inflow, offset, length); readers = the inverse of the routes; labels,
stolen units per transaction, per-wallet net flows, and each cluster's holdings.
lifo.fresh_trace must equal it (and, with --nb, the compiled lifo_nb.fresh_trace).  T6 (pieces expand to exactly the units moved) follows
from every route check below, since the reference routes expand to the moved units.

Engine runs, under five settings (defaults; heap only; heap only without the cutoff;
linear pass at once with no cache; linear pass at once with a 1-route cache):
  * a fresh Engine for every clustering P;
  * from each P, on that engine: regroup([B]) for every nonempty wallet set B, each
    followed by regroup(P) to undo it (B may cut several clusters, leaving remainders);
  * on one long-lived engine: a walk through every ordered pair of clusterings (P, Q)
    exactly once (an Euler circuit of the complete directed graph), each step a full
    regroup(Q), so later steps hit, miss or evict a warm cache;
  * with default settings, from each P: every merge of two clusters and every split of
    a cluster, each followed by an undo.
With --nb, the second and fourth settings also compact the compiled engine's arenas
whenever dead rows outnumber live ones.
After every operation the whole engine state must equal the reference for the expected
clustering (computed by set operations alone): routes, readers, labels, stolen units per
transaction, net flows, the partition (in the order clusters() lists it) and each
cluster's holdings, the owner of every wallet, and every cache entry (its routes and readers must equal those of its membership).
After each engine's run of operations, and after every 16th operation within it, its own
check_against_fresh() must pass and, with --nb, its hidden state must be consistent
(check_invariants: live-row counters, arena bounds and compaction, scratch arrays left
clean, cluster ids, holdings).  The mid-run checks matter: every run ends in the
clustering it started from, so a counter that drifts and drifts back would otherwise pass.
"""
import os
import sys
import time
from itertools import combinations
from multiprocessing import Pool

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from lifo import Ledger, Engine, fresh_trace, stolen_of  # noqa: E402

COMPILED = "--nb" in sys.argv
if COMPILED:
    import lifo_nb  # noqa: E402
    Engine = lifo_nb.Engine  # noqa: F811

STOLEN, CLEAN = 0, 1  # source wallets; labels 1 (stolen) and 0 (clean)
CONFIGS = [
    ("defaults", dict()),
    ("heap only", dict(sweep_after=10 ** 9)),
    ("heap only, no cutoff", dict(sweep_after=10 ** 9, cutoff=False)),
    ("linear pass, no cache", dict(sweep_after=0, cache_budget=0)),
    ("linear pass, 1-route cache", dict(sweep_after=0, cache_budget=1)),
]


# --------------------------------------------------------------------- enumeration
def ledgers(n_max, W, A):
    """Every valid ledger with 1..n_max transactions over at most W wallets, amounts 1..A,
    wallets numbered 2, 3, ... in order of first use."""
    out = []

    def rec(tx, bal, used):
        if tx:
            out.append(tuple(tx))
        if len(tx) == n_max:
            return
        wallets = list(range(2, 2 + used))
        dsts = wallets + ([2 + used] if used < W else [])
        for s in [STOLEN, CLEAN] + [w for w in wallets if bal.get(w, 0) > 0]:
            amax = A if s < 2 else min(A, bal[s])
            for d in dsts:
                if d == s:
                    continue
                for a in range(1, amax + 1):
                    nb = dict(bal)
                    nb[d] = nb.get(d, 0) + a
                    if s >= 2:
                        nb[s] -= a
                    rec(tx + [(s, d, a)], nb, max(used, d - 1))

    rec([], {}, 0)
    return out


def n_wallets(tx):
    return max(d for _, d, _ in tx) - 1  # a wallet is first used as a receiver


def partitions(items):
    """Every set partition of items, as a list of frozensets."""
    if not items:
        yield []
        return
    first, rest = items[0], items[1:]
    for p in partitions(rest):
        yield [frozenset([first])] + p
        for i in range(len(p)):
            yield p[:i] + [p[i] | {first}] + p[i + 1:]


def subsets(items):
    return [frozenset(c) for k in range(1, len(items) + 1) for c in combinations(items, k)]


def apply_regroup(P, blocks):
    """The clustering regroup(blocks) must produce, by set operations alone."""
    touched = frozenset().union(*blocks)
    out = set(blocks)
    for c in P:
        rest = c - touched
        if rest:
            out.add(rest)
    return frozenset(out)


def euler_circuit(k):
    """Node sequence through every ordered pair (u, v), u != v, of k nodes exactly once."""
    adj = {u: [v for v in range(k) if v != u] for u in range(k)}
    stack, circ = [0], []
    while stack:
        u = stack[-1]
        if adj[u]:
            stack.append(adj[u].pop())
        else:
            circ.append(stack.pop())
    circ.reverse()
    assert len(set(zip(circ, circ[1:]))) == len(circ) - 1 == k * (k - 1)
    return circ


# ------------------------------------------------------------------ specification
def spec_trace(L, co):
    """Unit-level semantics.  Units are atoms (t, x); a deposit or external transfer t
    pushes its own atoms (t, 0..a-1) bottom to top; an external transfer first removes the
    top a atoms of the sender's stack; an internal transfer moves nothing.  Returns
    (moved, labels, holdings): moved[t] = atoms t carries, bottom to top."""
    n = L.n
    src, dst, amt, slab = L.src, L.dst, L.amt, L.source_label
    stacks, unit_label, height_at = {}, {}, {}
    moved = [None] * n
    bal = {}
    events = {}       # cluster -> [(t, stack height after t)]
    consumed = {}     # atom -> transaction that removed it
    for t in range(n):
        s, d, a = src[t], dst[t], amt[t]
        cd = co[d]
        if slab[s] >= 0:
            carried = None
            lab = [slab[s]] * a
        else:
            cs = co[s]
            st = stacks.setdefault(cs, [])
            assert len(st) >= a, "T1: pop undefined"
            carried = st[len(st) - a:]
            moved[t] = carried
            lab = [unit_label[u] for u in carried]
            if cs != cd:
                del st[len(st) - a:]
                for u in carried:
                    assert u not in consumed, "T1: unit removed twice"
                    consumed[u] = t
        if slab[s] >= 0 or co[s] != cd:  # the receiver pushes t's own atoms
            st = stacks.setdefault(cd, [])
            for x in range(a):
                u = (t, x)
                unit_label[u] = lab[x]
                height_at[u] = (cd, len(st), t)
                st.append(u)
        if moved[t] is None:
            moved[t] = [(t, x) for x in range(a)]
        if slab[s] < 0:
            bal[s] = bal.get(s, 0) - a
        bal[d] = bal.get(d, 0) + a
        for c in {co[d]} | ({co[s]} if slab[s] < 0 else set()):
            h = len(stacks.get(c, []))
            assert h == sum(v for w, v in bal.items() if co[w] == c), "T1: stack != balances"
            events.setdefault(c, []).append((t, h))
    for u, (c, y, t0) in height_at.items():  # T2: first passage
        first = next((t for t, h in events[c] if t > t0 and h <= y), None)
        assert consumed.get(u) == first, "T2: first passage violated"
    labels = []
    for t in range(n):
        runs = []
        for u in moved[t]:
            x = unit_label[u]
            if runs and runs[-2] == x:
                runs[-1] += 1
            else:
                runs += [x, 1]
        labels.append(tuple(runs))
    hold = {}
    for c, st in stacks.items():
        hold[c] = sum(1 for u in st if unit_label[u])
    return moved, labels, hold


def runs_of_units(units):
    """Units as maximal runs (inflow, offset, length), flattened."""
    out = []
    for s, x in units:
        if out and out[-3] == s and out[-2] + out[-1] == x:
            out[-1] += 1
        else:
            out += [s, x, 1]
    return tuple(out)


def expand(p):
    return [(p[i], p[i + 1] + j) for i in range(0, len(p), 3) for j in range(p[i + 2])]


def inverse(routes):
    """readers of a set of routes {t: pieces}: inflow -> senders reading it, in time order."""
    rd = {}
    for t in sorted(routes):
        for i in range(0, len(routes[t]), 3):
            rd.setdefault(routes[t][i], []).append(t)
    return rd


class Ref:
    """The state an exact engine must hold for clustering P, built from the specification."""
    __slots__ = ("P", "route", "readers", "labels", "stol", "net", "hold", "order")


def reference(L, P, by_members):
    W, n = L.W, L.n
    co = [None] * W
    for b in P:
        for w in b:
            co[w] = min(b)
    moved, labels, hold = spec_trace(L, co)
    out, fhold = fresh_trace(L, co)
    assert out == labels, "fresh_trace labels != specification"
    assert {k: v for k, v in fhold.items() if v} == {k: v for k, v in hold.items() if v}, \
        "fresh_trace holdings != specification"
    if COMPILED:  # the compiled fresh trace, used by the benchmark as its reference
        nout, nhold = lifo_nb.fresh_trace(L, co)
        assert nout == labels and {k: v for k, v in nhold.items() if v} == \
            {k: v for k, v in hold.items() if v}, "compiled fresh trace != specification"
    deposit = [L.source_label[L.src[t]] >= 0 for t in range(n)]
    route = [None if deposit[t] else runs_of_units(moved[t]) for t in range(n)]
    for t in range(n):
        if route[t] is not None:
            assert expand(route[t]) == moved[t]
            for i in range(0, len(route[t]), 3):  # T7
                assert co[L.dst[route[t][i]]] == co[L.src[t]], "T7: piece outside cluster"
    stol = [stolen_of(lab) for lab in labels]
    net = [0] * W
    for t in range(n):
        net[L.dst[t]] += stol[t]
        if not deposit[t]:
            net[L.src[t]] -= stol[t]
    R = Ref()
    R.P = frozenset(P)
    R.order = sorted(P, key=min)  # clusters() lists clusters by their first wallet
    R.hold = {}
    readers = {}
    for b in P:
        R.hold[b] = hold.get(min(b), 0)
        assert R.hold[b] == sum(net[w] for w in b), "T5: holdings != net flows"
        r = {t: route[t] for t in range(n) if not deposit[t] and L.src[t] in b}
        rd = inverse(r)
        prev = by_members.setdefault(b, (r, rd))
        assert prev == (r, rd), "T4: carried units depend on more than membership"
        readers.update(rd)
    assert readers == inverse({t: p for t, p in enumerate(route) if p is not None})
    R.route, R.readers, R.labels = route, readers, labels
    R.stol, R.net = stol, net
    return R


# ------------------------------------------------------------------- one ledger
CHECKS = [0]


def check_state(E, R, by_members, what):
    CHECKS[0] += 1
    if CHECKS[0] % 16 == 0:  # mid-run too: every run ends where it started, which hides drift
        check_hidden(E, what)
    if COMPILED:
        return check_exported(E, R, by_members, what)
    if E.route != R.route:
        raise AssertionError(f"routes ({what})")
    if E.readers != R.readers:
        raise AssertionError(f"readers ({what})")
    if E.labels != R.labels:
        raise AssertionError(f"labels ({what})")
    if list(E.stol) != R.stol:
        raise AssertionError(f"stolen units per transaction ({what})")
    if list(E.net) != R.net:
        raise AssertionError(f"net flows ({what})")
    if {c.members: c.hold for c in E.clusters()} != R.hold:
        raise AssertionError(f"partition or holdings ({what})")
    if [c.members for c in E.clusters()] != R.order:
        raise AssertionError(f"order of clusters() ({what})")
    for b in R.P:
        for w in b:
            if w not in E.owner[w].members:
                raise AssertionError(f"owner ({what})")
    total = 0
    for m, (r, rd) in E.cache.items():
        if (r, rd) != by_members[m]:
            raise AssertionError(f"cache entry ({what})")
        total += len(r)
    if total != E.cache_entries or (E.cache_entries > E.cache_budget):
        raise AssertionError(f"cache accounting ({what})")


def check_exported(E, R, by_members, what):
    """The same checks on the compiled engine's state, exported to Python form."""
    S = E.export()
    for key, ref in (("route", R.route), ("readers", R.readers), ("labels", R.labels),
                     ("stol", R.stol), ("net", R.net), ("clusters", R.hold)):
        if S[key] != ref:
            raise AssertionError(f"{key} ({what})")
    if list(S["clusters"]) != R.order:
        raise AssertionError(f"order of clusters() ({what})")
    if not S["owners_ok"]:
        raise AssertionError(f"owner ({what})")
    total = 0
    for m, r, rd in S["cache"]:
        if (r, rd) != by_members[m]:
            raise AssertionError(f"cache entry ({what})")
        total += len(r)
    if total != S["cache_entries"] or S["cache_entries"] > S["cache_budget"]:
        raise AssertionError(f"cache accounting ({what})")


def check_hidden(E, what):
    """After a run of operations on one engine: its own self-check against a fresh trace,
    and for the compiled engine the state export() does not show (live-row counters,
    arena bounds and compaction, scratch arrays, cluster ids)."""
    if not E.check_against_fresh():
        raise AssertionError(f"check_against_fresh ({what})")
    if COMPILED:
        bad = E.check_invariants()
        if bad:
            raise AssertionError(f"hidden state: {bad} ({what})")


def check_ledger(tx):
    src = [s for s, _, _ in tx]
    dst = [d for _, d, _ in tx]
    amt = [a for _, _, a in tx]
    W = max(dst) + 1
    L = Ledger(src, dst, amt, {STOLEN: 1, CLEAN: 0}, W)
    wallets = L.wallets()
    parts = [frozenset(P) for P in partitions(wallets)]
    by_members = {}
    ref = {P: reference(L, P, by_members) for P in parts}
    assert set(by_members) == set(subsets(wallets))
    blocks = subsets(wallets)
    walk = euler_circuit(len(parts))
    ops = 0
    for ci, (name, cfg) in enumerate(CONFIGS):
        for P in parts:  # fresh engines, then every single-block regroup with undo
            E = Engine(L, P, **cfg)
            if COMPILED and ci % 2:
                E.compact_slack = 0  # compact the arenas whenever dead rows exceed live ones
            check_state(E, ref[P], by_members, f"{name}: build {sorted(map(sorted, P))}")
            for B in blocks:
                Q = apply_regroup(P, [B])
                E.regroup([B])
                check_state(E, ref[Q], by_members, f"{name}: regroup {sorted(B)} from {sorted(map(sorted, P))}")
                E.regroup(P)
                check_state(E, ref[P], by_members, f"{name}: undo regroup {sorted(B)}")
                ops += 2
            ops += 1
            if name == "defaults":
                for C1, C2 in combinations(sorted(P, key=sorted), 2):
                    E.merge([min(C1), min(C2)])
                    check_state(E, ref[apply_regroup(P, [C1 | C2])], by_members, f"merge {sorted(C1)} {sorted(C2)}")
                    E.regroup(P)
                    check_state(E, ref[P], by_members, "undo merge")
                    ops += 2
                for C in P:
                    for S in subsets(sorted(C))[:-1]:  # nonempty proper subsets
                        E.split(S)
                        check_state(E, ref[apply_regroup(P, [S])], by_members, f"split {sorted(S)} from {sorted(C)}")
                        E.regroup(P)
                        check_state(E, ref[P], by_members, "undo split")
                        ops += 2
            check_hidden(E, f"{name}: after the operations from {sorted(map(sorted, P))}")
        E = Engine(L, parts[walk[0]], **cfg)  # every ordered pair of clusterings, once
        if COMPILED and ci % 2:
            E.compact_slack = 0
        for i in walk[1:]:
            E.regroup(parts[i])
            check_state(E, ref[parts[i]], by_members, f"{name}: walk to {sorted(map(sorted, parts[i]))}")
            ops += 1
        check_hidden(E, f"{name}: after the walk")
    return dict(clusterings=len(parts), ops=ops)


def run_chunk(chunk):
    tot = dict(ledgers=0, clusterings=0, ops=0)
    for tx in chunk:
        try:
            c = check_ledger(tx)
        except AssertionError as e:
            return dict(failure=(str(e), tx))
        tot["ledgers"] += 1
        tot["clusterings"] += c["clusterings"]
        tot["ops"] += c["ops"]
    return tot


def scope(name, quick):
    if name == "S1":
        n_max = 4 if quick else 5
        return f"<= {n_max} transactions, <= 3 wallets, amounts 1-2", ledgers(n_max, 3, 2)
    n_max = 4 if quick else 5
    return (f"<= {n_max} transactions, exactly 4 wallets, amounts 1-2",
            [tx for tx in ledgers(n_max, 4, 2) if n_wallets(tx) == 4])


def main():
    quick = "--quick" in sys.argv
    print(f"engine: {'lifo_nb.Engine (compiled)' if COMPILED else 'lifo.Engine'}")
    only = [a for a in sys.argv[1:] if a in ("S1", "S2")] or ["S1", "S2"]
    grand = dict(ledgers=0, clusterings=0, ops=0)
    for name in only:
        t0 = time.time()
        desc, all_l = scope(name, quick)
        chunks = [all_l[k:k + 200] for k in range(0, len(all_l), 200)]
        part = next((a.split("/") for a in sys.argv if "/" in a and a[0].isdigit()), None)
        if part:  # every k-th chunk, so a long scope can run in pieces
            i, k = int(part[0]), int(part[1])
            chunks = chunks[i::k]
            desc += f", part {i + 1} of {k}"
        tot = dict(ledgers=0, clusterings=0, ops=0)
        with Pool(os.cpu_count() or 1) as pool:
            for res in pool.imap_unordered(run_chunk, chunks):
                if "failure" in res:
                    print(f"FAIL in {name}: {res['failure'][0]}\n  ledger (src, dst, amount): {res['failure'][1]}")
                    pool.terminate()
                    sys.exit(1)
                for k in tot:
                    tot[k] += res[k]
        for k in grand:
            grand[k] += tot[k]
        print(f"{name}: {desc}: {tot['ledgers']:,} ledgers, {tot['clusterings']:,} (ledger, clustering) "
              f"cases, {tot['ops']:,} engine states checked ({len(CONFIGS)} settings) "
              f"- all passed in {time.time() - t0:.0f} s")
        sys.stdout.flush()
    print(f"TOTAL: {grand['ledgers']:,} ledgers, {grand['clusterings']:,} clustering cases, "
          f"{grand['ops']:,} engine states checked; T1, T2, T4, T5, T7 asserted on the "
          f"specification, every state equal to it")


if __name__ == "__main__":
    main()
