"""SMT verification (Z3) of the lemmas the engine's correctness rests on.

python3 verify/smt_checks.py [--depth K]

Part 1  T6, run-length representation.  Every branch of lifo.sweep's per-event code
        (push; read or pop inside the top block; exact pop; multi-block read or pop) is
        transcribed line by line below with each comparison routed through br(), and
        executed symbolically: inflow ids, block sizes and the amount are unbounded Z3
        integers, the stack depth is every k = 0..K.  Every feasible path is enumerated and,
        on each, Z3 proves the result against the unit-level meaning of the stack:
          - the pieces are non-empty, total the amount, and expand to exactly the top a
            units of the stack, bottom to top;
          - a pop leaves exactly the units below them, as well-formed blocks;
          - a read leaves the stack unchanged; a push appends exactly t's own units;
          - no path can run off the bottom of the stack (the precondition a <= height
            is enough).
Part 2  T1 and T5, the arithmetic of the induction step.  Stacks are summarized by their
        height and stolen count; that an inflow adds and an outflow removes exactly the units
        the transaction carries is the content of Part 1 (T6), not re-derived here.  For one cluster C and one transaction
        of each kind (deposit into C, transfer into C, transfer out of C, internal to C,
        unrelated), Z3 proves for all values that the invariant
            stack height = sum of C's wallet balances  and
            stolen units on the stack = sum of C's wallets' net stolen flows
        is preserved, and that every read or pop C performs is defined.  The invariant holds
        initially (all zero), so it holds after every transaction of every ledger.
Part 3  T3, push order.  For a = 1..5 and every permutation pi of a units, Z3 proves that
        popping the top a units and pushing them back in order pi leaves every possible
        label sequence unchanged if and only if pi is the identity.
"""
import argparse
import itertools
import sys
import time

from z3 import And, Implies, Int, Ints, Bool, Not, Or, If, Solver, Sum, sat, unsat


class NeedDecision(Exception):
    def __init__(self, cond):
        self.cond = cond


class Underflow(Exception):
    pass


# ----------------------------------------------------- the code under verification
# Transcribed from lifo.sweep: the body executed for one event t of the cluster.
# Comparisons on amounts go through br(); everything else is unchanged.
def inflow_step(ss, rr, t, a):
    ss.append(t); rr.append(a)              # inflow: push


def sent_step(ss, rr, a, internal, br):
    r = rr[-1]
    if br(r >= a) and internal:             # internal read inside the top block
        s = ss[-1]; route = [(s, r - a, a)]
    elif br(r > a):                         # outflow inside the top block
        rr[-1] = r - a
        s = ss[-1]; route = [(s, r - a, a)]
    elif br(r == a):                        # outflow takes exactly the top block
        rr.pop()
        s = ss.pop(); route = [(s, 0, a)]
    else:                                   # several blocks: read or pop them
        acc = []
        need = a
        i = len(rr) - 1
        while br(need != 0):
            if i < 0:
                raise Underflow()
            r = rr[i]
            if br(r <= need):
                acc.append((ss[i], 0, r)); need = need - r; i -= 1
            else:
                acc.append((ss[i], r - need, need))
                if not internal:
                    rr[i] = r - need
                need = 0
        if not internal:
            del ss[i + 1:], rr[i + 1:]
        acc.reverse()
        route = acc
    return route


# ------------------------------------------------------------ meaning of a stack
def unit_at(ss, rr, y):
    """(inflow, position) of the unit at height y of the block stack, as Z3 terms."""
    terms = []
    base = 0
    for s, r in zip(ss, rr):
        terms.append((base, r, s))
        base = base + r
    s_term, x_term = Int("undef_s"), Int("undef_x")
    for b, r, s in reversed(terms):
        inside = And(b <= y, y < b + r)
        s_term = If(inside, s, s_term)
        x_term = If(inside, y - b, x_term)
    return s_term, x_term


def piece_unit(route, z):
    terms, base = [], 0
    for s, off, ln in route:
        terms.append((base, ln, s, off))
        base = base + ln
    s_term, x_term = Int("undef_ps"), Int("undef_px")
    for b, ln, s, off in reversed(terms):
        inside = And(b <= z, z < b + ln)
        s_term = If(inside, s, s_term)
        x_term = If(inside, off + (z - b), x_term)
    return s_term, x_term


def total(rr):
    return Sum(rr) if rr else 0


def well_formed(ss, rr):
    if len(ss) != len(rr):          # ids and sizes must stay aligned block for block
        return False
    c = [r >= 1 for r in rr]
    c += [ss[i] < ss[i + 1] for i in range(len(ss) - 1)]
    return And(c) if c else True


# ---------------------------------------------------------- path exploration
def prove(solver_facts, goal):
    s = Solver()
    s.add(*solver_facts)
    s.add(Not(goal))
    res = s.check()
    if res != unsat:
        raise AssertionError(f"counterexample: {s.model()}")


def feasible(facts):
    s = Solver()
    s.add(*facts)
    return s.check() == sat


def explore_sent(k, internal, step=sent_step):
    """All feasible paths of `step` on a k-block stack; returns the number proved."""
    S = [Int(f"s{i}") for i in range(k)]
    R = [Int(f"r{i}") for i in range(k)]
    a, y, z = Ints("a y z")
    T = total(R)
    pre = [well_formed(S, R), a >= 1, a <= T]
    assert feasible(pre), "vacuous precondition"
    proved = 0
    pending = [[]]
    while pending:
        decisions = pending.pop()
        trace = []

        def br(c, decisions=decisions, trace=trace):
            if isinstance(c, bool):
                return c
            if len(trace) < len(decisions):
                d = decisions[len(trace)]
                trace.append(c if d else Not(c))
                return d
            raise NeedDecision(c)

        ss, rr = list(S), list(R)
        try:
            route = step(ss, rr, a, internal, br)
        except NeedDecision as nd:
            for d in (True, False):
                if feasible(pre + trace + [nd.cond if d else Not(nd.cond)]):
                    pending.append(decisions + [d])
            continue
        except Underflow:
            if feasible(pre + trace):
                raise AssertionError(f"k={k}: a feasible path runs off the bottom of the stack")
            continue
        facts = pre + trace
        # the pieces: non-empty, total a, and exactly the top a units, bottom to top
        lens = [ln for _, _, ln in route]
        prove(facts, And(And([ln >= 1 for ln in lens]), Sum(lens) == a))
        ps, px = piece_unit(route, z)
        us, ux = unit_at(S, R, T - a + z)
        prove(facts + [0 <= z, z < a], And(ps == us, px == ux))
        if internal:  # a read leaves the stack unchanged, both lists
            if len(ss) != k or len(rr) != k:
                raise AssertionError(f"k={k}: a read changed the number of blocks")
            prove(facts, And(And([ss[i] == S[i] for i in range(k)]),
                             And([rr[i] == R[i] for i in range(k)])))
        else:         # a pop leaves exactly the units below, as well-formed blocks
            ns, nx = unit_at(ss, rr, y)
            os_, ox = unit_at(S, R, y)
            if len(ss) != len(rr):
                raise AssertionError(f"k={k}: a pop left ids and sizes misaligned")
            prove(facts, And(well_formed(ss, rr), total(rr) == T - a))
            prove(facts + [0 <= y, y < T - a], And(ns == os_, nx == ox))
        proved += 1
    return proved


def check_push(k):
    S = [Int(f"s{i}") for i in range(k)]
    R = [Int(f"r{i}") for i in range(k)]
    t, a, y = Ints("t a y")
    pre = [well_formed(S, R), a >= 1] + ([t > S[-1]] if k else [])
    ss, rr = list(S), list(R)
    inflow_step(ss, rr, t, a)
    T = total(R)
    ns, nx = unit_at(ss, rr, y)
    os_, ox = unit_at(S, R, y)
    prove(pre, well_formed(ss, rr))
    prove(pre + [0 <= y, y < T], And(ns == os_, nx == ox))
    prove(pre + [T <= y, y < T + a], And(ns == t, nx == y - T))
    return 1


# ------------------------------------------------------------------- Part 2
def part2():
    """Induction step for T1 (stack height = balances; reads and pops defined) and
    T5 (stolen units on the stack = net stolen flow), one transaction, all kinds."""
    H, S, Bal, Net, a, sig, bal_src, rest = Ints("H S Bal Net a sigma bal_src rest")
    cases = 0
    kinds = ["deposit into C", "transfer into C", "transfer out of C", "internal to C",
             "unrelated to C"]
    for kind in kinds:
        inv = And(H == Bal, S == Net)
        valid = [a >= 1, sig >= 0, sig <= a]
        if kind in ("transfer out of C", "internal to C"):
            # the sender is a member of C with enough balance; other members are >= 0
            valid += [bal_src >= a, rest >= 0, Bal == bal_src + rest]
        if kind == "transfer into C":
            valid += [bal_src >= a]   # the sender (outside C) can pay
        # semantics of the stack of C, and the definitions of balance and net flow
        if kind in ("deposit into C", "transfer into C"):
            H2, S2, Bal2, Net2 = H + a, S + sig, Bal + a, Net + sig
            defined = True
        elif kind == "transfer out of C":
            H2, S2, Bal2, Net2 = H - a, S - sig, Bal - a, Net - sig
            defined = H >= a
        elif kind == "internal to C":
            H2, S2, Bal2, Net2 = H, S, Bal - a + a, Net - sig + sig
            defined = H >= a
        else:
            H2, S2, Bal2, Net2 = H, S, Bal, Net
            defined = True
        assert feasible([inv] + valid), f"vacuous precondition for {kind}"
        prove([inv] + valid, And(defined, H2 == Bal2, S2 == Net2))
        cases += 1
    prove([H == 0, Bal == 0, S == 0, Net == 0], And(H == Bal, S == Net))  # base case
    return cases


# ------------------------------------------------------------------- Part 3
def part3(max_a=5):
    perms = 0
    for a in range(1, max_a + 1):
        labels = [Bool(f"x{i}") for i in range(a)]
        for pi in itertools.permutations(range(a)):
            pushed = [labels[pi[i]] for i in range(a)]
            same = And([pushed[i] == labels[i] for i in range(a)])
            s = Solver()
            s.add(Not(same))
            changed_possible = s.check() == sat
            assert changed_possible == (list(pi) != list(range(a))), f"push order {pi}"
            perms += 1
    return perms


MUTANTS = [  # (name, text in sent_step, replacement, count)
    ("read returns wrong offset", "s = ss[-1]; route = [(s, r - a, a)]", "s = ss[-1]; route = [(s, r - a + 1, a)]", 1),
    ("pop forgets to shrink the block", "        rr[-1] = r - a\n", "        pass\n", 1),
    ("multi-block pieces not reversed", "        acc.reverse()\n", "        pass\n", 1),
    ("pop deletes one block too many", "del ss[i + 1:], rr[i + 1:]", "del ss[i:], rr[i:]", 1),
    ("partial piece takes the wrong units", "acc.append((ss[i], r - need, need))", "acc.append((ss[i], 0, need))", 1),
    ("read modifies the stack", "                if not internal:\n                    rr[i] = r - need", "                if True:\n                    rr[i] = r - need", 1),
    ("exact pop keeps the id", "        s = ss.pop(); route = [(s, 0, a)]", "        s = ss[-1]; route = [(s, 0, a)]", 1),
    ("multi-block pop trims sizes only", "            del ss[i + 1:], rr[i + 1:]", "            del rr[i + 1:]", 1),
    ("multi-block read trims sizes", "        if not internal:\n            del ss[i + 1:], rr[i + 1:]", "        if True:\n            del rr[i + 1:]", 1),
]


def self_test():
    """Each injected bug must be refuted by Z3 with a counterexample."""
    import inspect
    src = inspect.getsource(sent_step)
    caught = 0
    for name, old, new, cnt in MUTANTS:
        assert old in src, name
        ns = {}
        exec(src.replace(old, new, cnt).replace("def sent_step", "def mutant"), globals(), ns)
        refuted = False
        for k in range(1, 5):
            for internal in (False, True):
                try:
                    explore_sent(k, internal, step=ns["mutant"])
                except AssertionError:
                    refuted = True
                    break
            if refuted:
                break
        print(f"  mutant '{name}': {'refuted by a counterexample' if refuted else 'NOT refuted'}")
        caught += refuted
    print(f"Self-test: {caught}/{len(MUTANTS)} injected bugs refuted (the proofs are not vacuous)")
    return caught == len(MUTANTS)


def fidelity(n_ledgers=3000, seed=7):
    """The transcription must compute exactly what the shipped sweeps compute.  Run it on
    concrete random ledgers and clusters (br() then just returns the concrete comparison)
    and compare every route with lifo.sweep and, when Numba is installed, with both
    compiled code paths: lifo_nb._sweep (used by regroup) and the routes lifo_nb.Engine
    builds in one pass over all clusters.  Together with Part 1 this ties the proof to the
    shipped code.  Returns (routes compared, compiled code compared too)."""
    import os
    import random
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    from lifo import sweep
    from test_engine import random_ledger, random_partition
    try:
        import numpy as np
        import lifo_nb
    except ImportError:
        lifo_nb = None
    rng = random.Random(seed)
    routes = 0
    for _ in range(n_ledgers):
        L = random_ledger(rng, rng.randint(2, 8), rng.randint(5, 120), n_labels=2,
                          max_amt=rng.choice([3, 9, 50]))
        part = random_partition(rng, L.wallets(), 0.5)
        if lifo_nb:
            A = lifo_nb.LedgerArrays(L)
            mask = np.zeros(L.W, np.bool_)
            mark = np.zeros(L.n, np.bool_)
            slot = np.full(L.n, -1, np.int64)
            built = lifo_nb.Engine(L, part).route
        for block in part:
            members = frozenset(block)
            real, _, _ = sweep(L, members)
            if lifo_nb:
                tx, ptr, pcs, *_ = lifo_nb._sweep(np.array(sorted(members), np.int64), mask,
                                                  A.wptr, A.widx, A.src, A.dst, A.amt, mark, slot)
                compiled = {int(t): tuple(pcs[ptr[i]:ptr[i + 1]].ravel().tolist())
                            for i, t in enumerate(tx)}
            ss, rr = [], []
            ev = sorted({t for w in members for t in L.wev[w]})
            for t in ev:
                if L.src[t] not in members:
                    inflow_step(ss, rr, t, L.amt[t])
                    continue
                pieces = sent_step(ss, rr, L.amt[t], L.dst[t] in members, lambda c: c)
                flat = tuple(x for piece in pieces for x in piece)
                assert flat == real[t], f"transcription differs from lifo.sweep at tx {t}"
                if lifo_nb:
                    assert flat == compiled[t], f"transcription differs from lifo_nb._sweep at tx {t}"
                    assert flat == built[t], f"transcription differs from lifo_nb's build at tx {t}"
                routes += 1
    return routes, lifo_nb is not None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--depth", type=int, default=12)
    a = ap.parse_args()
    t0 = time.time()
    if not self_test():
        sys.exit(1)
    n_routes, compiled = fidelity()
    also = " and the compiled lifo_nb sweep and build" if compiled else " (Numba not installed)"
    print(f"Fidelity: transcription equals lifo.sweep{also} on all {n_routes:,} routes of "
          f"3,000 random ledgers")
    paths = 0
    for k in range(0, a.depth + 1):
        n_push = check_push(k)
        n_ext = explore_sent(k, internal=False) if k else 0
        n_int = explore_sent(k, internal=True) if k else 0
        paths += n_push + n_ext + n_int
        print(f"Part 1  depth {k}: push 1 path, pop {n_ext} paths, read {n_int} paths - all proved")
        sys.stdout.flush()
    print(f"Part 1  T6: {paths} feasible paths of the sweep's event code proved, stacks of 0-{a.depth} "
          f"blocks, all inflow ids, block sizes and amounts")
    print(f"Part 2  T1, T5: induction-step arithmetic proved for all {part2()} transaction kinds, plus the base case")
    print(f"Part 3  T3: {part3()} push orders checked for a = 1..5; only the order-preserving push "
          f"leaves every label sequence unchanged")
    print(f"all SMT obligations discharged by Z3 in {time.time() - t0:.0f} s")


if __name__ == "__main__":
    main()
