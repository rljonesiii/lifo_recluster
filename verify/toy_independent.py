"""The challenge ledger traced by a separate implementation, written from the challenge's
rules alone (lots of (label, dollars), one stack per cluster, no shared code with the
package), then compared with the package's results.

python3 verify/toy_independent.py

Prints every transaction's attribution in the four states (baseline, +B-E, +B-E and
+D-F, then -B-E, which leaves D+F), the stolen amounts reaching X and Y, the stolen
holdings per cluster, and the same receipts with the push order reversed (FLIP).  The
last part compares each state's per-transaction labels and holdings with lifo.fresh_trace
and with both engines after the same merges and splits.
"""
LEDGER = [(1,"Stolen","A",100),(2,"A","B",60),(3,"A","C",40),(4,"Clean","B",40),
          (5,"B","D",70),(6,"C","E",40),(7,"D","F",50),(8,"E","F",40),(9,"Clean","F",30),
          (10,"F","G",80),(11,"D","H",20),(12,"G","I",50),(13,"H","I",20),(14,"I","X",60),
          (15,"G","Y",30)]
WALLETS = sorted({w for _,s,r,_ in LEDGER for w in (s,r) if w not in ("Stolen","Clean")})

def norm(lots):
    out = []
    for lab, a in lots:
        if a <= 0: continue
        if out and out[-1][0] == lab: out[-1] = (lab, out[-1][1] + a)
        else: out.append((lab, a))
    return out

def take_top(stack, amt, remove):
    """Return top `amt` dollars bottom-to-top; optionally remove from stack."""
    got, need, i = [], amt, len(stack) - 1
    while need > 0:
        if i < 0: raise ValueError("insufficient funds")
        lab, a = stack[i]; t = min(a, need)
        got.append((lab, t)); need -= t
        if remove:
            if t == a: stack.pop()
            else: stack[i] = (lab, a - t)
        i -= 1
    return list(reversed(got))  # bottom-to-top

def run(groups, flip=False):
    cl = {w: w for w in WALLETS}
    for g in groups:
        name = "+".join(g)
        for w in g: cl[w] = name
    stacks = {c: [] for c in set(cl.values())}
    tx = {}
    for t, s, r, amt in LEDGER:
        if s in ("Stolen", "Clean"):
            lot = [("stolen" if s == "Stolen" else "clean", amt)]
            stacks[cl[r]].extend(lot); stacks[cl[r]] = norm(stacks[cl[r]]); tx[t] = lot; continue
        cs, cr = cl[s], cl[r]
        if cs == cr:
            tx[t] = norm(take_top(stacks[cs], amt, False)); continue
        moved = take_top(stacks[cs], amt, True)
        tx[t] = norm(moved)
        stacks[cr].extend(list(reversed(moved)) if flip else moved)
        stacks[cr] = norm(stacks[cr])
    return cl, stacks, tx

stol = lambda lots: sum(a for l, a in lots if l == "stolen")
fmt = lambda lots: " ".join(f"{'S' if l=='stolen' else 'C'}{a}" for l, a in lots)

CASES = [("1 singletons", []), ("2 B+E", [("B","E")]),
         ("3 B+E, D+F", [("B","E"),("D","F")]), ("4 D+F", [("D","F")])]
for name, groups in CASES:
    cl, stacks, tx = run(groups)
    print(f"=== {name}")
    for t, s, r, amt in LEDGER:
        internal = s not in ("Stolen","Clean") and cl[s] == cl[r]
        print(f"  {t:2d} {s}->{r} {amt:3d}: stolen={stol(tx[t]):3d}  [{fmt(tx[t])}]{' (internal)' if internal else ''}")
    def recv(w, txs):
        return sum(stol(txs[t]) for t,s,r,_ in LEDGER if r == w and cl[s] != cl[r])
    print(f"  X={recv('X',tx)} Y={recv('Y',tx)}")
    print("  held stolen:", {c: stol(st) for c, st in sorted(stacks.items()) if stol(st)})
    print("  final stacks:", {c: fmt(st) for c, st in sorted(stacks.items()) if st})
    _, _, txf = run(groups, flip=True)
    print(f"  FLIP: X={recv('X',txf)} Y={recv('Y',txf)}")


# ------------------------------------------------- comparison with the package
import os  # noqa: E402
import sys  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import lifo  # noqa: E402
import toy  # noqa: E402
from engine import Engine  # noqa: E402

L = toy.ledger()
I = toy.IDX
engines = [lifo.Engine(L)]
if Engine is not lifo.Engine:
    engines.append(Engine(L))
ok = True
for k, (name, groups) in enumerate(CASES):
    if k == 1:
        for e in engines: e.merge([I["B"], I["E"]])
    if k == 2:
        for e in engines: e.merge([I["D"], I["F"]])
    if k == 3:  # -B-E: split B and E apart again, leaving D+F
        for e in engines: e.split([I["B"]])
    cl, stacks, tx = run(groups)
    mine = [tuple(x for lab, a in tx[t] for x in (1 if lab == "stolen" else 0, a)) for t, *_ in LEDGER]
    held = {frozenset(I[w] for w in c.split("+")): stol(st) for c, st in stacks.items() if stol(st)}
    co = [None] * L.W
    for w in WALLETS:
        co[I[w]] = cl[w]
    out, fhold = lifo.fresh_trace(L, co)
    same = out == mine and {frozenset(I[w] for w in c.split("+")): v for c, v in fhold.items() if v} == held
    for e in engines:
        same &= list(e.labels) == mine and {c.members: c.hold for c in e.clusters() if c.hold} == held
    ok &= same
    print(f"{name}: {'identical to' if same else 'DIFFERS from'} lifo.fresh_trace and "
          f"{' and '.join(type(e).__module__ + '.Engine' for e in engines)}")
if not ok:
    sys.exit(1)
