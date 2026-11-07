"""Mutation test: inject bugs into the engine one at a time and check that the test
harnesses catch every one.

python3 mutate.py [--exhaustive] [--nb]

Default: 11 bugs in lifo.py, each must be caught by test_engine.py (and, with
--exhaustive, by verify/exhaustive.py on its quick scopes).  --nb: 15 bugs in the
compiled engine lifo_nb.py instead, the same 11 kinds plus four in code it alone has
(the one-pass build, arena compaction, the heap's seen marks, the compiled fresh trace).
Popping the top units on an internal read and pushing them straight back is not among
them: by Theorem 3 that is an equivalent mutant, and no test can or should catch it.
"""
import importlib
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
COMPILED = "--nb" in sys.argv

MUTS_PY = [
    ("readers not dropped", "                del readers[s]", "                pass"),
    ("readers not rebuilt", "            readers.update(rd)\n            for t, p in r.items():", "            for t, p in r.items():"),
    ("holdings miss members", "            c = Cluster(m, sum(net[w] for w in m))\n            for w in m:\n                owner[w] = c\n            readers", "            c = Cluster(m, 0)\n            for w in m:\n                owner[w] = c\n            readers"),
    ("net flows not updated", "                    net[s] -= d; net[r] += d\n", "                    pass\n"),
    ("internal read pops", "        if r >= a and dst[t] in members:   # internal read inside the top block", "        if False:"),
    ("multi-block pop keeps blocks", "                del ss[i + 1:], rr[i + 1:]", "                pass"),
    ("stolen fast path wrong", "k = (new[1] if new[0] else 0)", "k = (new[1] if new[0] else 1)"),
    ("cutoff on changed labels", "            elif cutoff:\n                continue", "            if cutoff:\n                continue"),
    ("linear pass skipped", "                    t = heap[0] - 1", "                    t = L.n"),
    ("remainder dropped", "            if rest:\n                new.append(rest)", "            pass"),
    ("cached cluster's readers kept", "            else:\n                rd = entry[1]", "            else:\n                rd = {}"),
]

MUTS_NB = [
    ("readers not dropped", "        gone += dl[s]\n        dl[s] = 0", "        gone += 0"),
    ("readers not rebuilt", "            self.RD, self.ru = _put_readers(keys, rptr, rds, self.ds, self.dl, self.RD, self.ru)\n", ""),
    ("holdings miss members", "            self.hold[c] = _assign(ma, self.owner, c, self.net)", "            _assign(ma, self.owner, c, self.net); self.hold[c] = 0"),
    ("net flows not updated", "                net[s] -= d\n                net[r] += d\n", ""),
    ("internal read pops", "        if r >= a and internal:               # internal read inside the top block", "        if False:"),
    ("multi-block pop keeps blocks", "            if not internal:\n                top = i + 1", "            if False:\n                top = i + 1"),
    ("stolen count wrong", "            st += buf[i, 1]", "            st += 1"),
    ("cutoff on changed labels", "        elif cutoff:\n            continue", "        if cutoff:\n            continue"),
    ("linear pass skipped", "                t = heap[0] - 1", "                t = n"),
    ("remainder dropped", "            if rest:\n                new.append(rest)", "            pass"),
    ("cached cluster's readers kept", [
        ("            entry = self.cache.get(m)\n            if entry is None:  # the outgoing",
         "            entry = self.cache.get(m)\n            hit = entry is not None\n            if entry is None:  # the outgoing"),
        ("            keys = entry[3]\n", "            keys = entry[3][:0] if hit else entry[3]\n")]),
    ("build: multi-block pop keeps blocks", "                if not internal:\n                    head[cs] = x", "                if False:\n                    head[cs] = x"),
    ("compaction drops a row", "            B[pos:pos + c] = A[starts[t]:starts[t] + c]", "            B[pos:pos + c - 1] = A[starts[t]:starts[t] + c - 1]"),
    ("seen marks not cleared", "    for i in range(pn):\n        seen[pushed[i]] = False", "    for i in range(0):\n        seen[pushed[i]] = False"),
    # (popping on an internal read and pushing straight back is an equivalent mutant,
    #  Theorem 3; reversing the push order is not)
    ("compiled fresh trace pushes reversed", "        for i in range(k):                    # push in the same order, coalescing", "        for i in range(k - 1, -1, -1):"),
]


def mutant(src, change):
    for a, b in ([change] if isinstance(change, tuple) else change):
        assert a in src, a
        src = src.replace(a, b)
    return src


def load(name, src):
    mod = types.ModuleType(name)
    mod.__file__ = os.path.join(HERE, name + "_mutant.py")
    exec(compile(src, name + "_mutant", "exec"), mod.__dict__)
    sys.modules[name] = mod
    return mod


def main():
    name = "lifo_nb" if COMPILED else "lifo"
    src = open(os.path.join(HERE, name + ".py")).read()
    if COMPILED:
        # mutants cannot use the disk cache; bounds checks turn a stray index into an
        # IndexError instead of silent memory corruption or a crash
        src = src.replace("@njit(cache=True)", "@njit(boundscheck=True)")
    muts = [(m[0], (m[1], m[2]) if len(m) == 3 else m[1]) for m in (MUTS_NB if COMPILED else MUTS_PY)]
    original = importlib.import_module(name)
    import test_engine as T0
    caught = 0
    for label, change in muts:
        load(name, mutant(src, change))
        T = importlib.reload(T0)
        try:
            T.run(300, 3, compiled=COMPILED)
            T.validation()
            print(f"{label:<38} NOT caught")
        except Exception as e:
            caught += 1
            print(f"{label:<38} caught: {type(e).__name__}: {str(e)[:60]}")
    sys.modules[name] = original
    print(f"{caught}/{len(muts)} injected bugs caught by test_engine.py")

    if "--exhaustive" in sys.argv:  # the same bugs against verify/exhaustive.py (quick scopes)
        sys.path.insert(0, os.path.join(HERE, "verify"))
        if COMPILED and "--nb" not in sys.argv:
            sys.argv.append("--nb")
        import exhaustive as X0
        small = X0.scope("S1", True)[1] + X0.scope("S2", True)[1]
        caught = 0
        for label, change in muts:
            load(name, mutant(src, change))
            X = importlib.reload(X0)
            err = None
            for tx in small:
                try:
                    X.check_ledger(tx)
                except Exception as e:
                    err = f"{type(e).__name__}: {str(e)[:50]}"
                    break
            caught += err is not None
            print(f"{label:<38} {'caught: ' + err if err else 'NOT caught'}")
        print(f"{caught}/{len(muts)} injected bugs caught by verify/exhaustive.py")


if __name__ == "__main__":
    main()
