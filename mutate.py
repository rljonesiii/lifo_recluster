"""Mutation test: inject 11 bugs into lifo.py one at a time and check that test_engine.py
catches every one; with --exhaustive, also verify/exhaustive.py on its quick scopes.
python3 mutate.py [--exhaustive]
"""
import os, sys, importlib, types
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lifo.py')).read()
muts = [
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
import test_engine as T0
caught = 0
for name, a, b in muts:
    assert a in src, name
    mod = types.ModuleType('lifo'); exec(compile(src.replace(a, b), 'lifo_mut', 'exec'), mod.__dict__)
    sys.modules['lifo'] = mod
    T = importlib.reload(T0)
    try:
        T.validation(); T.run(300, 3); print(f"{name:<30} NOT caught")
    except Exception as e:
        caught += 1
        print(f"{name:<30} caught: {type(e).__name__}: {str(e)[:60]}")
print(f"{caught}/{len(muts)} injected bugs caught by test_engine.py")

if "--exhaustive" in sys.argv:  # the same bugs against verify/exhaustive.py (quick scopes)
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "verify"))
    import exhaustive as X0
    small = X0.scope("S1", True)[1] + X0.scope("S2", True)[1]
    caught = 0
    for name, a, b in muts:
        mod = types.ModuleType('lifo'); exec(compile(src.replace(a, b), 'lifo_mut', 'exec'), mod.__dict__)
        sys.modules['lifo'] = mod
        X = importlib.reload(X0)
        err = None
        for tx in small:
            try:
                X.check_ledger(tx)
            except Exception as e:
                err = f"{type(e).__name__}: {str(e)[:50]}"
                break
        caught += err is not None
        print(f"{name:<30} {'caught: ' + err if err else 'NOT caught'}")
    print(f"{caught}/{len(muts)} injected bugs caught by verify/exhaustive.py")
