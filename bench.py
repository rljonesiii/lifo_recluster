"""Scaling benchmark: incremental reclustering vs fresh full LIFO trace.

python3 bench.py N [--seed S] [--mem] [--ablate] [--out results.json]

For one synthetic network of N transactions, builds the engine, then applies a fixed
script of reclustering operations.  After every operation it times the incremental
update, times a fresh trace under the new clustering, and checks that per-transaction
attribution and per-cluster holdings are identical.  --mem repeats the script with
tracemalloc to record peak memory per operation (timings come from the untraced run).
--ablate repeats the script twice more: routing cache off, and equality cutoff off.
"""
import argparse
import gc
import json
import os
import random
import time
import tracemalloc

import psutil

from itertools import chain

from gen import generate, USER, HACKER
from lifo import Ledger, Engine, fresh_trace, stolen_of, _label_from_pieces


def rss_mb():
    return psutil.Process(os.getpid()).memory_info().rss / 2**20


# ------------------------------------------------------------- choosing operations
def balance_intervals(L, w):
    out = []
    b = 0
    start = None
    for t in L.wev[w]:
        if L.dst[t] == w:
            if b == 0:
                start = t
            b += L.amt[t]
        else:
            b -= L.amt[t]
            if b == 0:
                out.append((start, t))
    if b:
        out.append((start, L.n))
    return out


def last_inflows(L, w):
    """(time, balance-after) of each event of w, for interaction checks."""
    out = []
    b = 0
    last_in = -1
    for t in L.wev[w]:
        if L.dst[t] == w:
            b += L.amt[t]; last_in = t
        else:
            b -= L.amt[t]
        out.append((t, b, last_in))
    return out


def interacts(L, u, v):
    """Heuristic: x spends at a time when y holds funds deposited after x's latest
    deposit, so in a merged stack x's spend would reach y's funds first."""
    hu, hv = last_inflows(L, u), last_inflows(L, v)
    for hx, hy, x in ((hu, hv, u), (hv, hu, v)):
        j = 0; by = 0; ly = -1
        for t, bx, lx in hx:
            while j < len(hy) and hy[j][0] < t:
                by, ly = hy[j][1], hy[j][2]; j += 1
            if L.src[t] == x and by > 0 and ly > lx:
                return True
    return False


def pick_ops(L, eng, meta, seed):
    rng = random.Random(seed)
    n = L.n
    role = meta["role"]
    owner = eng.owner
    labels = eng.labels
    stolen_in = {}
    lab_in = {}
    for t in range(n):
        s = stolen_of(labels[t])
        if s and L.source_label[L.src[t]] < 0 and owner[L.src[t]] is not owner[L.dst[t]] or \
                s and L.source_label[L.src[t]] >= 0:
            d = L.dst[t]
            stolen_in[d] = stolen_in.get(d, 0) + s
            r = labels[t]
            for i in range(0, len(r), 2):
                if r[i]:
                    lab_in.setdefault(d, set()).add(r[i])
    single = [w for w in range(L.W) if owner[w] is not None and len(owner[w].members) == 1
              and role[w] in (USER, HACKER) and w in stolen_in]
    ops = []

    # 1. nested excursion (the toy's B+E pattern): v's whole life sits inside u's
    two = sorted((L.wev[w][0], L.wev[w][1], w) for w in single if len(L.wev[w]) == 2
                 and L.dst[L.wev[w][0]] == w and L.amt[L.wev[w][0]] == L.amt[L.wev[w][1]]
                 and n // 3 < L.wev[w][0] < 2 * n // 3)
    starts = [x[0] for x in two]
    from bisect import bisect_right
    pair = None
    for a, b, u in two:
        j = bisect_right(starts, a)
        while j < len(two) and two[j][0] < b:
            if two[j][1] < b and two[j][2] != u:
                pair = (u, two[j][2]); break
            j += 1
        if pair:
            break
    ops.append(("nested-excursion merge (no-op)", "merge", list(pair)))

    # 2. late, local, interacting merge
    late = [w for w in single if L.wev[w][0] > int(0.95 * n) and len(L.wev[w]) >= 2]
    rng.shuffle(late)
    pair2 = None
    for i, u in enumerate(late[:300]):
        for v in late[i + 1:i + 60]:
            if interacts(L, u, v):
                pair2 = (u, v); break
        if pair2:
            break
    ops.append(("late local merge", "merge", list(pair2)))

    # 3. entity discovery: a hidden change address joins its hacker entity
    hid = [w for w in meta["hidden"] if w in stolen_in
           and 0.2 * n < L.wev[w][0] < 0.8 * n and len(L.wev[w]) >= 2]
    hid.sort(key=lambda w: -stolen_in[w])
    w3 = None
    for w in hid:
        mates = [x for x in meta["by_entity"][meta["entity"][w]] if x not in meta["hidden"]]
        if mates and interacts(L, w, mates[0]):
            w3 = (mates[0], w); break
    if w3 is None:
        w3 = (next(x for x in meta["by_entity"][meta["entity"][hid[0]]] if x != hid[0]), hid[0])
    ops.append(("entity discovery merge", "merge", list(w3)))

    # 4. root merge: two early, large holders of hack-1 funds (near the hack source)
    h1 = L.wev[meta["hack_wallets"][0]][0]
    early = [w for w in single if lab_in.get(w, set()) & {1} and h1 < L.wev[w][0] < h1 + 0.05 * n
             and len(L.wev[w]) >= 2]
    early.sort(key=lambda w: -stolen_in[w])
    pair4 = None
    for i, u in enumerate(early[:150]):
        for v in early[i + 1:150]:
            if interacts(L, u, v):
                pair4 = (u, v); break
        if pair4:
            break
    ops.append(("root merge (large early hack-1 holders)", "merge", list(pair4)))

    # 5. merge across distant branches: a hack-1 wallet and a hack-2 wallet, mid-ledger
    mid = [w for w in single if 0.45 * n < L.wev[w][0] < 0.55 * n and len(L.wev[w]) >= 3]
    a1 = [w for w in mid if lab_in.get(w) == {1}]
    a2 = [w for w in mid if lab_in.get(w) == {2}]
    rng.shuffle(a1); rng.shuffle(a2)
    pair5 = None
    for u in a1[:200]:
        for v in a2[:200]:
            if interacts(L, u, v):
                pair5 = (u, v); break
        if pair5:
            break
    ops.append(("distant-branch merge (hack 1 x hack 2)", "merge", list(pair5)))

    # 6. a busy laundering wallet turns out to belong to exchange 0
    ex0 = meta["exchanges"][0]
    busy = max((w for w in single if role[w] == HACKER), key=lambda w: len(L.wev[w]))
    ops.append(("absorb into exchange cluster", "merge", [ex0["hot"], busy]))

    # 7. overlapping merge: the root-merge cluster joins the distant-branch wallet
    ops.append(("overlapping merge (4 + 5a)", "merge", [pair4[0], pair5[0]]))

    # 8. split reversing part of op 4 while op 7 stands
    ops.append(("split v4 out of (4+5a)", "split", [pair4[1]]))

    # 9. split exchange 1 in half (large cluster)
    ex1 = meta["exchanges"][1]
    deps = [w for w in ex1["deposits"] if owner[w] is owner[ex1["hot"]]]
    half = deps[: len(deps) // 2]
    ops.append(("split exchange 1 in half", "split", half))

    # 10. two exchanges turn out to be one entity (large x large merge)
    ex2, ex3 = meta["exchanges"][2], meta["exchanges"][3]
    ops.append(("merge exchanges 2 + 3", "merge", [ex2["hot"], ex3["hot"]]))

    # 11. undo op 2 exactly
    ops.append(("undo late local merge", "split", [pair2[1]]))

    # 12. re-merge the exchange halves
    ops.append(("re-merge exchange 1", "merge", [ex1["hot"], half[0]]))

    # 13. undo the exchange merge
    ex3_members = sorted(owner[ex3["hot"]].members)  # exactly the original cluster
    ops.append(("undo exchange 2 + 3 merge", "split", ex3_members))

    # 14. global change: all four exchanges are one entity
    ops.append(("merge all four exchanges", "merge", [ex["hot"] for ex in meta["exchanges"]]))
    return ops


# ------------------------------------------------------------------------ running
def run_script(L, clusters, ops, mem=False, verify=True, **engine_kw):
    gc.collect()
    r0 = rss_mb()
    t0 = time.perf_counter()
    eng = Engine(L, clusters, **engine_kw)
    build = time.perf_counter() - t0
    gc.collect()
    eng_mb = rss_mb() - r0
    eng.reset_stats()
    rows = []
    if mem:
        tracemalloc.start()
    for name, kind, ws in ops:
        gc.collect()
        if mem:
            tracemalloc.reset_peak()
            base = tracemalloc.get_traced_memory()[0]
        t0 = time.perf_counter()
        if kind == "merge":
            eng.merge(ws)
        else:
            eng.split(ws)
        inc = time.perf_counter() - t0
        st = eng.reset_stats()
        row = dict(op=name, kind=kind, inc_s=inc, **st)
        if mem:
            row["inc_peak_mb"] = (tracemalloc.get_traced_memory()[1] - base) / 2**20
        co, hold = eng.cluster_of()
        gc.collect()
        if mem:
            tracemalloc.reset_peak()
            base = tracemalloc.get_traced_memory()[0]
        t0 = time.perf_counter()
        out, fhold = fresh_trace(L, co)
        row["fresh_s"] = time.perf_counter() - t0
        if mem:
            row["fresh_peak_mb"] = (tracemalloc.get_traced_memory()[1] - base) / 2**20
        if verify:
            ok = out == eng.labels and all(fhold.get(k, 0) == v for k, v in hold.items()) \
                and all(hold.get(k, 0) == v for k, v in fhold.items())
            row["exact"] = ok
            assert ok, f"mismatch after {name}"
        del out, fhold
        rows.append(row)
    if mem:
        tracemalloc.stop()
    return dict(build_s=build, engine_mb=eng_mb, rows=rows), eng


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("n", type=int)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--mem", action="store_true")
    ap.add_argument("--ablate", action="store_true")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    t0 = time.perf_counter()
    (src, dst, amt, sources, W), clusters, meta = generate(a.n, seed=a.seed)
    gen_s = time.perf_counter() - t0
    r0 = rss_mb()
    L = Ledger(src, dst, amt, sources, W)
    ledger_mb = rss_mb() - r0
    exw = {ex["hot"] for ex in meta["exchanges"]}
    for ex in meta["exchanges"]:
        exw.update(ex["deposits"])

    # network statistics under the initial clustering, and the operation script
    eng0 = Engine(L, clusters)
    ops = pick_ops(L, eng0, meta, a.seed)
    cl = eng0.clusters()
    sizes = sorted(len(set(chain.from_iterable(L.wev[w] for w in c.members))) for c in cl)
    big = sorted(len(c.members) for c in cl)[-4:]
    stolen_tx = sum(1 for x in eng0.labels if stolen_of(x))
    co0 = eng0.cluster_of()[0]
    internal = sum(1 for t in range(L.n) if co0[src[t]] is not None and co0[src[t]] == co0[dst[t]])
    depth = [0] * L.n  # stolen hop depth: transactions since the stolen source
    for t in range(L.n):
        if L.source_label[src[t]] >= 0:
            depth[t] = 1 if L.source_label[src[t]] else 0
            continue
        p = eng0.route[t]; best = 0
        for i in range(0, len(p), 3):
            if depth[p[i]] > best and stolen_of(_label_from_pieces(p[i:i + 3], eng0.labels)):
                best = depth[p[i]]
        depth[t] = best + 1 if best else 0
    ds = sorted(d for d in depth if d)
    to_ex = [depth[t] for t in range(L.n) if dst[t] in exw and src[t] not in exw and depth[t]]
    net = dict(n=L.n, wallets=W, clusters=len(sizes), largest_cluster_members=big,
               largest_cluster_events=sizes[-4:], internal_tx=internal,
               stolen_carrying_tx=stolen_tx, max_hops=ds[-1], median_hops=ds[len(ds) // 2],
               p90_hops=ds[int(0.9 * len(ds))], stolen_exchange_deposits=len(to_ex),
               exchange_deposits_ge20_hops=sum(1 for d in to_ex if d >= 20),
               gen_s=gen_s, ledger_mb=ledger_mb)
    del eng0, depth, cl
    gc.collect()
    print(json.dumps(net))

    res, eng = run_script(L, clusters, ops)
    del eng
    gc.collect()
    result = dict(network=net, ops=[(o[0], o[1], len(o[2])) for o in ops], main=res)
    for r in res["rows"]:
        print(f"{r['op']:<42} inc {r['inc_s']*1000:9.1f} ms  fresh {r['fresh_s']*1000:9.1f} ms  "
              f"x{r['fresh_s']/max(r['inc_s'],1e-9):8.1f}  swept {r['swept']:>7} "
              f"routes {r['route_changed']:>6} relab {r['relabeled']:>7} changed {r['label_changed']:>7} "
              f"cache {r['cache_hits']} linear {r['linear_pass']} ok={r['exact']}")
    print(f"build {res['build_s']:.2f}s engine {res['engine_mb']:.0f} MB")
    if a.mem:
        mres, eng = run_script(L, clusters, ops, mem=True, verify=False)
        del eng
        gc.collect()
        result["mem"] = mres
        for r in mres["rows"]:
            print(f"  mem {r['op']:<42} inc peak {r['inc_peak_mb']:8.2f} MB   fresh peak {r['fresh_peak_mb']:8.2f} MB")
    if a.ablate:
        for key, kw in (("no_cache", dict(cache_budget=0)), ("no_cutoff", dict(cutoff=False))):
            ares, eng = run_script(L, clusters, ops, **kw)
            del eng
            gc.collect()
            result[key] = ares
            for r in ares["rows"]:
                print(f"  {key:<9} {r['op']:<42} inc {r['inc_s']*1000:9.1f} ms swept {r['swept']:>7} "
                      f"relab {r['relabeled']:>7} linear {r['linear_pass']} ok={r['exact']}")
    if a.out:
        with open(a.out, "w") as f:
            json.dump(result, f, indent=1)


if __name__ == "__main__":
    main()
