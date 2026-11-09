"""Per-cluster LIFO sweep for Spark.

Rows arrive sorted by (cluster, seq), one partition at a time. One generator per partition
keeps the current cluster's block stack across Arrow batches, so a task never holds more
than one cluster's stack. The branches are those of lifo.sweep in lifo_recluster: a block
(s, r) holds the bottom r units of inflow s, and an outflow or internal read of a units
takes the top a units, bottom first. Theorem 4 (routing locality) is what makes the
partitioning by cluster exact.
"""
import numpy as np
import pandas as pd
from numba import njit

IN, OUT, READ = 0, 1, 2
NO_CLUSTER = np.iinfo(np.int64).min  # singletons use ids -(wallet + 1), so this never occurs

PIECES_SCHEMA = "cluster long, t long, k int, s long, offset long, length long, base long"


@njit(cache=True)
def _grow(a, n):
    b = np.empty(max(2 * a.shape[0], n), a.dtype)
    b[: a.shape[0]] = a
    return b


@njit(cache=True)
def _sweep_batch(cl, seq, kind, amt, cur, st_s, st_r, top):
    """Sweep one batch. Returns the pieces it emits and the stack state to carry forward."""
    cap = 2 * cl.shape[0] + 16
    o_c = np.empty(cap, np.int64)
    o_t = np.empty(cap, np.int64)
    o_k = np.empty(cap, np.int32)
    o_s = np.empty(cap, np.int64)
    o_off = np.empty(cap, np.int64)
    o_len = np.empty(cap, np.int64)
    o_base = np.empty(cap, np.int64)
    m = 0
    for j in range(cl.shape[0]):
        if cl[j] != cur:                      # a new cluster: its stack starts empty
            cur = cl[j]
            top = 0
        t = seq[j]
        a = amt[j]
        if kind[j] == IN:                     # inflow: push one block
            if top == st_s.shape[0]:
                st_s = _grow(st_s, top + 1)
                st_r = _grow(st_r, top + 1)
            st_s[top] = t
            st_r[top] = a
            top += 1
            continue
        first = m                             # outflow or internal read: take the top a units
        need = a
        i = top - 1
        while need > 0:
            if i < 0:
                raise ValueError("overdraft: a cluster pays out more than it holds")
            if m == o_t.shape[0]:
                o_c = _grow(o_c, m + 1)
                o_t = _grow(o_t, m + 1)
                o_k = _grow(o_k, m + 1)
                o_s = _grow(o_s, m + 1)
                o_off = _grow(o_off, m + 1)
                o_len = _grow(o_len, m + 1)
                o_base = _grow(o_base, m + 1)
            r = st_r[i]
            o_c[m] = cur
            o_t[m] = t
            o_s[m] = st_s[i]
            if r <= need:                     # the whole block
                o_off[m] = 0
                o_len[m] = r
                need -= r
                i -= 1
            else:                             # the top `need` units of block i
                o_off[m] = r - need
                o_len[m] = need
                if kind[j] == OUT:
                    st_r[i] = r - need
                need = 0
            m += 1
        if kind[j] == OUT:                    # an outflow removes what it took
            top = i + 1
        lo, hi = first, m - 1                 # pieces were taken top-down: store bottom first
        while lo < hi:
            o_s[lo], o_s[hi] = o_s[hi], o_s[lo]
            o_off[lo], o_off[hi] = o_off[hi], o_off[lo]
            o_len[lo], o_len[hi] = o_len[hi], o_len[lo]
            lo += 1
            hi -= 1
        base = 0
        for q in range(first, m):             # piece number and offset within t's own units
            o_k[q] = q - first
            o_base[q] = base
            base += o_len[q]
    return (o_c[:m], o_t[:m], o_k[:m], o_s[:m], o_off[:m], o_len[:m], o_base[:m],
            cur, st_s, st_r, top)


def sweep_partitions(batches):
    """mapInPandas body: (cluster, seq, kind, amount) sorted by (cluster, seq) -> pieces."""
    cur, st_s, st_r, top = NO_CLUSTER, np.empty(1024, np.int64), np.empty(1024, np.int64), 0
    for pdf in batches:
        c, t, k, s, off, ln, base, cur, st_s, st_r, top = _sweep_batch(
            pdf["cluster"].to_numpy(np.int64), pdf["seq"].to_numpy(np.int64),
            pdf["kind"].to_numpy(np.int8), pdf["amount"].to_numpy(np.int64),
            cur, st_s, st_r, top)
        if len(t):
            yield pd.DataFrame({"cluster": c, "t": t, "k": k, "s": s,
                                "offset": off, "length": ln, "base": base})
