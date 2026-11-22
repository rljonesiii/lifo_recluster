"""Per-cluster LIFO sweep for Spark.

Rows arrive sorted by (cluster, seq), one partition at a time. One generator per partition
keeps the current cluster's block stack across Arrow batches, so a task never holds more
than one cluster's stack. The branches are those of lifo.sweep in lifo_recluster: a block
(s, r) holds the bottom r units of inflow s, and an outflow or internal read of a units
takes the top a units, bottom first. Theorem 4 (routing locality) is what makes the
partitioning by cluster exact.

chunk_partitions is the same sweep on time chunks of giant clusters, each chunk from an
empty stack; lifo_spark.chunks settles what the chunks owe each other.
"""
import numpy as np
import pandas as pd
from numba import njit

IN, OUT, READ = 0, 1, 2
NO_CLUSTER = np.iinfo(np.int64).min  # singletons use ids -(wallet + 1), so this never occurs
NO_CHUNK = -1
PIECE, DEBT, BLOCK, SUM = 0, 1, 2, 3   # the records of a chunk sweep

PIECES_SCHEMA = "cluster long, t long, k int, s long, offset long, length long, base long"
CHUNKS_SCHEMA = ("rec byte, cluster long, chunk int, t long, k int, s long, offset long, "
                 "length long, base long")


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


@njit(cache=True)
def _put(out, m, rec, c, ch, t, k, s, off, ln, base):
    """Append one record (a row of CHUNKS_SCHEMA) to out, growing it when full."""
    if m == out.shape[0]:
        grown = np.empty((2 * m + 16, out.shape[1]), out.dtype)
        grown[:m] = out[:m]
        out = grown
    out[m, 0] = rec
    out[m, 1] = c
    out[m, 2] = ch
    out[m, 3] = t
    out[m, 4] = k
    out[m, 5] = s
    out[m, 6] = off
    out[m, 7] = ln
    out[m, 8] = base
    return out, m + 1


@njit(cache=True)
def _leftovers(out, m, c, ch, D, deep, st_s, st_r, top):
    """The end of a chunk: its leftover blocks, bottom up, each at heights [y, y + r) above
    the chunk's low-water mark, then its sum: D units popped below its start (offset), P
    units left (length), and the deepest debt below its start (base)."""
    y = 0
    for i in range(top):
        out, m = _put(out, m, BLOCK, c, ch, -1, -1, st_s[i], y, st_r[i], 0)
        y += st_r[i]
    return _put(out, m, SUM, c, ch, -1, -1, -1, D, y, deep)


@njit(cache=True)
def _chunk_batch(cl, ch, seq, kind, amt, cur, cur_ch, D, deep, st_s, st_r, top, final):
    """_sweep_batch on time chunks, each from an empty stack. A payment that runs its
    chunk's stack dry owes the rest to earlier chunks: a debt of `need` units at depths
    [D, D + need) below the chunk's start (offset D, length need), where D counts what the
    chunk's outflows have already popped below it. The debt is t's deepest units, so t's
    own pieces start at base `need`, and chunks.settle gives the debt's pieces bases below
    that. A chunk's leftovers and sum follow its last event; final flushes the last chunk."""
    out = np.empty((2 * cl.shape[0] + 16, 9), np.int64)
    m = 0
    for j in range(cl.shape[0]):
        if cl[j] != cur or ch[j] != cur_ch:   # a new chunk: its stack starts empty
            if cur_ch != NO_CHUNK:
                out, m = _leftovers(out, m, cur, cur_ch, D, deep, st_s, st_r, top)
            cur, cur_ch, top, D, deep = cl[j], ch[j], 0, 0, 0
        t = seq[j]
        if kind[j] == IN:
            if top == st_s.shape[0]:
                st_s = _grow(st_s, top + 1)
                st_r = _grow(st_r, top + 1)
            st_s[top] = t
            st_r[top] = amt[j]
            top += 1
            continue
        first = m
        need = amt[j]
        i = top - 1
        while need > 0 and i >= 0:
            r = st_r[i]
            if r <= need:                     # the whole block
                out, m = _put(out, m, PIECE, cur, cur_ch, t, 0, st_s[i], 0, r, 0)
                need -= r
                i -= 1
            else:                             # the top `need` units of block i
                out, m = _put(out, m, PIECE, cur, cur_ch, t, 0, st_s[i], r - need, need, 0)
                if kind[j] == OUT:
                    st_r[i] = r - need
                need = 0
        if kind[j] == OUT:
            top = i + 1
        lo, hi = first, m - 1                 # taken top-down: store bottom first
        while lo < hi:
            for col in (5, 6, 7):
                out[lo, col], out[hi, col] = out[hi, col], out[lo, col]
            lo += 1
            hi -= 1
        base = need
        for q in range(first, m):
            out[q, 4] = q - first
            out[q, 8] = base
            base += out[q, 7]
        if need > 0:
            out, m = _put(out, m, DEBT, cur, cur_ch, t, -1, -1, D, need, 0)
            deep = max(deep, D + need)
            if kind[j] == OUT:
                D += need
    if final and cur_ch != NO_CHUNK:
        out, m = _leftovers(out, m, cur, cur_ch, D, deep, st_s, st_r, top)
    return out[:m], cur, cur_ch, D, deep, st_s, st_r, top


CHUNK_COLUMNS = ("rec", "cluster", "chunk", "t", "k", "s", "offset", "length", "base")


def chunk_partitions(batches):
    """mapInPandas body: (cluster, chunk, seq, kind, amount) sorted by (cluster, chunk, seq)
    -> CHUNKS_SCHEMA records: pieces, debts, and each chunk's leftover blocks and sum."""
    frame = lambda rows: (pd.DataFrame(rows, columns=CHUNK_COLUMNS)
                            .astype({"rec": "int8", "chunk": "int32", "k": "int32"}))
    state = (NO_CLUSTER, NO_CHUNK, 0, 0, np.empty(1024, np.int64), np.empty(1024, np.int64), 0)
    for pdf in batches:
        rows, *state = _chunk_batch(
            pdf["cluster"].to_numpy(np.int64), pdf["chunk"].to_numpy(np.int64),
            pdf["seq"].to_numpy(np.int64), pdf["kind"].to_numpy(np.int8),
            pdf["amount"].to_numpy(np.int64), *state, False)
        if len(rows):
            yield frame(rows)
    none = np.empty(0, np.int64)
    rows, *_ = _chunk_batch(none, none, none, np.empty(0, np.int8), none, *state, True)
    if len(rows):
        yield frame(rows)
