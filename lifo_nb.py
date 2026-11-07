"""
Exact LIFO tracing with incremental reclustering, compiled with Numba.

Same model, algorithm and results as lifo.Engine, the pure-Python reference and
fallback (see lifo.py for the model).  Here the state lives in NumPy arrays and every
loop over events or transactions runs as compiled code; Python keeps only the regroup
bookkeeping and the membership cache.

State
-----
  routes   arena P, rows (inflow, offset, length):  route of t = P[rs[t] : rs[t] + rl[t]]
  labels   arena LB, rows (label, units):           label of t = LB[ls[t] : ls[t] + ll[t]]
  readers  arena RD:                                readers of s = RD[ds[s] : ds[s] + dl[s]]
  stol[t]  stolen units t carries     net[w]  stolen received - sent by wallet w
  owner[w] cluster id (-1 for a source)   hold[cluster id]  stolen holdings
Arenas are append-only: an update writes new rows and repoints the transaction, and a
compaction reclaims dead rows once they outnumber the live ones.

Every kernel mirrors the corresponding code in lifo.py: _sweep has lifo.sweep's
branches, _propagate has Engine._propagate's heap, cutoff and linear pass, and
Engine.regroup does its steps in the same order, including the routing cache.
"""
from collections import OrderedDict

import numpy as np
from numba import njit

import math

import lifo
from lifo import Ledger, check_partition, check_wallets, stolen_of  # noqa: F401  (re-exported)

I64 = np.int64


# ------------------------------------------------------------------- the ledger
VOLUME_LIMIT = 2 ** 62


def fits(L):
    """Whether the compiled engine can hold this ledger exactly in int64.  Every quantity
    the kernels form (a block, a run, a piece, a stack, a net flow, a holding, or a partial
    sum of these) is bounded in absolute value by twice the ledger's total volume, so a
    volume below 2**62 keeps all of them inside int64.  Labels must fit in int64."""
    return sum(L.amt) < VOLUME_LIMIT and all(l < 2 ** 63 for l in L.source_label)


class LedgerArrays:
    """A validated lifo.Ledger as int64 arrays, with per-wallet events in CSR form."""

    def __init__(self, L):
        if not fits(L):
            raise ValueError("ledger too large for the compiled engine's int64 arithmetic "
                             "(total volume must be below 2**62 and labels below 2**63); "
                             "use lifo.Engine, which computes with Python integers")
        self.L = L
        self.n, self.W = L.n, L.W
        self.src = np.asarray(L.src, I64)
        self.dst = np.asarray(L.dst, I64)
        self.amt = np.asarray(L.amt, I64)
        self.slab = np.asarray(L.source_label, I64)
        lens = np.fromiter((len(e) for e in L.wev), I64, L.W)
        self.wptr = np.zeros(L.W + 1, I64)
        np.cumsum(lens, out=self.wptr[1:])
        self.widx = np.fromiter((t for e in L.wev for t in e), I64, int(self.wptr[-1]))
        self.wallets = np.flatnonzero(self.slab < 0).astype(I64)


# ------------------------------------------------------------- small helpers
@njit(cache=True)
def _grow2(A, need):
    B = np.empty((max(need, 2 * A.shape[0] + 16), A.shape[1]), A.dtype)
    B[:A.shape[0]] = A
    return B


@njit(cache=True)
def _grow1(A, need):
    B = np.empty(max(need, 2 * A.shape[0] + 16), A.dtype)
    B[:A.shape[0]] = A
    return B


@njit(cache=True)
def _events(members, wptr, widx, mark):
    """Sorted events of a cluster: the union of its wallets' event lists.  A large
    cluster marks its events in the scratch array `mark` and scans it once, which is
    linear in the ledger; a small one sorts.  `mark` is left all False."""
    tot = 0
    for w in members:
        tot += wptr[w + 1] - wptr[w]
    if members.shape[0] == 1:
        w = members[0]
        return widx[wptr[w]:wptr[w + 1]].copy()
    n = mark.shape[0]
    if 16 * tot > n:
        for w in members:
            for i in range(wptr[w], wptr[w + 1]):
                mark[widx[i]] = True
        out = np.empty(tot, I64)
        m = 0
        for t in range(n):
            if mark[t]:
                mark[t] = False
                out[m] = t
                m += 1
        return out[:m]
    buf = np.empty(tot, I64)
    k = 0
    for w in members:
        for i in range(wptr[w], wptr[w + 1]):
            buf[k] = widx[i]
            k += 1
    buf.sort()
    m = 0
    for i in range(tot):
        if i == 0 or buf[i] != buf[i - 1]:
            buf[m] = buf[i]
            m += 1
    return buf[:m]


@njit(cache=True)
def _invert(tx, ptr, pcs, slot):
    """Readers of a routing: inflow -> senders whose pieces point into it, in time order.
    Counting by inflow through the scratch array `slot` (all -1, and left so)."""
    k = pcs.shape[0]
    keys = np.empty(k, I64)
    cnt = np.zeros(k + 1, I64)
    nk = 0
    for q in range(k):
        s = pcs[q, 0]
        if slot[s] < 0:
            slot[s] = nk
            keys[nk] = s
            nk += 1
        cnt[slot[s]] += 1
    rptr = np.zeros(nk + 1, I64)
    for i in range(nk):
        rptr[i + 1] = rptr[i] + cnt[i]
        cnt[i] = rptr[i]
    rds = np.empty(k, I64)
    for i in range(tx.shape[0]):           # senders in time order
        for q in range(ptr[i], ptr[i + 1]):
            j = slot[pcs[q, 0]]
            rds[cnt[j]] = tx[i]
            cnt[j] += 1
    for i in range(nk):
        slot[keys[i]] = -1
    return keys[:nk].copy(), rptr, rds


# --------------------------------------------------------------------- the sweep
@njit(cache=True)
def _sweep(members, mask, wptr, widx, src, dst, amt, mark, slot):
    """Routing of one cluster, one LIFO pass over its events: lifo.sweep compiled.
    Returns the senders in time order, their pieces (CSR), the readers (CSR by inflow)
    and the number of events swept."""
    for w in members:
        mask[w] = True
    ev = _events(members, wptr, widx, mark)
    m = ev.shape[0]
    ss = np.empty(m, I64)   # inflow of each block, bottom to top
    rr = np.empty(m, I64)   # units left in each block (always its bottom units)
    top = 0
    tx = np.empty(m, I64)
    ptr = np.zeros(m + 1, I64)
    nt = 0
    pcs = np.empty((m + 16, 3), I64)
    k = 0
    for e in range(m):
        t = ev[e]
        a = amt[t]
        if not mask[src[t]]:                  # inflow: push
            ss[top] = t
            rr[top] = a
            top += 1
            continue
        if k + top > pcs.shape[0]:
            pcs = _grow2(pcs, k + top)
        r = rr[top - 1]
        internal = mask[dst[t]]
        if r >= a and internal:               # internal read inside the top block
            pcs[k, 0] = ss[top - 1]; pcs[k, 1] = r - a; pcs[k, 2] = a; k += 1
        elif r > a:                           # outflow inside the top block
            rr[top - 1] = r - a
            pcs[k, 0] = ss[top - 1]; pcs[k, 1] = r - a; pcs[k, 2] = a; k += 1
        elif r == a:                          # outflow takes exactly the top block
            top -= 1
            pcs[k, 0] = ss[top]; pcs[k, 1] = 0; pcs[k, 2] = a; k += 1
        else:                                 # several blocks: read or pop them
            need = a
            i = top - 1
            start = k
            while need:
                r = rr[i]
                if r <= need:
                    pcs[k, 0] = ss[i]; pcs[k, 1] = 0; pcs[k, 2] = r; k += 1
                    need -= r
                    i -= 1
                else:
                    pcs[k, 0] = ss[i]; pcs[k, 1] = r - need; pcs[k, 2] = need; k += 1
                    if not internal:
                        rr[i] = r - need
                    need = 0
            if not internal:
                top = i + 1
            lo = start
            hi = k - 1
            while lo < hi:                    # bottom first
                for c in range(3):
                    x = pcs[lo, c]; pcs[lo, c] = pcs[hi, c]; pcs[hi, c] = x
                lo += 1
                hi -= 1
        tx[nt] = t
        nt += 1
        ptr[nt] = k
    for w in members:
        mask[w] = False
    tx = tx[:nt].copy()
    ptr = ptr[:nt + 1].copy()
    pcs = pcs[:k].copy()
    keys, rptr, rds = _invert(tx, ptr, pcs, slot)
    return tx, ptr, pcs, keys, rptr, rds, m


# ------------------------------------------------------------------- labels
@njit(cache=True)
def _label_runs(t, rs, rl, P, ls, ll, LB, buf):
    """Labels carried by the pieces of t's route, read from the labels of the inflows
    they point into, with equal neighbours coalesced: lifo._label_from_pieces."""
    bound = 0
    for q in range(rs[t], rs[t] + rl[t]):
        bound += ll[P[q, 0]]
    if buf.shape[0] < bound:
        buf = np.empty((max(bound, 2 * buf.shape[0]), 2), I64)
    k = 0
    for q in range(rs[t], rs[t] + rl[t]):
        s = P[q, 0]
        off = P[q, 1]
        end = off + P[q, 2]
        j = ls[s]
        pos = 0
        while pos + LB[j, 1] <= off:
            pos += LB[j, 1]
            j += 1
        cur = off
        while cur < end:
            lab = LB[j, 0]
            rend = pos + LB[j, 1]
            take = (rend if rend < end else end) - cur
            if k > 0 and buf[k - 1, 0] == lab:
                buf[k - 1, 1] += take
            else:
                buf[k, 0] = lab
                buf[k, 1] = take
                k += 1
            cur += take
            pos = rend
            j += 1
    return k, buf


@njit(cache=True)
def _stolen(buf, k):
    st = 0
    for i in range(k):
        if buf[i, 0] != 0:
            st += buf[i, 1]
    return st


# --------------------------------------------------------------------- build
@njit(cache=True)
def _build(src, dst, amt, slab, owner, n_clusters):
    """Every cluster's routing in one pass over the ledger (each cluster's blocks in its
    own linked list, which equals sweeping the clusters one by one, Theorem 4), then the
    readers, then labels, stolen counts and net flows in time order."""
    n = src.shape[0]
    head = np.full(n_clusters, -1, I64)
    bs = np.empty(n + 1, I64)    # block inflow
    br = np.empty(n + 1, I64)    # units left
    bn = np.empty(n + 1, I64)    # next block down
    nb = 0
    rs = np.zeros(n, I64)
    rl = np.zeros(n, I64)
    P = np.empty((n + 16, 3), I64)
    pu = 0
    for t in range(n):
        s = src[t]
        d = dst[t]
        a = amt[t]
        cd = owner[d]
        internal = False
        if slab[s] < 0:
            cs = owner[s]
            internal = cs == cd
            h = head[cs]
            r = br[h]
            rs[t] = pu
            if pu + 1 > P.shape[0]:
                P = _grow2(P, pu + 1)
            if r >= a and internal:
                P[pu, 0] = bs[h]; P[pu, 1] = r - a; P[pu, 2] = a; pu += 1
            elif r > a:
                br[h] = r - a
                P[pu, 0] = bs[h]; P[pu, 1] = r - a; P[pu, 2] = a; pu += 1
            elif r == a:
                P[pu, 0] = bs[h]; P[pu, 1] = 0; P[pu, 2] = a; pu += 1
                head[cs] = bn[h]
            else:
                need = a
                x = h
                start = pu
                while need:
                    if pu + 1 > P.shape[0]:
                        P = _grow2(P, pu + 1)
                    r = br[x]
                    if r <= need:
                        P[pu, 0] = bs[x]; P[pu, 1] = 0; P[pu, 2] = r; pu += 1
                        need -= r
                        x = bn[x]
                    else:
                        P[pu, 0] = bs[x]; P[pu, 1] = r - need; P[pu, 2] = need; pu += 1
                        if not internal:
                            br[x] = r - need
                        need = 0
                if not internal:
                    head[cs] = x
                lo = start
                hi = pu - 1
                while lo < hi:
                    for c in range(3):
                        y = P[lo, c]; P[lo, c] = P[hi, c]; P[hi, c] = y
                    lo += 1
                    hi -= 1
            rl[t] = pu - rs[t]
        if not internal:                      # the receiver pushes t's units
            bs[nb] = t
            br[nb] = a
            bn[nb] = head[cd]
            head[cd] = nb
            nb += 1
    # readers, by counting sort on the inflow; senders end up in time order
    dl = np.zeros(n, I64)
    for q in range(pu):
        dl[P[q, 0]] += 1
    ds = np.zeros(n, I64)
    acc = 0
    for s in range(n):
        ds[s] = acc
        acc += dl[s]
    RD = np.empty(acc + 16, I64)
    fill = np.zeros(n, I64)
    for t in range(n):
        for q in range(rs[t], rs[t] + rl[t]):
            s = P[q, 0]
            RD[ds[s] + fill[s]] = t
            fill[s] += 1
    ru = acc
    # labels in time order: every input precedes its reader
    ls = np.zeros(n, I64)
    ll = np.zeros(n, I64)
    LB = np.empty((n + 16, 2), I64)
    lu = 0
    stol = np.zeros(n, I64)
    net = np.zeros(owner.shape[0], I64)
    buf = np.empty((64, 2), I64)
    for t in range(n):
        s = src[t]
        if slab[s] >= 0:
            k = 1
            buf[0, 0] = slab[s]
            buf[0, 1] = amt[t]
        else:
            k, buf = _label_runs(t, rs, rl, P, ls, ll, LB, buf)
        if lu + k > LB.shape[0]:
            LB = _grow2(LB, lu + k)
        LB[lu:lu + k] = buf[:k]
        ls[t] = lu
        ll[t] = k
        lu += k
        st = _stolen(buf, k)
        if st:
            stol[t] = st
            net[dst[t]] += st
            if slab[s] < 0:
                net[s] -= st
    return rs, rl, P, pu, ds, dl, RD, ru, ls, ll, LB, lu, stol, net, buf


# ------------------------------------------------------------- regroup kernels
@njit(cache=True)
def _snapshot(members, mask, wptr, widx, src, dst, rs, rl, P, ds, dl, RD, mark):
    """An outgoing cluster's routing and readers, copied from the current state: the
    routes of the transactions it sends and the readers of the inflows it received."""
    for w in members:
        mask[w] = True
    ev = _events(members, wptr, widx, mark)
    nt = 0; npc = 0; nk = 0; nrd = 0
    for t in ev:
        if mask[src[t]]:
            nt += 1
            npc += rl[t]
        elif dl[t] > 0:
            nk += 1
            nrd += dl[t]
    tx = np.empty(nt, I64); ptr = np.zeros(nt + 1, I64); pcs = np.empty((npc, 3), I64)
    keys = np.empty(nk, I64); rptr = np.zeros(nk + 1, I64); rds = np.empty(nrd, I64)
    i = 0; q = 0; j = 0; p = 0
    for t in ev:
        if mask[src[t]]:
            tx[i] = t
            pcs[q:q + rl[t]] = P[rs[t]:rs[t] + rl[t]]
            q += rl[t]
            i += 1
            ptr[i] = q
        elif dl[t] > 0:
            keys[j] = t
            rds[p:p + dl[t]] = RD[ds[t]:ds[t] + dl[t]]
            p += dl[t]
            j += 1
            rptr[j] = p
    for w in members:
        mask[w] = False
    return tx, ptr, pcs, keys, rptr, rds


@njit(cache=True)
def _install(tx, ptr, pcs, rs, rl, P, pu):
    """Install a new cluster's routes; every transaction whose route changed is a seed."""
    dirty = np.empty(tx.shape[0], I64)
    nd = 0
    dlive = 0
    for i in range(tx.shape[0]):
        t = tx[i]
        a = ptr[i]
        c = ptr[i + 1] - a
        same = c == rl[t]
        if same:
            o = rs[t]
            for j in range(c):
                if pcs[a + j, 0] != P[o + j, 0] or pcs[a + j, 1] != P[o + j, 1] \
                        or pcs[a + j, 2] != P[o + j, 2]:
                    same = False
                    break
        if not same:
            if pu + c > P.shape[0]:
                P = _grow2(P, pu + c)
            P[pu:pu + c] = pcs[a:a + c]
            dlive += c - rl[t]
            rs[t] = pu
            rl[t] = c
            pu += c
            dirty[nd] = t
            nd += 1
    return P, pu, dirty[:nd].copy(), dlive


@njit(cache=True)
def _owners(touched, owner, cmark):
    """Clusters containing the given wallets, in order of first appearance.  `cmark` is
    a scratch array over cluster ids (cluster ids never exceed the number of wallets),
    left all False."""
    out = np.empty(touched.shape[0], I64)
    m = 0
    for w in touched:
        c = owner[w]
        if not cmark[c]:
            cmark[c] = True
            out[m] = c
            m += 1
    for j in range(m):
        cmark[out[j]] = False
    return out[:m]


@njit(cache=True)
def _drop_readers(keys, dl):
    gone = 0
    for s in keys:
        gone += dl[s]
        dl[s] = 0
    return gone


@njit(cache=True)
def _put_readers(keys, rptr, rds, ds, dl, RD, ru):
    nr = rds.shape[0]
    if ru + nr > RD.shape[0]:
        RD = _grow1(RD, ru + nr)
    RD[ru:ru + nr] = rds
    for i in range(keys.shape[0]):
        ds[keys[i]] = ru + rptr[i]
        dl[keys[i]] = rptr[i + 1] - rptr[i]
    return RD, ru + nr


@njit(cache=True)
def _assign(ma, owner, c, net):
    tot = 0
    for w in ma:
        owner[w] = c
        tot += net[w]
    return tot


@njit(cache=True)
def _hpush(h, n, x):
    i = n
    h[i] = x
    while i > 0:
        p = (i - 1) >> 1
        if h[p] <= h[i]:
            break
        h[p], h[i] = h[i], h[p]
        i = p
    return n + 1


@njit(cache=True)
def _hpop(h, n):
    x = h[0]
    n -= 1
    h[0] = h[n]
    i = 0
    while True:
        l = 2 * i + 1
        if l >= n:
            break
        c = l
        if l + 1 < n and h[l + 1] < h[l]:
            c = l + 1
        if h[i] <= h[c]:
            break
        h[i], h[c] = h[c], h[i]
        i = c
    return x, n


@njit(cache=True)
def _propagate(seeds, rs, rl, P, ls, ll, LB, lu, ds, dl, RD, stol, net, owner, hold,
               src, dst, seen, limit, cutoff, buf):
    """Recompute labels in time order from the seeds: Engine._propagate compiled.
    Every input of a transaction precedes it, so each label is computed from final
    inputs; an unchanged label stops the wave there; past `limit` recomputations the
    rest is one linear pass."""
    n = rs.shape[0]
    cap = max(16, 2 * seeds.shape[0])
    heap = np.empty(cap, I64)
    hn = 0
    pushed = np.empty(cap, I64)
    pn = 0
    for x in seeds:                          # seeds are sorted, so already a heap
        heap[hn] = x
        hn += 1
        seen[x] = True
        pushed[pn] = x
        pn += 1
    relabeled = 0
    changed = 0
    linear = False
    dlive = 0
    t = -1
    while True:
        if linear:
            t += 1
            if t >= n:
                break
            if rl[t] == 0:
                continue
        else:
            if hn == 0:
                break
            if relabeled > limit:
                linear = True
                t = heap[0] - 1
                continue
            t, hn = _hpop(heap, hn)
        relabeled += 1
        k, buf = _label_runs(t, rs, rl, P, ls, ll, LB, buf)
        same = k == ll[t]
        if same:
            b = ls[t]
            for i in range(k):
                if buf[i, 0] != LB[b + i, 0] or buf[i, 1] != LB[b + i, 1]:
                    same = False
                    break
        if not same:
            changed += 1
            if lu + k > LB.shape[0]:
                LB = _grow2(LB, lu + k)
            LB[lu:lu + k] = buf[:k]
            dlive += k - ll[t]
            ls[t] = lu
            ll[t] = k
            lu += k
            d = _stolen(buf, k) - stol[t]
            if d != 0:                        # holdings follow the stolen units carried
                stol[t] += d
                s = src[t]
                r = dst[t]
                net[s] -= d
                net[r] += d
                hold[owner[s]] -= d
                hold[owner[r]] += d
        elif cutoff:
            continue
        if not linear:
            for q in range(ds[t], ds[t] + dl[t]):
                u = RD[q]
                if not seen[u]:
                    seen[u] = True
                    if hn == heap.shape[0]:
                        heap = _grow1(heap, hn + 1)
                    hn = _hpush(heap, hn, u)
                    if pn == pushed.shape[0]:
                        pushed = _grow1(pushed, pn + 1)
                    pushed[pn] = u
                    pn += 1
    for i in range(pn):
        seen[pushed[i]] = False
    return LB, lu, buf, relabeled, changed, linear, dlive


@njit(cache=True)
def _compact2(starts, lens, A):
    """Copy the live rows of an arena into a fresh one, in transaction order."""
    total = 0
    for t in range(lens.shape[0]):
        total += lens[t]
    B = np.empty((total + 16, A.shape[1]), A.dtype)
    pos = 0
    for t in range(lens.shape[0]):
        c = lens[t]
        if c:
            B[pos:pos + c] = A[starts[t]:starts[t] + c]
            starts[t] = pos
            pos += c
    return B, pos


@njit(cache=True)
def _compact1(starts, lens, A):
    total = 0
    for t in range(lens.shape[0]):
        total += lens[t]
    B = np.empty(total + 16, A.dtype)
    pos = 0
    for t in range(lens.shape[0]):
        c = lens[t]
        if c:
            B[pos:pos + c] = A[starts[t]:starts[t] + c]
            starts[t] = pos
            pos += c
    return B, pos


# ---------------------------------------------------------- reference: fresh trace
@njit(cache=True)
def _fresh(src, dst, amt, slab, co, n_clusters):
    """Full recomputation, lifo.fresh_trace compiled: one stack of (label, units) runs
    per cluster, as a linked list.  Returns the labels in CSR form and the holdings."""
    n = src.shape[0]
    head = np.full(n_clusters, -1, I64)
    cap = n + 16
    nl = np.empty(cap, I64); na = np.empty(cap, I64); nx = np.empty(cap, I64)
    nn = 0
    fs = np.zeros(n, I64); fl = np.zeros(n, I64)
    FL = np.empty((n + 16, 2), I64)
    fu = 0
    tmp = np.empty((64, 2), I64)
    used = np.zeros(n_clusters, np.bool_)  # clusters that ever received an inflow
    for t in range(n):
        s = src[t]; d = dst[t]; a = amt[t]
        cd = co[d]
        if slab[s] >= 0:
            k = 1
            tmp[0, 0] = slab[s]
            tmp[0, 1] = a
            internal = False
        else:
            cs = co[s]
            internal = cs == cd
            need = a
            x = head[cs]
            k = 0
            while need:
                if k == tmp.shape[0]:
                    tmp = _grow2(tmp, k + 1)
                r = na[x]
                if r <= need:
                    tmp[k, 0] = nl[x]; tmp[k, 1] = r; k += 1
                    need -= r
                    x = nx[x]
                else:
                    tmp[k, 0] = nl[x]; tmp[k, 1] = need; k += 1
                    if not internal:
                        na[x] = r - need
                    need = 0
            if not internal:
                head[cs] = x
            lo = 0
            hi = k - 1
            while lo < hi:
                for c in range(2):
                    y = tmp[lo, c]; tmp[lo, c] = tmp[hi, c]; tmp[hi, c] = y
                lo += 1
                hi -= 1
        if fu + k > FL.shape[0]:
            FL = _grow2(FL, fu + k)
        FL[fu:fu + k] = tmp[:k]
        fs[t] = fu
        fl[t] = k
        fu += k
        if internal:
            continue
        used[cd] = True
        for i in range(k):                    # push in the same order, coalescing
            h = head[cd]
            if h >= 0 and nl[h] == tmp[i, 0]:
                na[h] += tmp[i, 1]
            else:
                if nn == nl.shape[0]:
                    nl = _grow1(nl, nn + 1); na = _grow1(na, nn + 1); nx = _grow1(nx, nn + 1)
                nl[nn] = tmp[i, 0]; na[nn] = tmp[i, 1]; nx[nn] = h
                head[cd] = nn
                nn += 1
    hold = np.zeros(n_clusters, I64)
    for c in range(n_clusters):
        x = head[c]
        while x >= 0:
            if nl[x] != 0:
                hold[c] += na[x]
            x = nx[x]
    return fs, fl, FL[:fu], hold, used


@njit(cache=True)
def _labels_differ(ls, ll, LB, fs, fl, FL):
    """First transaction whose two label sequences differ, or -1."""
    for t in range(ls.shape[0]):
        if ll[t] != fl[t]:
            return t
        a = ls[t]
        b = fs[t]
        for i in range(ll[t]):
            if LB[a + i, 0] != FL[b + i, 0] or LB[a + i, 1] != FL[b + i, 1]:
                return t
    return -1


def fresh_trace_arrays(A, co_ids, n_clusters):
    """Compiled fresh trace on ledger arrays; co_ids: wallet -> cluster id (int64).
    Returns the labels in CSR form and the holdings per cluster id."""
    fs, fl, FL, hold, _ = _fresh(A.src, A.dst, A.amt, A.slab, co_ids, n_clusters)
    return fs, fl, FL, hold


def fresh_trace(L, cluster_of, order="preserve", A=None):
    """Same interface and results as lifo.fresh_trace.  Any hashable cluster key works,
    None included; the reversed push order exists only in the reference, which it uses."""
    if order != "preserve":
        return lifo.fresh_trace(L, cluster_of, order)
    A = A or LedgerArrays(L)
    keys = {}
    co = np.full(L.W, -1, I64)
    for w in range(L.W):
        if L.source_label[w] < 0:
            co[w] = keys.setdefault(cluster_of[w], len(keys))
    fs, fl, FL, hold, used = _fresh(A.src, A.dst, A.amt, A.slab, co, max(1, len(keys)))
    flat = FL.ravel().tolist()
    out = [tuple(flat[2 * fs[t]:2 * (fs[t] + fl[t])]) for t in range(L.n)]
    return out, {c: int(hold[i]) for c, i in keys.items() if used[i]}


# ------------------------------------------------------------------- the engine
class ClusterView:
    __slots__ = ("members", "hold")

    def __init__(self, members, hold):
        self.members = members
        self.hold = hold


class _RunsView:
    """Read-only sequence view of an arena: item t as a flat tuple, or None if empty."""

    def __init__(self, eng, which):
        self.e = eng
        self.which = which

    def __len__(self):
        return self.e.L.n

    def __getitem__(self, t):
        if isinstance(t, slice):
            return [self[i] for i in range(*t.indices(len(self)))]
        e = self.e
        if self.which == "labels":
            return tuple(e.LB[e.ls[t]:e.ls[t] + e.ll[t]].ravel().tolist())
        if e.rl[t] == 0:
            return None
        return tuple(e.P[e.rs[t]:e.rs[t] + e.rl[t]].ravel().tolist())

    def __iter__(self):
        return iter(self.tolist())

    def tolist(self):
        e = self.e
        if self.which == "labels":
            S, N, A = e.ls, e.ll, e.LB
        else:
            S, N, A = e.rs, e.rl, e.P
        flat = A.ravel().tolist()
        w = A.shape[1]
        out = []
        for s, c in zip(S.tolist(), N.tolist()):
            out.append(tuple(flat[w * s:w * (s + c)]) if c or self.which == "labels" else None)
        return out

    def __eq__(self, other):
        return self.tolist() == list(other)


class Engine:
    def __init__(self, L, clusters=(), sweep_after=None, cache_budget=None, cutoff=True):
        """Same parameters and semantics as lifo.Engine."""
        self.L = L
        self.A = A = LedgerArrays(L)
        n = L.n
        self.sweep_after = n // 4 if sweep_after is None else sweep_after
        self.cache_budget = n if cache_budget is None else cache_budget
        self.cutoff = cutoff
        # The reference switches to the linear pass when `relabeled > sweep_after`; at most
        # n recomputations happen, so any threshold of n or more means never, and the
        # comparison against an integer count only sees floor(sweep_after).
        sa = self.sweep_after
        self._limit = n if not sa < n else max(-1, math.floor(sa))
        self.cache = OrderedDict()
        self.cache_entries = 0
        self.stats = self._zero()
        groups = check_partition(L, clusters)
        covered = set().union(*groups) if groups else set()
        groups += [frozenset((w,)) for w in L.wallets() if w not in covered]
        self.owner = np.full(L.W, -1, I64)
        self.members = {}
        for cid, m in enumerate(groups):
            self.members[cid] = m
            self.owner[list(m)] = cid
        self.free = []
        (self.rs, self.rl, self.P, self.pu, self.ds, self.dl, self.RD, self.ru, self.ls,
         self.ll, self.LB, self.lu, self.stol, self.net, self.buf) = _build(
            A.src, A.dst, A.amt, A.slab, self.owner, max(1, len(groups)))
        # a quarter of headroom, so the first updates append without copying an arena
        self.P = _headroom(self.P, self.pu)
        self.LB = _headroom(self.LB, self.lu)
        self.RD = _headroom(self.RD, self.ru)
        self.hold = np.zeros(max(16, 2 * len(groups)), I64)
        ws = A.wallets
        np.add.at(self.hold, self.owner[ws], self.net[ws])
        self.live_p, self.live_l, self.live_r = self.pu, self.lu, self.ru
        self.mask = np.zeros(L.W, np.bool_)
        self.seen = np.zeros(n, np.bool_)
        self.compact_slack = 4096  # dead arena rows tolerated beyond the live ones
        self.mark = np.zeros(n, np.bool_)    # scratch for _events, left all False
        self.slot = np.full(n, -1, I64)      # scratch for _invert, left all -1
        self.cmark = np.zeros(L.W + 1, np.bool_)  # scratch for _owners, left all False

    @staticmethod
    def _zero():
        return dict(swept=0, route_changed=0, relabeled=0, label_changed=0, cache_hits=0,
                    linear_pass=0, compactions=0)

    def reset_stats(self):
        s, self.stats = self.stats, self._zero()
        return s

    # ----------------------------------------------------------------- operations
    def merge(self, wallets):
        """Merge the clusters containing the given wallets."""
        cs = {}  # in wallet order, as lifo.Engine.merge, so the cache order matches too
        for w in check_wallets(self.L.W, wallets):
            c = int(self.owner[w])
            if c < 0:
                raise ValueError("only non-source wallets can be clustered")
            cs[c] = None
        if len(cs) > 1:
            self.regroup([frozenset().union(*(self.members[c] for c in cs))])

    def split(self, part):
        """Split the wallets in `part`, which must share one cluster, into their own."""
        part = frozenset(check_wallets(self.L.W, part))
        if not part:
            return
        if self.owner[next(iter(part))] < 0 or len({int(self.owner[w]) for w in part}) != 1:
            raise ValueError("split part must lie inside one cluster")
        self.regroup([part])

    def regroup(self, blocks):
        """Make each wallet set in `blocks` a cluster.  The other members of the clusters
        they touch stay together as the remainder of each such cluster."""
        L, A = self.L, self.A
        blocks = check_partition(L, blocks)  # validate before changing anything
        if not blocks:
            return
        touched = frozenset().union(*blocks)
        old = _owners(_arr(touched), self.owner, self.cmark).tolist()
        new = list(blocks)
        for c in old:
            rest = self.members[c] - touched
            if rest:
                new.append(rest)
        if set(new) == {self.members[c] for c in old}:
            return
        for c in old:
            m = self.members.pop(c)
            entry = self.cache.get(m)
            if entry is None:  # the outgoing cluster's routing, kept for a later undo
                entry = _snapshot(_arr(m), self.mask, A.wptr, A.widx, A.src, A.dst,
                                  self.rs, self.rl, self.P, self.ds, self.dl, self.RD, self.mark)
                if self.cache_budget:
                    self._cache_put(m, entry)
            keys = entry[3]
            self.live_r -= int(_drop_readers(keys, self.dl))  # replaced wholesale below
            self.free.append(c)
        dirty = []
        for m in new:
            ma = _arr(m)
            entry = self._cache_get(m)
            if entry is None:
                tx, ptr, pcs, keys, rptr, rds, k = _sweep(
                    ma, self.mask, A.wptr, A.widx, A.src, A.dst, A.amt, self.mark, self.slot)
                self.stats["swept"] += int(k)
                entry = (tx, ptr, pcs, keys, rptr, rds)
                self._cache_put(m, entry)
            else:
                self.stats["cache_hits"] += 1
            tx, ptr, pcs, keys, rptr, rds = entry
            c = self._new_id()
            self.members[c] = m
            self.hold[c] = _assign(ma, self.owner, c, self.net)
            self.RD, self.ru = _put_readers(keys, rptr, rds, self.ds, self.dl, self.RD, self.ru)
            self.live_r += int(rds.shape[0])
            self.P, self.pu, d, dlive = _install(tx, ptr, pcs, self.rs, self.rl, self.P, self.pu)
            self.live_p += dlive
            dirty.append(d)
        dirty = np.sort(np.concatenate(dirty)) if dirty else np.empty(0, I64)
        self.stats["route_changed"] += int(dirty.shape[0])
        self._propagate(dirty)
        self._compact_if_needed()

    repartition = regroup

    def _new_id(self):
        if self.free:
            return self.free.pop()
        c = len(self.members) + len(self.free)
        while c in self.members:
            c += 1
        if c >= self.hold.shape[0]:
            self.hold = _grow1(self.hold, c + 1)
        return c

    # ------------------------------------------------------------- routing cache
    def _cache_get(self, m):
        e = self.cache.get(m)
        if e is not None:
            self.cache.move_to_end(m)
        return e

    def _cache_put(self, m, e):
        if m in self.cache:
            self.cache.move_to_end(m)
            return
        self.cache[m] = e
        self.cache_entries += int(e[0].shape[0])
        while self.cache_entries > self.cache_budget and self.cache:
            _, old = self.cache.popitem(last=False)
            self.cache_entries -= int(old[0].shape[0])

    # --------------------------------------------------------------- propagation
    def _propagate(self, seeds):
        if not seeds.shape[0]:
            return
        (self.LB, self.lu, self.buf, relabeled, changed, linear, dlive) = _propagate(
            seeds, self.rs, self.rl, self.P, self.ls, self.ll, self.LB, self.lu, self.ds,
            self.dl, self.RD, self.stol, self.net, self.owner, self.hold, self.A.src,
            self.A.dst, self.seen, self._limit, bool(self.cutoff), self.buf)
        self.live_l += dlive
        self.stats["relabeled"] += int(relabeled)
        self.stats["label_changed"] += int(changed)
        self.stats["linear_pass"] += int(linear)

    def _compact_if_needed(self):
        k = self.compact_slack
        if self.pu > 2 * self.live_p + k:
            self.P, self.pu = _compact2(self.rs, self.rl, self.P)
            self.stats["compactions"] += 1
        if self.lu > 2 * self.live_l + k:
            self.LB, self.lu = _compact2(self.ls, self.ll, self.LB)
            self.stats["compactions"] += 1
        if self.ru > 2 * self.live_r + k:
            self.RD, self.ru = _compact1(self.ds, self.dl, self.RD)
            self.stats["compactions"] += 1

    # ------------------------------------------------------------------- queries
    @property
    def labels(self):
        return _RunsView(self, "labels")

    @property
    def route(self):
        return _RunsView(self, "route")

    @property
    def readers(self):
        RD = self.RD.tolist()
        return {t: RD[s:s + c] for t, (s, c) in enumerate(zip(self.ds.tolist(), self.dl.tolist())) if c}

    def clusters(self):
        """The clusters in order of their first wallet, as lifo.Engine.clusters() lists
        them.  Each ClusterView is a snapshot: its holdings do not follow later updates."""
        ids = self.owner[self.A.wallets]
        if not ids.shape[0]:
            return []
        _, first = np.unique(ids, return_index=True)
        return [ClusterView(self.members[c], int(self.hold[c])) for c in ids[np.sort(first)].tolist()]

    def cluster_of(self):
        """wallet -> cluster key (smallest member), and stolen holdings by key."""
        key = {c: min(m) for c, m in self.members.items()}
        co = [key[c] if c >= 0 else None for c in self.owner.tolist()]
        return co, {key[c]: int(self.hold[c]) for c in self.members}

    def check_against_fresh(self):
        """Compare with the compiled fresh trace of the current clustering."""
        fs, fl, FL, hold = fresh_trace_arrays(self.A, self.owner, self.hold.shape[0])
        if _labels_differ(self.ls, self.ll, self.LB, fs, fl, FL) >= 0:
            return False
        return all(int(hold[c]) == int(self.hold[c]) for c in self.members)

    def check_invariants(self):
        """State that export() does not show, checked for the test harnesses: live-row
        counters, arena bounds, scratch arrays left clean, cluster ids and ownership.
        Returns a list of problems (empty when all hold)."""
        bad = []
        for name, (S, N, used, live) in {
                "routes": (self.rs, self.rl, self.pu, self.live_p),
                "labels": (self.ls, self.ll, self.lu, self.live_l),
                "readers": (self.ds, self.dl, self.ru, self.live_r)}.items():
            if int(N.sum()) != live:
                bad.append(f"{name}: live counter {live} != {int(N.sum())}")
            nz = N > 0
            if nz.any() and int((S[nz] + N[nz]).max()) > used:
                bad.append(f"{name}: a row lies beyond the used part of the arena")
        if self.pu > self.P.shape[0] or self.lu > self.LB.shape[0] or self.ru > self.RD.shape[0]:
            bad.append("an arena's used part exceeds its capacity")
        k = self.compact_slack  # after every operation, as _compact_if_needed leaves them
        if self.pu > 2 * self.live_p + k or self.lu > 2 * self.live_l + k or \
                self.ru > 2 * self.live_r + k:
            bad.append("an arena holds more dead rows than the compaction threshold allows")
        if self.mask.any() or self.mark.any() or self.seen.any() or self.cmark.any() \
                or (self.slot != -1).any():
            bad.append("a scratch array was left dirty")
        if set(self.free) & set(self.members) or len(set(self.free)) != len(self.free):
            bad.append("cluster id bookkeeping")
        if self.members and max(self.members) >= self.hold.shape[0]:
            bad.append("holdings array too small")
        ws = self.A.wallets
        if (self.owner[self.A.slab >= 0] != -1).any():
            bad.append("a source has an owner")
        seen = set()
        net = self.net.tolist()
        for c, m in self.members.items():
            if seen & m or any(int(self.owner[w]) != c for w in m):
                bad.append(f"cluster {c}: members and owners disagree")
            if int(self.hold[c]) != sum(net[w] for w in m):
                bad.append(f"cluster {c}: holdings are not the members' net flows")
            seen |= m
        if seen != set(ws.tolist()):
            bad.append("clusters do not partition the wallets")
        return bad

    def export(self):
        """The whole state in the reference engine's Python form, for verification."""
        flatP = self.P.ravel().tolist()
        route = [tuple(flatP[3 * s:3 * (s + c)]) if c else None
                 for s, c in zip(self.rs.tolist(), self.rl.tolist())]
        flatL = self.LB.ravel().tolist()
        labels = [tuple(flatL[2 * s:2 * (s + c)]) for s, c in zip(self.ls.tolist(), self.ll.tolist())]
        RD = self.RD.tolist()
        readers = {t: RD[s:s + c] for t, (s, c) in enumerate(zip(self.ds.tolist(), self.dl.tolist())) if c}
        owners_ok = all(w in self.members[int(self.owner[w])] for w in self.A.wallets.tolist())
        cache = [(m, _routes_of(e), _readers_of(e)) for m, e in self.cache.items()]
        return dict(route=route, readers=readers, labels=labels, stol=self.stol.tolist(),
                    net=self.net.tolist(), clusters={c.members: c.hold for c in self.clusters()},
                    owners_ok=owners_ok, cache=cache, cache_entries=self.cache_entries,
                    cache_budget=self.cache_budget)


def state_of(E):
    """The whole state of either engine (this one or lifo.Engine) in one Python form:
    routes, readers, labels, stolen counts, net flows, partition with holdings, owners,
    and the routing cache in LRU order."""
    if isinstance(E, Engine):
        return E.export()
    return dict(route=list(E.route), readers={k: list(v) for k, v in E.readers.items() if v},
                labels=list(E.labels), stol=list(E.stol), net=list(E.net),
                clusters={c.members: c.hold for c in E.clusters()},
                owners_ok=all(w in E.owner[w].members for w in E.L.wallets()),
                cache=[(m, dict(e[0]), {k: list(v) for k, v in e[1].items()})
                       for m, e in E.cache.items()],
                cache_entries=E.cache_entries, cache_budget=E.cache_budget)


def _headroom(A, used):
    need = used + used // 4 + 16
    if A.shape[0] >= need:
        return A
    B = np.empty((need,) + A.shape[1:], A.dtype)
    B[:used] = A[:used]
    return B


def _arr(m):
    return np.fromiter(m, I64, len(m))


def _routes_of(e):
    tx, ptr, pcs = e[0].tolist(), e[1].tolist(), e[2].ravel().tolist()
    return {t: tuple(pcs[3 * ptr[i]:3 * ptr[i + 1]]) for i, t in enumerate(tx)}


def _readers_of(e):
    keys, rptr, rds = e[3].tolist(), e[4].tolist(), e[5].tolist()
    return {s: rds[rptr[i]:rptr[i + 1]] for i, s in enumerate(keys)}
