"""
Exact LIFO fund tracing with incremental reclustering.

Model
-----
* Transactions t = 0..n-1 execute in index order: (src[t], dst[t], amt[t]) with positive
  integer amounts.  Source wallets emit labelled funds (label 0 = clean, >= 1 = stolen).
* A clustering partitions the non-source wallets.  Each cluster owns one LIFO stack.
    deposit (src is a source):          push amt units with the source's label
    external transfer (clusters differ): pop the top amt units of the sender's stack and
                                         push them onto the receiver's in the same order
    internal transfer (same cluster):    no state change; the transaction carries the top
                                         amt units (what pop + order-preserving push moves)
* Outputs: for every transaction, the run-length label sequence of the funds it moved,
  bottom to top, as a flat tuple (label, units, label, units, ...) with equal neighbours
  coalesced; for every cluster, the stolen units still on its stack at the end.

Representation
--------------
* Routing.  For each transaction a cluster sends, LIFO decides which units of which
  earlier inflows it carries: a short tuple of pieces (inflow, offset, length, ...).
  A cluster's routing depends only on its own membership and the immutable ledger, so it
  is computed by one LIFO sweep over the cluster's events and memoized by membership.
* Readers.  readers[s] lists the transactions whose pieces point into inflow s.  Every
  reader of s is sent by the cluster that received s, so a cluster's readers are built by
  its own sweep and replaced wholesale when the cluster changes.
* Labels.  labels[t] = labels of the pieces of route[t]; deposits carry the source label.
* Holdings.  A cluster's stolen holdings equal the sum over its wallets of stolen received
  minus stolen sent: an internal transfer adds to one member and subtracts from another.
  Per-wallet net flows do not depend on the clustering and change only with labels.

Update (Engine.regroup, which merge and split call)
---------------------------------------------------
1. Replace the clusters containing the regrouped wallets by the new blocks plus what is
   left of them.
2. For each new cluster take routing and readers from the cache, or sweep its events.
3. Every transaction sent by a regrouped wallet whose route changed seeds propagation.
4. Recompute labels in time order from a min-heap, stopping wherever a recomputed label
   equals the old one.  If the wave grows past n/4 transactions, finish with one linear
   pass.  Holdings move by the change in stolen units of each changed label.
"""
from collections import OrderedDict
from heapq import heappush, heappop
from itertools import chain
from numbers import Integral, Real


# --------------------------------------------------------------------------- ledger
def _integer(x, what):
    """x as a Python int; anything that is not an integer (floats included) is rejected."""
    if isinstance(x, bool) or not isinstance(x, Integral):
        raise ValueError(f"{what} must be an integer, got {x!r}")
    return int(x)


def check_wallets(W, wallets):
    """Wallet ids as Python ints, each in 0..W-1 (no negative indexing)."""
    out = []
    for w in wallets:
        w = _integer(w, "wallet id")
        if not 0 <= w < W:
            raise ValueError(f"unknown wallet {w}")
        out.append(w)
    return out


class Ledger:
    def __init__(self, src, dst, amt, sources, n_wallets, names=None):
        """sources: dict wallet -> label (0 clean, >= 1 stolen).  Every wallet's balance
        must stay non-negative; then every cluster's does too, under any clustering.
        Wallet ids, amounts and labels must be integers; they are stored as Python ints."""
        if not (len(src) == len(dst) == len(amt)):
            raise ValueError("src, dst and amt must have the same length")
        self.W = _integer(n_wallets, "n_wallets")
        if self.W < 0:
            raise ValueError("n_wallets must be >= 0")
        self.src = check_wallets(self.W, src)
        self.dst = check_wallets(self.W, dst)
        self.amt = [_integer(a, "amount") for a in amt]
        src, dst, amt = self.src, self.dst, self.amt
        self.n = len(src)
        self.names = names
        lab = [-1] * self.W
        for w, l in sources.items():
            (w,) = check_wallets(self.W, [w])
            l = _integer(l, "source label")
            if l < 0:
                raise ValueError(f"source {w}: label must be >= 0")
            lab[w] = l
        self.source_label = lab
        n_wallets = self.W
        wev = [[] for _ in range(n_wallets)]
        bal = [0] * n_wallets
        for t in range(self.n):
            s, d, a = src[t], dst[t], amt[t]
            if a <= 0:
                raise ValueError(f"tx {t}: amount must be positive")
            if s == d:
                raise ValueError(f"tx {t}: self-transfer")
            if lab[d] >= 0:
                raise ValueError(f"tx {t}: a source cannot receive funds")
            if lab[s] < 0:
                if bal[s] < a:
                    raise ValueError(f"tx {t}: wallet {s} overdraws")
                bal[s] -= a
                wev[s].append(t)
            bal[d] += a
            wev[d].append(t)
        self.wev = wev  # per-wallet sorted list of transactions touching it

    def wallets(self):
        return [w for w in range(self.W) if self.source_label[w] < 0]


def stolen_of(runs):
    """Stolen units in a run sequence (label, units, label, units, ...)."""
    tot = 0
    for i in range(0, len(runs), 2):
        if runs[i]:
            tot += runs[i + 1]
    return tot


def check_threshold(x):
    """The linear-pass threshold: any real number (inf and nan included; nan never fires)."""
    if isinstance(x, bool) or not isinstance(x, Real):
        raise ValueError(f"sweep_after must be a number, got {x!r}")
    return x


def check_partition(L, blocks):
    """Validate wallet sets: disjoint, non-source wallets only.  Returns frozensets."""
    out = []
    seen = set()
    for b in blocks:
        m = frozenset(check_wallets(L.W, b))
        if not m:
            continue
        if m & seen:
            raise ValueError("wallet sets overlap")
        if any(L.source_label[w] >= 0 for w in m):
            raise ValueError("only non-source wallets can be clustered")
        seen |= m
        out.append(m)
    return out


# ------------------------------------------------------------- reference: fresh trace
def fresh_trace(L, cluster_of, order="preserve"):
    """Full recomputation, the reference the engine must always equal.
    cluster_of: list wallet -> hashable cluster key.  order='reverse' (push popped funds
    upside down) exists only to show how the challenge answer depends on that choice.
    Returns (per-transaction runs, {cluster key: stolen holdings})."""
    src, dst, amt, slab = L.src, L.dst, L.amt, L.source_label
    stacks = {}
    out = [None] * L.n
    for t in range(L.n):
        s = src[t]; d = dst[t]; a = amt[t]
        cd = cluster_of[d]
        ls = slab[s]
        if ls >= 0:
            st = stacks.get(cd)
            if st is None:
                st = stacks[cd] = ([], [])
            labs, amts = st
            if labs and labs[-1] == ls:
                amts[-1] += a
            else:
                labs.append(ls); amts.append(a)
            out[t] = (ls, a)
            continue
        cs = cluster_of[s]
        labs, amts = stacks[cs]
        need = a
        acc = []
        if cs == cd:  # internal: read the top without changing anything
            i = len(amts) - 1
            while need:
                x = amts[i]
                if x <= need:
                    acc.append(x); acc.append(labs[i]); need -= x; i -= 1
                else:
                    acc.append(need); acc.append(labs[i]); need = 0
            acc.reverse()
            out[t] = tuple(acc)
            continue
        while need:
            x = amts[-1]
            if x <= need:
                acc.append(amts.pop()); acc.append(labs.pop()); need -= x
            else:
                amts[-1] = x - need; acc.append(need); acc.append(labs[-1]); need = 0
        acc.reverse()
        runs = tuple(acc)
        out[t] = runs
        st2 = stacks.get(cd)
        if st2 is None:
            st2 = stacks[cd] = ([], [])
        labs2, amts2 = st2
        rng = range(0, len(runs), 2) if order == "preserve" else range(len(runs) - 2, -1, -2)
        for k in rng:
            lab = runs[k]; x = runs[k + 1]
            if labs2 and labs2[-1] == lab:
                amts2[-1] += x
            else:
                labs2.append(lab); amts2.append(x)
    hold = {}
    for c, (labs, amts) in stacks.items():
        hold[c] = sum(x for lab, x in zip(labs, amts) if lab)
    return out, hold


# --------------------------------------------------------------------------- routing
def sweep(L, members):
    """Routing of one cluster: a single LIFO pass over its own events.
    Returns (route, readers, events): route {tx: pieces} for every transaction the cluster
    sends; readers {inflow: [tx, ...]} for every inflow some of those pieces point into."""
    src, dst, amt = L.src, L.dst, L.amt
    if len(members) == 1:
        (w,) = members
        ev = L.wev[w]
    else:
        ev = sorted(set(chain.from_iterable([L.wev[w] for w in members])))
    ss = []  # inflow of each block on the stack, bottom to top
    rr = []  # units left in each block (always its bottom units)
    ss_push, rr_push, ss_pop, rr_pop = ss.append, rr.append, ss.pop, rr.pop
    route = {}
    readers = {}
    get = readers.get
    for t in ev:
        a = amt[t]
        if src[t] not in members:          # inflow: push
            ss_push(t); rr_push(a)
            continue
        r = rr[-1]
        if r >= a and dst[t] in members:   # internal read inside the top block
            s = ss[-1]; route[t] = (s, r - a, a)
        elif r > a:                        # outflow inside the top block
            rr[-1] = r - a
            s = ss[-1]; route[t] = (s, r - a, a)
        elif r == a:                       # outflow takes exactly the top block
            rr_pop()
            s = ss_pop(); route[t] = (s, 0, a)
        else:                              # several blocks: read or pop them
            internal = dst[t] in members
            acc = []
            need = a
            i = len(rr) - 1
            while need:
                r = rr[i]
                if r <= need:
                    acc.append((ss[i], 0, r)); need -= r; i -= 1
                else:
                    acc.append((ss[i], r - need, need))
                    if not internal:
                        rr[i] = r - need
                    need = 0
            if not internal:
                del ss[i + 1:], rr[i + 1:]
            acc.reverse()
            p = route[t] = tuple(chain.from_iterable(acc))
            for k in range(0, len(p), 3):
                rl = get(p[k])
                if rl is None:
                    readers[p[k]] = [t]
                else:
                    rl.append(t)
            continue
        rl = get(s)                        # single piece: record the reader
        if rl is None:
            readers[s] = [t]
        else:
            rl.append(t)
    return route, readers, len(ev)


def _label_from_pieces(p, labels):
    """Labels carried by pieces p, read from the labels of the inflows they point into."""
    out = []
    for i in range(0, len(p), 3):
        runs = labels[p[i]]
        off = p[i + 1]
        ln = p[i + 2]
        if len(runs) == 2:  # the inflow carries one label
            lab = runs[0]
            if out and out[-2] == lab:
                out[-1] += ln
            else:
                out.append(lab); out.append(ln)
            continue
        j = 0; pos = 0
        while pos + runs[j + 1] <= off:
            pos += runs[j + 1]; j += 2
        end = off + ln
        cur = off
        while cur < end:
            lab = runs[j]
            rend = pos + runs[j + 1]
            take = (rend if rend < end else end) - cur
            if out and out[-2] == lab:
                out[-1] += take
            else:
                out.append(lab); out.append(take)
            cur += take
            pos = rend; j += 2
    return tuple(out)


# ------------------------------------------------------------------------- engine
class Cluster:
    __slots__ = ("members", "hold")

    def __init__(self, members, hold):
        self.members = members  # frozenset of wallets
        self.hold = hold        # stolen units on the cluster's stack at the end


class Engine:
    def __init__(self, L, clusters=(), sweep_after=None, cache_budget=None, cutoff=True):
        """clusters: disjoint wallet sets; every other non-source wallet is a singleton.
        sweep_after: wave size after which propagation finishes with one linear pass
        (default n // 4).  cache_budget: routes kept in the membership cache, LRU
        (default n).  cutoff=False disables the equality cutoff (for ablation only)."""
        self.L = L
        n = L.n
        self.sweep_after = n // 4 if sweep_after is None else check_threshold(sweep_after)
        self.cache_budget = n if cache_budget is None else cache_budget
        self.cutoff = cutoff
        self.cache = OrderedDict()
        self.cache_entries = 0
        self.stats = self._zero()
        src, dst, amt, slab = L.src, L.dst, L.amt, L.source_label
        groups = check_partition(L, clusters)
        covered = set().union(*groups) if groups else set()
        groups += [frozenset((w,)) for w in L.wallets() if w not in covered]
        route = [None] * n
        readers = {}
        for m in groups:
            r, rd, _ = sweep(L, m)
            for t, p in r.items():
                route[t] = p
            readers.update(rd)
        labels = [None] * n
        stol = [0] * n  # stolen units carried by each transaction (Python ints: no bound)
        net = [0] * L.W  # stolen received - sent, per wallet
        for t in range(n):
            s = src[t]
            if slab[s] >= 0:
                lab = labels[t] = (slab[s], amt[t])
            else:
                lab = labels[t] = _label_from_pieces(route[t], labels)
            k = stolen_of(lab)
            if k:
                stol[t] = k
                net[dst[t]] += k
                if slab[s] < 0:
                    net[s] -= k
        owner = [None] * L.W
        for m in groups:
            c = Cluster(m, sum(net[w] for w in m))
            for w in m:
                owner[w] = c
        self.owner, self.route, self.readers = owner, route, readers
        self.labels, self.stol, self.net = labels, stol, net

    @staticmethod
    def _zero():
        return dict(swept=0, route_changed=0, relabeled=0, label_changed=0, cache_hits=0,
                    linear_pass=0)

    def reset_stats(self):
        s, self.stats = self.stats, self._zero()
        return s

    # ----------------------------------------------------------------- operations
    def merge(self, wallets):
        """Merge the clusters containing the given wallets."""
        cs = {}
        for w in check_wallets(self.L.W, wallets):
            c = self.owner[w]
            if c is None:
                raise ValueError("only non-source wallets can be clustered")
            cs[id(c)] = c
        if len(cs) > 1:
            self.regroup([frozenset().union(*(c.members for c in cs.values()))])

    def split(self, part):
        """Split the wallets in `part`, which must share one cluster, into their own."""
        part = frozenset(check_wallets(self.L.W, part))
        if not part:
            return
        if self.owner[next(iter(part))] is None or len({id(self.owner[w]) for w in part}) != 1:
            raise ValueError("split part must lie inside one cluster")
        self.regroup([part])

    def regroup(self, blocks):
        """Make each wallet set in `blocks` a cluster.  The other members of the clusters
        they touch stay together as the remainder of each such cluster."""
        L, owner, route, readers, net = self.L, self.owner, self.route, self.readers, self.net
        blocks = check_partition(L, blocks)  # validate before changing anything
        if not blocks:
            return
        touched = frozenset().union(*blocks)
        old = list({id(owner[w]): owner[w] for w in touched}.values())
        new = list(blocks)
        for c in old:
            rest = c.members - touched
            if rest:
                new.append(rest)
        if set(new) == {c.members for c in old}:
            return
        src, dst, wev = L.src, L.dst, L.wev
        for c in old:
            entry = self.cache.get(c.members)
            if entry is None:  # the outgoing cluster's routing, kept for a later undo
                rd = {t: readers[t] for w in c.members for t in wev[w]
                      if dst[t] == w and t in readers}
                if self.cache_budget:
                    r = {t: route[t] for w in c.members for t in wev[w] if src[t] == w}
                    self._cache_put(c.members, (r, rd))
            else:
                rd = entry[1]
            for s in rd:  # its readers are replaced wholesale below
                del readers[s]
        dirty = []
        for m in new:
            entry = self._cache_get(m)
            if entry is None:
                r, rd, k = sweep(L, m)
                self.stats["swept"] += k
                entry = (r, rd)
                self._cache_put(m, entry)
            else:
                self.stats["cache_hits"] += 1
                r, rd = entry
            c = Cluster(m, sum(net[w] for w in m))
            for w in m:
                owner[w] = c
            readers.update(rd)
            for t, p in r.items():
                if p != route[t]:
                    route[t] = p
                    dirty.append(t)
        self.stats["route_changed"] += len(dirty)
        self._propagate(dirty)

    repartition = regroup

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
        self.cache_entries += len(e[0])
        while self.cache_entries > self.cache_budget and self.cache:
            _, old = self.cache.popitem(last=False)
            self.cache_entries -= len(old[0])

    # --------------------------------------------------------------- propagation
    def _propagate(self, seeds):
        """Recompute labels in time order from the seeds.  Every input of a transaction
        precedes it, so each label is computed from final inputs; an unchanged label
        stops the wave there."""
        if not seeds:
            return
        L, labels, route, readers = self.L, self.labels, self.route, self.readers
        owner, net, stol, cutoff = self.owner, self.net, self.stol, self.cutoff
        src, dst = L.src, L.dst
        heap = sorted(seeds)  # seeds are unique, and a sorted list is a heap
        seen = set(heap)
        get = readers.get
        relabeled = changed = 0
        limit = self.sweep_after
        linear = False
        t = -1
        while True:
            if linear:  # wave too wide: one pass over every later transaction
                t += 1
                if t >= L.n:
                    break
                p = route[t]
                if p is None:
                    continue
            else:
                if not heap:
                    break
                if relabeled > limit:
                    linear = True
                    self.stats["linear_pass"] += 1
                    t = heap[0] - 1
                    continue
                t = heappop(heap)
                p = route[t]
            relabeled += 1
            runs = labels[p[0]]
            if len(p) == 3 and len(runs) == 2:  # one piece of a one-label inflow
                new = (runs[0], p[2])
            else:
                new = _label_from_pieces(p, labels)
            if new != labels[t]:
                changed += 1
                labels[t] = new
                k = (new[1] if new[0] else 0) if len(new) == 2 else stolen_of(new)
                d = k - stol[t]
                if d:  # holdings follow the stolen units this transaction carries
                    stol[t] = k
                    s = src[t]; r = dst[t]
                    net[s] -= d; net[r] += d
                    owner[s].hold -= d; owner[r].hold += d
            elif cutoff:
                continue
            if not linear:
                rl = get(t)
                if rl:
                    for u in rl:
                        if u not in seen:
                            seen.add(u)
                            heappush(heap, u)
        self.stats["relabeled"] += relabeled
        self.stats["label_changed"] += changed

    # ------------------------------------------------------------------- queries
    def clusters(self):
        return list({id(c): c for c in self.owner if c is not None}.values())

    def cluster_of(self):
        """wallet -> cluster key (smallest member), and stolen holdings by key."""
        cl = self.clusters()
        key = {id(c): min(c.members) for c in cl}
        co = [key[id(c)] if c is not None else None for c in self.owner]
        return co, {key[id(c)]: c.hold for c in cl}

    def check_against_fresh(self):
        co, hold = self.cluster_of()
        out, fhold = fresh_trace(self.L, co)
        return out == self.labels and \
            {k: v for k, v in hold.items() if v} == {k: v for k, v in fhold.items() if v}
