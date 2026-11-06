"""Synthetic laundering networks for the scaling test.

Ingredients: several hacks (one stolen label each), a clean source, user entities that
own several addresses (change addresses -> internal transfers), exchanges with thousands
of deposit addresses that sweep into a hot wallet (large clusters, internal transfers)
and pay out withdrawals, recency-biased spending (deep peel chains, >=20 hops), full
sweeps and partial splits, and reconvergence into existing wallets.

The initial clustering is the true entity partition except that some change addresses
are not yet linked (later merges "discover" them).
"""
import random

USER, DEPOSIT, HOT, HACKER = 0, 1, 2, 3


def generate(n_tx, seed=0, n_exchanges=4, n_hacks=3, hack_amount=None,
             p_clean=0.10, p_withdraw=0.05, p_launder=0.20):
    rng = random.Random(seed)
    if hack_amount is None:  # stolen volume ~ 30% of all clean deposits
        hack_amount = int(0.1 * p_clean * n_tx * 500_000)
    sources = {h: h + 1 for h in range(n_hacks)}  # hack h emits label h+1
    CLEAN_SRC = n_hacks
    sources[CLEAN_SRC] = 0
    W0 = n_hacks + 1
    bal = [0] * W0
    entity = [-1] * W0
    role = [-1] * W0
    funded = []
    fpos = [-1] * W0
    recent = []
    src, dst, amt = [], [], []
    n_entities = 0

    def new_wallet(ent, rl):
        w = len(bal)
        bal.append(0); entity.append(ent); role.append(rl); fpos.append(-1)
        return w

    def new_entity():
        nonlocal n_entities
        n_entities += 1
        return n_entities - 1

    def credit(w, a):
        if bal[w] == 0:
            fpos[w] = len(funded); funded.append(w)
        bal[w] += a
        recent.append(w)

    def debit(w, a):
        bal[w] -= a
        if bal[w] == 0:
            i = fpos[w]; last = funded.pop()
            if last != w:
                funded[i] = last; fpos[last] = i
            fpos[w] = -1

    def emit(s, d, a):
        src.append(s); dst.append(d); amt.append(a)
        if s not in sources:
            debit(s, a)
        credit(d, a)

    exchanges = []
    for _ in range(n_exchanges):
        e = new_entity()
        hot = new_wallet(e, HOT)
        exchanges.append(dict(entity=e, hot=hot, deposits=[]))
    hack_times = sorted(int(n_tx * f) for f in (0.01, 0.35, 0.70)[:n_hacks])
    hack_wallets = []
    users = []  # all user-like wallets, for random reconvergence targets
    launder = []  # hacker-controlled wallets (laundering tree)
    hidden = set()  # change addresses not linked in the initial clustering

    while len(src) < n_tx:
        t = len(src)
        if hack_times and t >= hack_times[0]:
            h = len(hack_wallets)
            hack_times.pop(0)
            e = new_entity()
            w = new_wallet(e, HACKER)
            hack_wallets.append(w); launder.append(w)
            emit(h, w, hack_amount)
            continue
        r = rng.random()
        if r < p_clean or not funded:
            if users and rng.random() < 0.6:
                d = rng.choice(users)
            else:
                d = new_wallet(new_entity(), USER); users.append(d)
            emit(CLEAN_SRC, d, rng.randint(1_000, 1_000_000))
            continue
        if r < p_clean + p_withdraw:
            ex = rng.choice(exchanges)
            hot = ex["hot"]
            if bal[hot] > 0:
                if users and rng.random() < 0.5:
                    d = rng.choice(users)
                else:
                    d = new_wallet(new_entity(), USER); users.append(d)
                emit(hot, d, rng.randint(1, bal[hot]) if rng.random() < 0.9 else bal[hot])
                continue
        if r < p_clean + p_withdraw + p_launder and launder:
            # laundering hop: a recent hacker-controlled wallet peels funds onward
            s = -1
            for _ in range(4):
                k = min(int(rng.expovariate(1 / 30.0)), len(launder) - 1)
                cand = launder[-1 - k]
                if bal[cand] > 0:
                    s = cand
                    break
            if len(launder) > 20000:
                del launder[:10000]
            if s >= 0:
                b = bal[s]
                a = b if rng.random() < 0.2 else max(1, int(b * rng.uniform(0.3, 0.7)))
                q = rng.random()
                if q < 0.55:
                    d = new_wallet(new_entity(), HACKER); launder.append(d)
                elif q < 0.65:
                    d = new_wallet(entity[s], HACKER); launder.append(d)
                    if rng.random() < 0.5:
                        hidden.add(d)
                elif q < 0.80:
                    ex = rng.choice(exchanges)
                    if ex["deposits"] and rng.random() < 0.6:
                        d = rng.choice(ex["deposits"])
                    else:
                        d = new_wallet(ex["entity"], DEPOSIT); ex["deposits"].append(d)
                else:
                    d = funded[rng.randrange(len(funded))]
                if d == s:
                    d = new_wallet(new_entity(), HACKER); launder.append(d)
                emit(s, d, a)
                continue
        # pick a sender: recency-biased (deep chains) or uniform among funded wallets
        s = -1
        if recent and rng.random() < 0.7:
            k = min(int(rng.expovariate(1 / 6.0)), len(recent) - 1)
            cand = recent[-1 - k]
            if bal[cand] > 0:
                s = cand
        if s < 0:
            s = funded[rng.randrange(len(funded))]
        if len(recent) > 4096:
            del recent[:2048]
        b = bal[s]
        if role[s] == DEPOSIT and rng.random() < 0.9:
            ex = exchanges[[x["entity"] for x in exchanges].index(entity[s])]
            emit(s, ex["hot"], b)  # sweep: internal to the exchange cluster
            continue
        if role[s] == HOT:
            if users and rng.random() < 0.5:
                d = rng.choice(users)
            else:
                d = new_wallet(new_entity(), USER); users.append(d)
            emit(s, d, rng.randint(1, b))
            continue
        a = b if rng.random() < 0.3 else max(1, int(b * rng.uniform(0.1, 0.9)))
        q = rng.random()
        if q < 0.12:  # change address of the same entity
            d = new_wallet(entity[s], role[s] if role[s] == HACKER else USER)
            users.append(d)
            if role[s] == HACKER:
                launder.append(d)
            if rng.random() < 0.5:
                hidden.add(d)
        elif q < 0.45:
            d = new_wallet(new_entity(), USER); users.append(d)
        elif q < 0.60:
            ex = rng.choice(exchanges)
            if ex["deposits"] and rng.random() < 0.6:
                d = rng.choice(ex["deposits"])
            else:
                d = new_wallet(ex["entity"], DEPOSIT); ex["deposits"].append(d)
        else:
            d = funded[rng.randrange(len(funded))] if rng.random() < 0.5 else rng.choice(users or funded)
        if d == s:
            d = new_wallet(new_entity(), USER); users.append(d)
        emit(s, d, a)

    W = len(bal)
    by_entity = {}
    for w in range(W0, W):
        by_entity.setdefault(entity[w], []).append(w)
    clusters = []
    for e, ws in by_entity.items():
        known = [w for w in ws if w not in hidden]
        if known:
            clusters.append(known)
        for w in ws:
            if w in hidden:
                clusters.append([w])
    meta = dict(entity=entity, role=role, exchanges=exchanges, hack_wallets=hack_wallets,
                hidden=hidden, by_entity=by_entity, sources=sources)
    return (src, dst, amt, sources, W), clusters, meta
