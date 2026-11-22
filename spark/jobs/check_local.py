"""Exactness check of the Spark build, in local mode, against lifo_recluster.

    PYTHONPATH=jobs python jobs/check_local.py --repo .. [--n-gen 5000] [--jump-table] \
        [--hot-key-rows N] [--chunk-events N]

Builds one ledger out of a generated laundering network and many small random ledgers
(disjoint wallets, one after another in time), runs the Spark build on it, and requires
the routes to equal lifo.sweep and the labels and holdings to equal lifo.fresh_trace.
Half of the one-wallet clusters are left out of membership, so the job's own singletons,
cluster -(wallet + 1), are checked as well as listed ones. --hot-key-rows and
--chunk-events are the job's; set low, they send most keys down the broadcast path and
sweep most clusters in chunks.
"""
import argparse
import random
import sys
import time


def combined_ledger(n_gen, n_small, seed):
    from gen import generate
    from lifo import Ledger
    from test_engine import random_ledger, random_partition

    rng = random.Random(seed)
    (s, d, a, srcs, W), clusters, _ = generate(n_gen, seed=seed)
    parts = [(s, d, a, srcs, W, clusters)]
    for _ in range(n_small):
        L = random_ledger(rng, rng.randint(2, 8), rng.randint(5, 60), n_labels=3,
                          max_amt=rng.choice([3, 9, 50]))
        parts.append((L.src, L.dst, L.amt,
                      {w: lab for w, lab in enumerate(L.source_label) if lab >= 0},
                      L.W, random_partition(rng, L.wallets(), 0.5)))
    offsets = [sum(p[4] for p in parts[:i]) for i in range(len(parts))]
    shift = lambda ws, o: [w + o for w in ws]
    L = Ledger([w for p, o in zip(parts, offsets) for w in shift(p[0], o)],
               [w for p, o in zip(parts, offsets) for w in shift(p[1], o)],
               [x for p in parts for x in p[2]],
               {w + o: lab for p, o in zip(parts, offsets) for w, lab in p[3].items()},
               sum(p[4] for p in parts))
    blocks = [shift(b, o) for p, o in zip(parts, offsets) for b in p[5]]
    return L, blocks


def expected(L, blocks):
    from lifo import fresh_trace, sweep

    cluster_of = [-(w + 1) for w in range(L.W)]
    for i, b in enumerate(blocks):
        for w in b:
            cluster_of[w] = i
    clusters = {}
    for w in L.wallets():                  # the listed clusters and the wallets left out
        clusters.setdefault(cluster_of[w], []).append(w)
    routes = {t: p for m in clusters.values() for t, p in sweep(L, frozenset(m))[0].items()}
    out, hold = fresh_trace(L, cluster_of)

    def stolen_runs(runs):
        starts = [sum(runs[1:k:2]) for k in range(0, len(runs), 2)]
        return [(st, runs[k + 1], runs[k]) for st, k in zip(starts, range(0, len(runs), 2))
                if runs[k]]
    labels = {t: r for t, r in ((t, stolen_runs(runs)) for t, runs in enumerate(out)) if r}
    return routes, labels, {c: v for c, v in hold.items() if v}, cluster_of


def actual(spark, L, blocks, partitions, labels_checkpoint_every, jump_table=False,
           hot_key_rows=None, chunk_events=None):
    import pandas as pd
    from lifo_spark.build import build

    df = lambda d: spark.createDataFrame(pd.DataFrame(d))
    transfers = df({"seq": range(L.n), "src": L.src, "dst": L.dst, "amount": L.amt})
    sources = df({"wallet": [w for w in range(L.W) if L.source_label[w] >= 0],
                  "label": pd.Series([lab for lab in L.source_label if lab >= 0], dtype="int32")})
    membership = df({"wallet": [w for b in blocks for w in b],
                     "cluster": [i for i, b in enumerate(blocks) for _ in b]})
    out, rounds = build(transfers, sources, membership, partitions, labels_checkpoint_every,
                        jump_table, hot_key_rows=hot_key_rows, chunk_events=chunk_events)
    pieces = out["pieces"].toPandas().sort_values(["t", "k"])
    routes = {t: tuple(g[["s", "offset", "length"]].to_numpy().ravel().tolist())
              for t, g in pieces.groupby("t")}
    runs = out["labels"].toPandas().sort_values(["t", "start"])
    labels = {t: list(map(tuple, g[["start", "length", "label"]].to_numpy().tolist()))
              for t, g in runs.groupby("t")}
    holdings = dict(out["holdings"].toPandas()[["cluster", "stolen"]].itertuples(index=False))
    return routes, labels, holdings, rounds


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="..")
    ap.add_argument("--n-gen", type=int, default=5000)
    ap.add_argument("--n-small", type=int, default=300)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--partitions", type=int, default=8)
    ap.add_argument("--batch", type=int, default=64,
                    help="Arrow batch size; small, so clusters span batches and the carried "
                         "stack is exercised")
    ap.add_argument("--labels-checkpoint-every", type=int, default=1)
    ap.add_argument("--jump-table", action="store_true",
                    help="propagate over heads only, as the job's --jump-table does")
    ap.add_argument("--hot-key-rows", type=int, help="the job's --hot-key-rows")
    ap.add_argument("--chunk-events", type=int, help="the job's --chunk-events")
    args = ap.parse_args()
    sys.path.insert(0, args.repo)

    from pyspark.sql import SparkSession
    L, blocks = combined_ledger(args.n_gen, args.n_small, args.seed)
    listed = [b for i, b in enumerate(blocks) if len(b) > 1 or i % 2]   # the rest are left out
    t0 = time.time()
    want_routes, want_labels, want_hold, _ = expected(L, listed)
    spark = (SparkSession.builder.master("local[2]").appName("lifo-check")
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.shuffle.partitions", str(args.partitions))
             .config("spark.sql.execution.arrow.maxRecordsPerBatch", str(args.batch))
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    got_routes, got_labels, got_hold, rounds = actual(spark, L, listed, args.partitions,
                                                      args.labels_checkpoint_every,
                                                      args.jump_table, args.hot_key_rows,
                                                      args.chunk_events)
    spark.stop()

    checks = [("routes", want_routes, got_routes), ("stolen labels", want_labels, got_labels),
              ("holdings", want_hold, got_hold)]
    print(f"ledger: {L.n:,} transfers, {L.W:,} wallets, {len(blocks):,} clusters "
          f"({len(blocks) - len(listed):,} left out of membership); "
          f"{len(got_routes):,} routes, {len(got_labels):,} tainted transfers, "
          f"{rounds} label rounds{' over heads' if args.jump_table else ''}, "
          f"{time.time() - t0:.0f} s")
    ok = True
    for name, want, got in checks:
        bad = [k for k in set(want) | set(got) if want.get(k) != got.get(k)]
        print(f"  {name:14s} {len(want):>7,} expected  {'all equal' if not bad else f'{len(bad)} DIFFER, e.g. {sorted(bad)[:3]}'}")
        ok &= not bad
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
