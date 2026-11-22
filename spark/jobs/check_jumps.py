"""Exactness check of the jump table, in local mode, against lifo_recluster.

    PYTHONPATH=jobs python jobs/check_jumps.py --repo .. [--steps 6] [--hot-key-rows N] \
        [--chunk-events N]

Builds the combined ledger of check_local.py, runs the batch build with a jump table,
then applies random merges and splits one at a time with jumps.recluster. After the build
and after every step, it requires the labels and holdings to equal lifo.fresh_trace for
the clustering at that point, and the incrementally kept pieces to equal a full re-sweep.
"""
import argparse
import random
import sys
import time


def random_change(rng, L, blocks):
    """Merge the clusters at the two ends of a random transfer, or split a random cluster."""
    blocks = [list(b) for b in blocks]
    multi = [i for i, b in enumerate(blocks) if len(b) > 1]
    if multi and rng.random() < 0.5:
        i = rng.choice(multi)
        b = blocks[i][:]
        rng.shuffle(b)
        cut = rng.randint(1, len(b) - 1)
        blocks[i] = b[:cut]
        blocks.append(b[cut:])
        return blocks, f"split a {len(b)}-wallet cluster into {cut} + {len(b) - cut}"
    where = {w: i for i, b in enumerate(blocks) for w in b}
    while True:
        t = rng.randrange(L.n)
        a, b = L.src[t], L.dst[t]
        if L.source_label[a] < 0 and L.source_label[b] < 0 and where.get(a, -a - 1) != where.get(b, -b - 1):
            break
    ga = blocks[where[a]] if a in where else [a]
    gb = blocks[where[b]] if b in where else [b]
    rest = [blk for i, blk in enumerate(blocks) if i not in (where.get(a), where.get(b))]
    return rest + [ga + gb], f"merge clusters of {len(ga)} and {len(gb)} wallets (transfer {t})"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="..")
    ap.add_argument("--n-gen", type=int, default=1500)
    ap.add_argument("--n-small", type=int, default=60)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--steps", type=int, default=6)
    ap.add_argument("--partitions", type=int, default=8)
    ap.add_argument("--driver-memory", default="3g")
    ap.add_argument("--hot-key-rows", type=int, help="the job's --hot-key-rows")
    ap.add_argument("--chunk-events", type=int, help="the job's --chunk-events")
    args = ap.parse_args()
    sys.path.insert(0, args.repo)

    import pandas as pd
    from pyspark.sql import SparkSession
    from check_local import combined_ledger, expected
    from lifo_spark import jumps as J
    from lifo_spark.build import events, flows_and_holdings, materialize, routes, stolen_deposits

    L, blocks = combined_ledger(args.n_gen, args.n_small, args.seed)
    spark = (SparkSession.builder.master("local[2]").appName("lifo-jumps-check")
             .config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.shuffle.partitions", str(args.partitions))
             .config("spark.driver.memory", args.driver_memory)
             .config("spark.sql.ui.retainedExecutions", "20")    # a long local session runs
             .config("spark.ui.retainedJobs", "50")              # thousands of small jobs; keep
             .config("spark.ui.retainedStages", "50")            # the status store small
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    df = lambda d: spark.createDataFrame(pd.DataFrame(d))
    transfers = materialize(df({"seq": range(L.n), "src": L.src, "dst": L.dst, "amount": L.amt}))
    sources = df({"wallet": [w for w in range(L.W) if L.source_label[w] >= 0],
                  "label": pd.Series([x for x in L.source_label if x >= 0], dtype="int32")})
    membership = lambda bs: materialize(df({"wallet": pd.Series([w for b in bs for w in b], dtype="int64"),
                                            "cluster": pd.Series([i for i, b in enumerate(bs) for _ in b],
                                                                 dtype="int64")}))

    def check(state, m, bs, title, counts=""):
        _, want_labels, want_hold, _ = expected(L, bs)
        runs = state["labels"].toPandas().sort_values(["t", "start"])
        got_labels = {t: list(map(tuple, g[["start", "length", "label"]].to_numpy().tolist()))
                      for t, g in runs.groupby("t")}
        _, holdings = flows_and_holdings(transfers, sources, m, state["labels"])
        got_hold = dict(holdings.toPandas()[["cluster", "stolen"]].itertuples(index=False))
        full = routes(events(transfers, sources, m), args.partitions).drop("cluster")
        same_pieces = (state["pieces"].drop("cluster").exceptAll(full).isEmpty()
                       and full.exceptAll(state["pieces"].drop("cluster")).isEmpty())
        bad = [t for t in set(want_labels) | set(got_labels) if want_labels.get(t) != got_labels.get(t)]
        badh = [c for c in set(want_hold) | set(got_hold) if want_hold.get(c) != got_hold.get(c)]
        ok = not bad and not badh and same_pieces
        print(f"{title:58s} {counts}\n   labels {len(want_labels):,}: "
              f"{'all equal' if not bad else f'{len(bad)} DIFFER'}; holdings {len(want_hold):,}: "
              f"{'all equal' if not badh else f'{len(badh)} DIFFER'}; pieces "
              f"{'equal a full re-sweep' if same_pieces else 'DIFFER'}", flush=True)
        return ok

    t0 = time.time()
    m = membership(blocks)
    pieces = materialize(routes(events(transfers, sources, m), args.partitions,
                                args.chunk_events))
    from lifo_spark.build import propagate
    deposits = materialize(stolen_deposits(transfers, sources))
    _, full_rounds = propagate(pieces, deposits, deposits.select("t"))
    state, rounds = J.build(pieces, deposits, hot_key_rows=args.hot_key_rows)
    print(f"ledger: {L.n:,} transfers, {L.W:,} wallets, {len(blocks):,} clusters; "
          f"{state['jumps'].count():,} jump rows, {state['hpieces'].select('t').distinct().count():,} "
          f"payments with head routes")
    ok = check(state, m, blocks, "batch build with the jump table",
               f"{rounds} rounds over heads, against {full_rounds} over all payments")
    rng = random.Random(args.seed)
    for step in range(args.steps):
        new_blocks, what = random_change(rng, L, blocks)
        m2 = membership(new_blocks)
        state, c = J.recluster(transfers, sources, m, m2, state, args.partitions,
                               args.hot_key_rows, args.chunk_events)
        ok &= check(state, m2, new_blocks, f"{step + 1}. {what}",
                    f"seeds {c['seeds']}, re-pointed {c['repointed']} rows and {c['repointed_pieces']} "
                    f"head pieces, head rounds {c['rounds']}, "
                    f"heads changed {c['heads_changed']}, filled {c['filled']}")
        blocks, m = new_blocks, m2
        spark.sparkContext._jvm.System.gc()   # lets the context cleaner drop the last step's checkpoints
    print(f"{'ALL EQUAL' if ok else 'MISMATCH'} ({time.time() - t0:.0f} s)")
    spark.stop()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
