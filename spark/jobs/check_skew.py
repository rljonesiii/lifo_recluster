"""Skew check of the Spark build, in local mode, against lifo_recluster.

    PYTHONPATH=jobs python jobs/check_skew.py --repo .. [--scale 1]

Builds a ledger with the skew of a real one: an exchange whose hot wallet is in most
transfers and whose cluster holds most events, a large stolen deposit that thousands of
withdrawals read, and a theft fanned out to a thousand wallets and forwarded through
pass-through chains into the exchange, so that one deposit heads thousands of jump rows.

First it sweeps random clusters in chunks of 1 to 400 events and requires the pieces to
equal one sequential sweep's. Then it requires the job's outputs on the skewed ledger to be
exact, without the skew flags, and with --hot-key-rows and --chunk-events on both label
paths: the routes equal lifo.sweep, and the labels and holdings equal lifo.fresh_trace.
Last, it measures five steps, with broadcast joins off (as when both sides are large) and
AQE's partition coalescing off (as when partitions are not tiny): for each, the records
read by the largest and the median task, from Spark's REST API, as the step stands, with
AQE's skew join at a 1 KB threshold, and with the job's fix.
"""
import argparse
import json
import random
from collections import Counter
import sys
import time
import urllib.request


def skewed_ledger(seed=1, scale=1):
    """A valid ledger with hub wallets, a hot deposit, a giant cluster, and path-compressed
    chains, and its clusters as lists of wallets (the exchange first)."""
    from lifo import Ledger
    from test_engine import random_partition

    rng = random.Random(seed)
    src, dst, amt, sources, bal, count = [], [], [], {}, {}, [0]

    def wallets(n):
        count[0] += n
        return list(range(count[0] - n, count[0]))

    def pay(s, d, a):
        if s not in sources:
            assert 0 < a <= bal.get(s, 0), (s, a)
            bal[s] -= a
        bal[d] = bal.get(d, 0) + a
        src.append(s)
        dst.append(d)
        amt.append(a)

    (hacker,), banks = wallets(1), wallets(20)
    sources.update({hacker: 1, **{b: 0 for b in banks}})
    (hot,), addresses = wallets(1), wallets(40 * scale)
    savers, payees, (mule,) = wallets(1500 * scale), wallets(1500 * scale), wallets(1)
    for u in savers:
        pay(rng.choice(banks), u, rng.randint(200, 2000))
    burst = 3000 * scale              # one stolen deposit, then withdrawals that all take from it
    pay(hacker, hot, 30 * burst)
    for _ in range(burst):
        pay(hot, rng.choice(payees), rng.randint(1, 20))
    pay(hacker, mule, 100_000 * scale)  # a theft fanned out, then forwarded whole in chains
    chains = []
    for _ in range(1000 * scale):
        f, g1, g2 = wallets(3)
        pay(mule, f, 100)
        chains.append([f, g1, g2, rng.choice(addresses)])
    for _ in range(30_000 * scale):   # the exchange's busy years
        u = rng.random()
        if u < 0.40:
            s = rng.choice(savers)
            if bal[s]:
                pay(s, rng.choice(addresses), rng.randint(1, min(bal[s], 300)))
        elif u < 0.55:
            a = rng.choice(addresses)
            if bal.get(a):
                pay(a, hot, bal[a])       # a sweep: an internal transfer
        elif u < 0.90:
            if bal[hot]:
                pay(hot, rng.choice(payees), rng.randint(1, min(bal[hot], 400)))
        elif chains:
            i = rng.randrange(len(chains))
            c = chains[i]
            pay(c[0], c[1], bal[c[0]])
            c.pop(0)
            if len(c) == 1:
                chains[i] = chains[-1]
                chains.pop()
    for c in chains:
        for a, b in zip(c, c[1:]):
            pay(a, b, bal[a])
    L = Ledger(src, dst, amt, sources, count[0])
    blocks = ([[hot] + addresses] + random_partition(rng, savers, 0.3)
              + random_partition(rng, payees, 0.3) + [[w] for w in range(mule, count[0])])
    return L, blocks


def random_events(spark, seed, n_clusters, n_events):
    """Random clusters of inflows, outflows, and internal reads that never overdraw."""
    from lifo_spark.sweep import IN, OUT, READ

    rng, rows, seq = random.Random(seed), [], 0
    for c in range(n_clusters):
        bal = 0
        for _ in range(rng.randint(1, n_events)):
            u = rng.random()
            if bal == 0 or u < 0.45:
                a = rng.randint(1, 9)
                rows.append((c - 3, seq, IN, a))
                bal += a
            elif u < 0.7:
                rows.append((c - 3, seq, READ, rng.randint(1, bal)))
            else:
                a = rng.randint(1, bal)
                rows.append((c - 3, seq, OUT, a))
                bal -= a
            seq += rng.randint(1, 3)
    rng.shuffle(rows)
    return spark.createDataFrame(rows, "cluster long, seq long, kind byte, amount long")


def task_records(spark, opener, name, action):
    """Run action in its own job group, then return, over the stages it ran that read a
    shuffle, the one whose largest task read the most records: (largest, median, tasks)."""
    sc = spark.sparkContext
    group = f"{name}-{time.time_ns()}"
    sc.setJobGroup(group, name)
    action()
    api = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    get = lambda path: json.load(opener.open(f"{api}/{path}"))
    stages = {s for j in get("jobs") if j.get("jobGroup") == group for s in j["stageIds"]}
    best = (0, 0, 0)
    for sid in stages:
        for attempt in get(f"stages/{sid}"):
            if attempt["status"] != "COMPLETE" or not attempt["shuffleReadRecords"]:
                continue
            q = get(f"stages/{sid}/{attempt['attemptId']}/taskSummary?quantiles=0.5,1.0")
            median, largest = q["shuffleReadMetrics"]["readRecords"]
            best = max(best, (int(largest), int(median), attempt["numTasks"]))
    return best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="..")
    ap.add_argument("--scale", type=int, default=1)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--hot-key-rows", type=int, default=100)
    ap.add_argument("--chunk-events", type=int, default=2048)
    ap.add_argument("--balance-bins", type=int, default=64)
    args = ap.parse_args()
    sys.path.insert(0, args.repo)

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from check_local import actual, expected
    from lifo_spark import build as B
    from lifo_spark import jumps as J

    t0 = time.time()
    spark = (SparkSession.builder.master("local[2]").appName("lifo-skew-check")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.shuffle.partitions", str(args.partitions))
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
             .config("spark.driver.memory", "3g")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    ok = True
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "37")  # chunks span batches
    for seed, n_clusters, n_events, chunk in ((1, 5, 400, 50), (2, 3, 2000, 7), (3, 8, 300, 3),
                                              (4, 2, 5000, 400), (5, 6, 60, 1)):
        ev, lines = random_events(spark, seed, n_clusters, n_events), []
        one = B.one_task(ev, args.partitions)
        got = B.routes(ev, args.partitions, chunk, lines.append)
        same = one.exceptAll(got).isEmpty() and got.exceptAll(one).isEmpty()
        ok &= same
        print(f"random clusters, seed {seed}: {ev.count():,} events, chunks of {chunk}: "
              f"{lines[0]}; pieces {'equal one sweep' if same else 'DIFFER'}", flush=True)
    spark.conf.unset("spark.sql.execution.arrow.maxRecordsPerBatch")

    L, blocks = skewed_ledger(args.seed, args.scale)
    listed = [b for i, b in enumerate(blocks) if len(b) > 1 or i % 2]   # the rest are left out
    want = expected(L, listed)[:3]
    exchange = set(blocks[0])
    events = sum(1 for s, d in zip(L.src, L.dst) if s in exchange or d in exchange)
    readers = Counter(s for route in want[0].values() for s in route[::3])
    single = {t: route[0] for t, route in want[0].items() if len(route) == 3}
    head = lambda t: head(single[t]) if t in single else t
    heads = Counter(head(t) for t in single)
    print(f"\nledger: {L.n:,} transfers, {L.W:,} wallets, {len(blocks):,} clusters. The "
          f"exchange's hot wallet is in {len(L.wev[blocks[0][0]]):,} transfers, and its "
          f"cluster has {events:,} events. The most-read input is read by "
          f"{readers.most_common(1)[0][1]:,} pieces, and the busiest head has "
          f"{heads.most_common(1)[0][1]:,} of the {len(single):,} jump rows.")
    flags = f"--hot-key-rows {args.hot_key_rows} --chunk-events {args.chunk_events}"
    for jump_table, fix in ((False, False), (False, True), (True, True)):
        got = actual(spark, L, listed, args.partitions, 1, jump_table,
                     args.hot_key_rows if fix else None, args.chunk_events if fix else None)
        bad = [sum(want[i].get(k) != got[i].get(k) for k in set(want[i]) | set(got[i]))
               for i in range(3)]
        ok &= not any(bad)
        name = (flags if fix else "no skew flags") + (" --jump-table" if jump_table else "")
        rounds = f"{got[3]} label round{'' if got[3] == 1 else 's'}"
        print(f"{name}: {rounds}{' over heads' if jump_table else ''}; routes, labels, and "
              "holdings " + ("all equal" if not any(bad) else f"DIFFER {bad}"), flush=True)

    import pandas as pd
    df = lambda d: spark.createDataFrame(pd.DataFrame(d))
    transfers = B.materialize(df({"seq": range(L.n), "src": L.src, "dst": L.dst,
                                  "amount": L.amt}).repartition(args.partitions))
    sources = df({"wallet": [w for w in range(L.W) if L.source_label[w] >= 0],
                  "label": pd.Series([x for x in L.source_label if x >= 0], dtype="int32")})
    membership = df({"wallet": [w for b in listed for w in b],
                     "cluster": [i for i, b in enumerate(listed) for _ in b]})
    rows = args.hot_key_rows
    ev = B.materialize(B.events(transfers, sources, membership))
    pieces = B.materialize(B.one_task(ev, args.partitions))
    deposits = B.materialize(B.stolen_deposits(transfers, sources))
    by_t = pieces.repartition(args.partitions, "t").cache()
    by_t.count()
    first = B.materialize(pieces.join(deposits.select(F.col("t").alias("s")), "s", "left_semi")
                                .select("t").distinct())
    state, _ = J.build(pieces, deposits)
    jumps, labels = state["jumps"], state["labels"]
    moves = B.materialize(
        transfers.select(F.col("dst").alias("wallet"), "seq", F.col("amount").alias("d"))
                 .unionByName(transfers.select(F.col("src").alias("wallet"), "seq",
                                               (-F.col("amount")).alias("d")))
                 .join(sources.select("wallet"), "wallet", "left_anti"))
    hot_w, hot_s, hot_h = (B.hot_wallets(transfers, rows), B.hot_keys(pieces, "s", rows),
                           B.hot_keys(jumps, "head", rows))
    print(f"hot keys over {rows:,} rows, found once per build: {hot_w.count():,} wallets, "
          f"{hot_s.count():,} inputs, {hot_h.count():,} heads")
    run = lambda make: lambda: make().write.format("noop").mode("overwrite").save()
    steps = [
        ("membership join, on wallet", "hot wallets by broadcast",
         lambda fix: B.events(transfers, sources, membership, hot_w if fix else None)),
        ("relabel, on input", "hot inputs by broadcast",
         lambda fix: B.relabel(first, by_t, deposits, hot_s if fix else None)),
        ("fill-in, on head", "hot heads by broadcast",
         lambda fix: J.fill_in(jumps, labels, hot_h if fix else None)),
        ("overdraft check, per wallet", f"{args.balance_bins} time bins",
         lambda fix: B.overdrafts(moves, args.balance_bins if fix else None)),
        ("sweep, per cluster", f"chunks of {args.chunk_events:,} events",
         lambda fix: B.routes(ev, args.partitions, args.chunk_events if fix else None)),
    ]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    cell = lambda r: f"{r[0]:,} / {r[1]:,} ({r[2]})"
    print("\nrecords read by the largest / median task (tasks) of the step's most skewed stage;"
          "\nbroadcast joins and partition coalescing off")
    print(f"{'step':28s} {'as is':>19s} {'AQE skew join':>19s} {'with the fix':>19s}  fix")
    for name, fix_name, step in steps:
        as_is = task_records(spark, opener, name, run(lambda: step(False)))
        spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1k")
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1k")
        aqe = task_records(spark, opener, name, run(lambda: step(False)))
        spark.conf.unset("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes")
        spark.conf.unset("spark.sql.adaptive.advisoryPartitionSizeInBytes")
        fixed = task_records(spark, opener, name, run(lambda: step(True)))
        print(f"{name:28s} {cell(as_is):>19s} {cell(aqe):>19s} {cell(fixed):>19s}  {fix_name}",
              flush=True)
    spark.stop()
    print(f"{'ALL EQUAL' if ok else 'MISMATCH'} ({time.time() - t0:.0f} s)")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
