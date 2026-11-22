"""Batch build on Spark: routes for every cluster, stolen labels, net flows, and holdings.

    spark-submit build.py --transfers URI --sources URI --membership URI --out URI \\
        [--format delta|parquet] [--partitions 512] [--buckets 256] [--checkpoint-dir URI] \\
        [--labels-checkpoint-every 1] [--jump-table] [--hot-key-rows N] [--chunk-events N] \\
        [--balance-bins N]

Inputs (Delta or Parquet, same --format):
  transfers   seq long, src long, dst long, amount long   one row per transfer; seq is the
                                                           global order (block, tx, output)
  sources     wallet long, label int                       0 clean, >= 1 stolen
  membership  wallet long, cluster long                    cluster ids >= 0; a wallet left
                                                           out is its own cluster
The inputs must be a valid ledger and clustering, by the rules lifo.Ledger enforces, with
unique keys. main() checks them before the build; see violations().
Outputs under --out: pieces, labels (stolen runs only), flows, and holdings; with
--jump-table, also the jump table (jumps) and the heads' pieces (hpieces).

Skew: --hot-key-rows sends keys with more rows than that (hub wallets, hot inputs, hot
heads) down a broadcast path in the joins on them (split_join); --chunk-events sweeps
clusters with more events than that in time chunks (lifo_spark.chunks); --balance-bins
checks running balances in time bins, so no wallet's moves sit in one window.

Every step is a function from DataFrames to DataFrames; only main() reads and writes.
"""
import argparse
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lifo_spark.sweep import IN, OUT, READ, PIECES_SCHEMA, sweep_partitions


def events(transfers: DataFrame, sources: DataFrame, membership: DataFrame,
           hot=None) -> DataFrame:
    """One event per (cluster, transfer): an inflow, an outflow, or an internal read. hot,
    if given, is hot_wallets: they find their clusters by broadcast (split_join)."""
    def cluster_of(side):
        m = membership.select(F.col("wallet").alias(side), F.col("cluster").alias(f"c_{side}"))
        h = None if hot is None else hot.withColumnRenamed("wallet", side)
        return lambda df: (split_join(df, m, side, h, "left")
                             .withColumn(f"c_{side}", F.coalesce(f"c_{side}", -F.col(side) - 1)))
    from_source = F.broadcast(sources.select(F.col("wallet").alias("src"),
                                             F.lit(True).alias("from_source")))
    x = (transfers.join(from_source, "src", "left")
                  .withColumn("from_source", F.coalesce("from_source", F.lit(False)))
                  .transform(cluster_of("src"))
                  .transform(cluster_of("dst")))
    inflows = (x.where("from_source OR c_src <> c_dst")
                .select(F.col("c_dst").alias("cluster"), "seq",
                        F.lit(IN).cast("byte").alias("kind"), "amount"))
    sent = (x.where("NOT from_source")
             .select(F.col("c_src").alias("cluster"), "seq",
                     F.when(F.col("c_src") == F.col("c_dst"), READ).otherwise(OUT)
                      .cast("byte").alias("kind"), "amount"))
    return inflows.unionByName(sent)


def one_task(ev: DataFrame, partitions: int) -> DataFrame:
    """Sweep every cluster in its own task: pieces (cluster, t, k, s, offset, length, base)."""
    return (ev.repartition(partitions, "cluster")
              .sortWithinPartitions("cluster", "seq")
              .mapInPandas(sweep_partitions, PIECES_SCHEMA))


def routes(ev: DataFrame, partitions: int, chunk_events=None, log=None) -> DataFrame:
    """The pieces of every cluster's sweep. With chunk_events, clusters with more events
    than that are swept in time chunks of about that many events (lifo_spark.chunks),
    which gives the same pieces; the rest are swept one cluster to a task."""
    if chunk_events is None:
        return one_task(ev, partitions)
    from lifo_spark import chunks                     # chunks imports this module
    ev = materialize(ev)
    giant = chunks.giant_clusters(ev, chunk_events)
    g = F.broadcast(giant.select("cluster"))
    return (one_task(ev.join(g, "cluster", "left_anti"), partitions)
            .unionByName(chunks.routes(ev.join(g, "cluster", "left_semi"), giant,
                                       chunk_events, partitions, log)))


def stolen_deposits(transfers: DataFrame, sources: DataFrame) -> DataFrame:
    """Labels of deposits from stolen sources: one run covering the whole transfer."""
    stolen = F.broadcast(sources.where("label >= 1")
                                .select(F.col("wallet").alias("src"), "label"))
    return transfers.join(stolen, "src").select(
        F.col("seq").alias("t"), F.lit(0).cast("long").alias("start"),
        F.col("amount").alias("length"), "label")


def coalesce_runs(runs: DataFrame) -> DataFrame:
    """Merge adjacent runs of one transaction that carry the same label."""
    w = Window.partitionBy("t").orderBy("start")
    prev_end = F.lag(F.col("start") + F.col("length")).over(w)
    prev_label = F.lag("label").over(w)
    breaks = (prev_end.isNull() | (prev_end != F.col("start"))
              | (prev_label != F.col("label"))).cast("int")
    return (runs.withColumn("brk", breaks)
                .withColumn("g", F.sum("brk").over(w.rowsBetween(Window.unboundedPreceding, 0)))
                .groupBy("t", "g")
                .agg(F.min("start").alias("start"), F.sum("length").alias("length"),
                     F.first("label").alias("label"))
                .drop("g"))


def relabel(candidates: DataFrame, pieces: DataFrame, labels: DataFrame, hot=None) -> DataFrame:
    """New stolen runs of each candidate from all of its pieces (the labels-from-pieces
    lemma): each piece copies the overlap of its input's stolen runs, shifted to t. hot,
    if given, holds hot inputs s, joined by broadcast (split_join)."""
    inputs = labels.select(F.col("t").alias("s"), F.col("start").alias("in_start"),
                           F.col("length").alias("in_len"), "label")
    lo = F.greatest("offset", "in_start")
    hi = F.least(F.col("offset") + F.col("length"), F.col("in_start") + F.col("in_len"))
    return coalesce_runs(
        split_join(pieces.join(candidates, "t", "left_semi"), inputs, "s", hot)
              .withColumn("lo", lo).withColumn("hi", hi)
              .where("lo < hi")
              .select("t", (F.col("base") + F.col("lo") - F.col("offset")).alias("start"),
                      (F.col("hi") - F.col("lo")).alias("length"), "label"))


def as_sets(runs: DataFrame, name: str) -> DataFrame:
    return runs.groupBy("t").agg(
        F.array_sort(F.collect_list(F.struct("start", "length", "label"))).alias(name))


def materialize(df: DataFrame) -> DataFrame:
    """Cut the lineage each round: a reliable checkpoint to object storage when a
    checkpoint directory is set (on Kubernetes), an executor-local one otherwise."""
    reliable = df.sparkSession.sparkContext.getCheckpointDir()
    return df.checkpoint() if reliable else df.localCheckpoint()


def hot_keys(df: DataFrame, key: str, rows) -> DataFrame:
    """The keys with more than `rows` rows in df, materialized, or None when rows is None.
    split_join broadcasts them, so the threshold should leave few."""
    if rows is None:
        return None
    return materialize(df.groupBy(key).count().where(F.col("count") > rows).select(key))


def hot_wallets(transfers: DataFrame, rows) -> DataFrame:
    """Hub wallets: the wallets in more than `rows` transfers, or None when rows is None."""
    return hot_keys(transfers.select(F.col("src").alias("wallet"))
                             .unionByName(transfers.select(F.col("dst").alias("wallet"))),
                    "wallet", rows)


def split_join(big: DataFrame, small: DataFrame, key: str, hot, how="inner") -> DataFrame:
    """big joined with small on key. Rows of hot keys join small's rows for those keys by
    broadcast, where the rows already sit, instead of piling into one shuffle partition;
    the other rows join as usual. The halves split the keys, so their union is the join."""
    if hot is None:
        return big.join(small, key, how)
    h = F.broadcast(hot)
    return (big.join(h, key, "left_semi")
               .join(F.broadcast(small.join(h, key, "left_semi")), key, how)
               .unionByName(big.join(h, key, "left_anti").join(small, key, how)))


def keyed(pieces: DataFrame, partitions: int):
    """The pieces twice, hash-partitioned and cached: by s, to find the readers of the
    changed labels, and by t, to recompute those readers. The pieces are the static side of
    every round's joins, so with this layout a round shuffles the frontier, not the pieces."""
    return tuple(pieces.repartition(partitions, key).cache() for key in ("s", "t"))


def plural(n: int, noun: str) -> str:
    """'1 piece', '2,644 pieces': for the log."""
    return f"{n:,} {noun}{'' if n == 1 else 's'}"


def round_line(number: int, changed: int, work: DataFrame, labels, start: float) -> str:
    """One log line for a label round: the labels that changed, the input that the most of
    the round's pieces read (the frontier's hottest key), the labels' partitions when they
    were materialized, and the time."""
    hot = work.groupBy("s").count().orderBy(F.desc("count"), "s").first()
    parts = labels.rdd.getNumPartitions() if labels is not None else None
    readers = f"hottest input {hot['s']} read by {plural(hot['count'], 'piece')}; " if hot else ""
    return (f"round {number}: {plural(changed, 'label')} changed; " + readers
            + (f"labels in {plural(parts, 'partition')}; " if parts else "")
            + f"{time.time() - start:.1f} s")


def propagate(pieces: DataFrame, labels: DataFrame, changed: DataFrame,
              labels_checkpoint_every=1, max_rounds=100_000, partitions=None, log=None,
              hot=None):
    """Rounds to a fixed point: recompute every reader of a changed label, keep the ones
    whose label changed (the equality cutoff). Routes point back in time (Theorem 6), so
    the graph is acyclic and the rounds end. From the stolen deposits, the rounds that
    change a label number one less than the most hops a stolen unit travels.

    Each round materializes candidates, new, and changed. A round reads the last round's
    frames several times, so without that the plan grows about ninefold a round. labels
    is materialized every labels_checkpoint_every rounds; in between, its plan grows by
    two joins and a union a round. A union keeps both sides' partitions, so a broadcast
    update would add partitions every round; a coalesce caps them when labels are
    materialized, without a shuffle.

    The pieces are partitioned once by each join key (keyed), and the cached copies are
    dropped at the end. hot, if given, holds hot inputs for relabel. log, if given, gets
    one line a round. Returns (labels, rounds)."""
    if labels_checkpoint_every < 1:
        raise ValueError("labels_checkpoint_every must be 1 or more")
    due = lambda r: (r + 1) % labels_checkpoint_every == 0  # materialize labels after round r
    n = partitions or int(pieces.sparkSession.conf.get("spark.sql.shuffle.partitions"))
    by_s, by_t = keyed(pieces, n)
    try:
        for rounds in range(max_rounds):
            start = time.time()
            readers = by_s.join(changed.withColumnRenamed("t", "s"), "s", "left_semi")
            candidates = materialize(readers.select("t").distinct())
            new = materialize(relabel(candidates, by_t, labels, hot))
            old = as_sets(labels.join(candidates, "t", "left_semi"), "old")
            changed = materialize(candidates.join(as_sets(new, "new"), "t", "left")
                                            .join(old, "t", "left")
                                            .where(~F.col("new").eqNullSafe(F.col("old")))
                                            .select("t"))
            size = changed.count()
            work = by_t.join(candidates, "t", "left_semi")
            if size == 0:
                labels = labels if due(rounds - 1) else materialize(labels.coalesce(n))
                if log:
                    log(round_line(rounds + 1, 0, work, labels, start))
                return labels, rounds
            update = (labels.join(changed, "t", "left_anti")
                            .unionByName(new.join(changed, "t", "left_semi")))
            labels = materialize(update.coalesce(n)) if due(rounds) else update
            if log:
                log(round_line(rounds + 1, size, work, labels if due(rounds) else None, start))
        raise RuntimeError("label propagation did not converge")
    finally:
        by_s.unpersist()
        by_t.unpersist()


def flows_and_holdings(transfers, sources, membership, labels):
    """Net stolen flow per wallet, summed per cluster (Theorem 5: no stacks needed)."""
    sigma = labels.groupBy("t").agg(F.sum("length").alias("sigma"))
    tainted = transfers.join(sigma.withColumnRenamed("t", "seq"), "seq")
    is_source = sources.select(F.col("wallet").alias("src"))
    flows = (tainted.select(F.col("dst").alias("wallet"), F.col("sigma").alias("d"))
                    .unionByName(tainted.join(is_source, "src", "left_anti")
                                        .select(F.col("src").alias("wallet"),
                                                (-F.col("sigma")).alias("d")))
                    .groupBy("wallet").agg(F.sum("d").alias("net_stolen")))
    holdings = (flows.join(membership, "wallet", "left")
                     .withColumn("cluster", F.coalesce("cluster", -F.col("wallet") - 1))
                     .groupBy("cluster").agg(F.sum("net_stolen").alias("stolen"))
                     .where("stolen <> 0"))
    return flows, holdings


def build(transfers, sources, membership, partitions=512, labels_checkpoint_every=1,
          jump_table=False, log=None, hot_key_rows=None, chunk_events=None):
    """The batch build. With jump_table, labels propagate over heads only (jumps.build), and
    the outputs add the jump table and the heads' pieces, which a reclustering starts from.
    hot_key_rows sends keys with more rows than that down a broadcast path in the joins on
    wallet, input, and head; chunk_events sweeps clusters with more events than that in
    time chunks. Neither changes the outputs."""
    hot = hot_wallets(transfers, hot_key_rows)
    if log and hot is not None:
        log(f"{plural(hot.count(), 'hot wallet')}, each in more than "
            f"{plural(hot_key_rows, 'transfer')}")
    pieces = routes(events(transfers, sources, membership, hot), partitions, chunk_events,
                    log).cache()
    deposits = materialize(stolen_deposits(transfers, sources))
    if jump_table:
        from lifo_spark import jumps                  # jumps imports this module
        state, rounds = jumps.build(pieces, deposits, partitions=partitions, log=log,
                                    hot_key_rows=hot_key_rows)
    else:
        hot = hot_keys(pieces, "s", hot_key_rows)
        if log and hot is not None:
            log(f"{plural(hot.count(), 'hot input')}, each read by more than "
                f"{plural(hot_key_rows, 'piece')}")
        labels, rounds = propagate(pieces, deposits, deposits.select("t"),
                                   labels_checkpoint_every, partitions=partitions, log=log,
                                   hot=hot)
        state = dict(pieces=pieces, labels=labels)
    flows, holdings = flows_and_holdings(transfers, sources, membership, state["labels"])
    return dict(state, flows=flows, holdings=holdings), rounds


def overdrafts(moves: DataFrame, bins=None) -> DataFrame:
    """The moves (wallet, seq, d) after which their wallet's balance is negative. Without
    bins, one window per wallet, so a hub wallet's moves all sit in one task. With bins,
    the seq range is cut into that many bins of equal width, and (sum, lowest running
    balance) per (wallet, bin) composes like the sweep's chunk summaries: a scan over a
    wallet's bins gives each bin's opening balance, and only the bins whose opening plus
    lowest is negative are checked row by row."""
    if bins is None:
        balance = F.sum("d").over(Window.partitionBy("wallet").orderBy("seq"))
        return moves.withColumn("balance", balance).where("balance < 0")
    span = moves.agg(F.min("seq").alias("lo"),
                     ((F.max("seq") - F.min("seq") + 1) / bins).alias("width"))
    bin_of = F.least(F.floor((F.col("seq") - F.col("lo")) / F.col("width")), F.lit(bins - 1))
    within = Window.partitionBy("wallet", "bin").orderBy("seq")
    before = (Window.partitionBy("wallet").orderBy("bin")
                    .rowsBetween(Window.unboundedPreceding, -1))
    run = (moves.crossJoin(F.broadcast(span)).withColumn("bin", bin_of).drop("lo", "width")
                .withColumn("run", F.sum("d").over(within)))
    low = (run.groupBy("wallet", "bin")
              .agg(F.sum("d").alias("total"), F.min("run").alias("lowest"))
              .withColumn("opening", F.coalesce(F.sum("total").over(before), F.lit(0)))
              .where(F.col("opening") + F.col("lowest") < 0)
              .select("wallet", "bin", "opening"))
    return (run.join(low, ["wallet", "bin"])
               .select("wallet", "seq", "d", (F.col("opening") + F.col("run")).alias("balance"))
               .where("balance < 0"))


def violations(transfers: DataFrame, sources: DataFrame, membership: DataFrame,
               bins=None) -> dict:
    """Rows that break a rule of a valid ledger and clustering, by rule: the rules
    lifo.Ledger enforces, and the unique keys the joins assume. The theorems hold only for
    valid inputs, and the sweep catches only a cluster-level overdraft, which a clustering
    can hide. bins is overdrafts' number of time bins."""
    src = sources.select("wallet")
    dup = lambda df, key: df.join(df.groupBy(key).count().where("count > 1"), key, "left_semi")
    moves = (transfers.select(F.col("dst").alias("wallet"), "seq", F.col("amount").alias("d"))
                      .unionByName(transfers.select(F.col("src").alias("wallet"), "seq",
                                                    (-F.col("amount")).alias("d")))
                      .join(src, "wallet", "left_anti"))
    return {
        "seq not unique": dup(transfers, "seq"),
        "amount not positive": transfers.where("amount <= 0"),
        "self-transfer": transfers.where("src = dst"),
        "negative wallet id": transfers.where("src < 0 OR dst < 0"),
        "wallet overdraws": overdrafts(moves, bins),
        "source receives": transfers.join(src.toDF("dst"), "dst", "left_semi"),
        "source listed twice": dup(sources, "wallet"),
        "negative label": sources.where("label < 0"),
        "wallet in two clusters": dup(membership, "wallet"),
        "negative cluster id": membership.where("cluster < 0"),
        "source clustered": membership.join(src, "wallet", "left_semi"),
    }


def validate(transfers: DataFrame, sources: DataFrame, membership: DataFrame,
             bins=None) -> None:
    """Stop before the build if any rule is broken, with the number of rows per rule."""
    counts = {rule: df.count()
              for rule, df in violations(transfers, sources, membership, bins).items()}
    bad = {rule: n for rule, n in counts.items() if n}
    if bad:
        raise ValueError(f"invalid input, rows by rule: {bad}")


def main():
    ap = argparse.ArgumentParser()
    for name in ("transfers", "sources", "membership", "out"):
        ap.add_argument(f"--{name}", required=True)
    ap.add_argument("--format", default="delta", choices=["delta", "parquet"])
    ap.add_argument("--partitions", type=int, default=512)
    ap.add_argument("--buckets", type=int, default=256)
    ap.add_argument("--checkpoint-dir", help="reliable storage for the per-round checkpoints")
    ap.add_argument("--labels-checkpoint-every", type=int, default=1, metavar="N",
                    help="materialize the labels table every N rounds instead of every round")
    ap.add_argument("--jump-table", action="store_true",
                    help="propagate labels over heads only, and write the jump table too; "
                         "labels are then materialized every round")
    ap.add_argument("--hot-key-rows", type=int, metavar="N",
                    help="join keys with more than N rows (hub wallets, hot inputs, hot heads) "
                         "by broadcast, where their rows sit")
    ap.add_argument("--chunk-events", type=int, metavar="N",
                    help="sweep clusters with more than N events in time chunks of about N")
    ap.add_argument("--balance-bins", type=int, metavar="N",
                    help="check running balances in N time bins, not one window per wallet")
    args = ap.parse_args()
    spark = SparkSession.builder.appName("lifo-build").getOrCreate()
    if args.checkpoint_dir:
        spark.sparkContext.setCheckpointDir(args.checkpoint_dir)
    read = lambda name: spark.read.format(args.format).load(getattr(args, name))
    transfers, sources, membership = (read(n) for n in ("transfers", "sources", "membership"))
    validate(transfers, sources, membership, args.balance_bins)
    out, rounds = build(transfers, sources, membership, args.partitions,
                        args.labels_checkpoint_every, args.jump_table,
                        log=lambda line: print(f"lifo-build: {line}", flush=True),
                        hot_key_rows=args.hot_key_rows, chunk_events=args.chunk_events)
    out["pieces"] = (out["pieces"]
                     .withColumn("bucket", F.pmod(F.xxhash64("cluster"), F.lit(args.buckets)))
                     .repartition(args.buckets, "bucket").sortWithinPartitions("cluster", "s"))
    for name, df in out.items():
        writer = df.write.format(args.format).mode("overwrite")
        (writer.partitionBy("bucket") if name == "pieces" else writer).save(f"{args.out}/{name}")
    print(f"lifo-build: done in {rounds} label rounds")
    spark.stop()


if __name__ == "__main__":
    main()
