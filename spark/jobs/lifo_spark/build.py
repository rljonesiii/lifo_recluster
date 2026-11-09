"""Batch build on Spark: routes for every cluster, stolen labels, net flows and holdings.

    spark-submit build.py --transfers URI --sources URI --membership URI --out URI \\
        [--format delta|parquet] [--partitions 512] [--buckets 256] [--checkpoint-dir URI] \\
        [--labels-checkpoint-every 1]

Inputs (Delta or Parquet, same --format):
  transfers   seq long, src long, dst long, amount long   one row per transfer; seq is the
                                                           global order (block, tx, output)
  sources     wallet long, label int                       0 clean, >= 1 stolen
  membership  wallet long, cluster long                    cluster ids >= 0; a wallet left
                                                           out is its own cluster
Outputs under --out: pieces, labels (stolen runs only), flows, holdings.

Every step is a function from DataFrames to DataFrames; only main() reads and writes.
"""
import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lifo_spark.sweep import IN, OUT, READ, PIECES_SCHEMA, sweep_partitions


def events(transfers: DataFrame, sources: DataFrame, membership: DataFrame) -> DataFrame:
    """One event per (cluster, transfer): an inflow, an outflow or an internal read."""
    def cluster_of(side):
        m = membership.select(F.col("wallet").alias(side), F.col("cluster").alias(f"c_{side}"))
        return lambda df: (df.join(m, side, "left")
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


def routes(ev: DataFrame, partitions: int) -> DataFrame:
    """Sweep every cluster in its own task: pieces (cluster, t, k, s, offset, length, base)."""
    return (ev.repartition(partitions, "cluster")
              .sortWithinPartitions("cluster", "seq")
              .mapInPandas(sweep_partitions, PIECES_SCHEMA))


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


def relabel(candidates: DataFrame, pieces: DataFrame, labels: DataFrame) -> DataFrame:
    """New stolen runs of each candidate from all of its pieces (the labels-from-pieces
    lemma): each piece copies the overlap of its input's stolen runs, shifted to t."""
    inputs = labels.select(F.col("t").alias("s"), F.col("start").alias("in_start"),
                           F.col("length").alias("in_len"), "label")
    lo = F.greatest("offset", "in_start")
    hi = F.least(F.col("offset") + F.col("length"), F.col("in_start") + F.col("in_len"))
    return coalesce_runs(
        pieces.join(candidates, "t", "left_semi")
              .join(inputs, "s")
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


def propagate(pieces: DataFrame, labels: DataFrame, changed: DataFrame,
              labels_checkpoint_every=1, max_rounds=100_000):
    """Rounds to a fixed point: recompute every reader of a changed label, keep the ones
    whose label changed (the equality cutoff). Routes point back in time (Theorem 6), so
    the graph is acyclic and the rounds end. From the stolen deposits, the rounds that
    change a label number one less than the most hops a stolen unit travels.

    Each round materializes candidates, new and changed. A round reads the last round's
    frames several times, so without that the plan grows about ninefold a round. labels
    is materialized every labels_checkpoint_every rounds; in between, its plan grows by
    two joins and a union a round. Returns (labels, rounds)."""
    if labels_checkpoint_every < 1:
        raise ValueError("labels_checkpoint_every must be 1 or more")
    due = lambda r: (r + 1) % labels_checkpoint_every == 0  # materialize labels after round r
    for rounds in range(max_rounds):
        candidates = materialize(pieces.join(changed.withColumnRenamed("t", "s"), "s", "left_semi")
                                       .select("t").distinct())
        new = materialize(relabel(candidates, pieces, labels))
        changed = materialize(
            candidates.join(as_sets(new, "new"), "t", "left")
                      .join(as_sets(labels.join(candidates, "t", "left_semi"), "old"), "t", "left")
                      .where(~F.col("new").eqNullSafe(F.col("old")))
                      .select("t"))
        if changed.isEmpty():
            return (labels if due(rounds - 1) else materialize(labels)), rounds
        update = (labels.join(changed, "t", "left_anti")
                        .unionByName(new.join(changed, "t", "left_semi")))
        labels = materialize(update) if due(rounds) else update
    raise RuntimeError("label propagation did not converge")


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


def build(transfers, sources, membership, partitions=512, labels_checkpoint_every=1):
    pieces = routes(events(transfers, sources, membership), partitions).cache()
    deposits = materialize(stolen_deposits(transfers, sources))
    labels, rounds = propagate(pieces, deposits, deposits.select("t"), labels_checkpoint_every)
    flows, holdings = flows_and_holdings(transfers, sources, membership, labels)
    return dict(pieces=pieces, labels=labels, flows=flows, holdings=holdings), rounds


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
    args = ap.parse_args()
    spark = SparkSession.builder.appName("lifo-build").getOrCreate()
    if args.checkpoint_dir:
        spark.sparkContext.setCheckpointDir(args.checkpoint_dir)
    read = lambda name: spark.read.format(args.format).load(getattr(args, name))
    out, rounds = build(read("transfers"), read("sources"), read("membership"), args.partitions,
                        args.labels_checkpoint_every)
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
