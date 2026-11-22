"""A giant cluster's sweep, in time chunks.

A cluster's sweep is sequential, so build.routes sweeps each cluster in one task, and a
giant cluster, such as an exchange with millions of events, makes one straggler task
that no partitioning of the cluster key can split. Time can. Under LIFO each unit pushed
onto the stack is matched with the event that pops it, the way an open parenthesis
matches its close, so each time chunk can be swept from an empty stack (sweep.py's
chunk_partitions) and summarized by (D, P): the units it pops below its own start, and
the units it leaves. Summaries compose associatively. A scan over them gives each chunk's
start height and low-water mark, and two keyed interval joins settle the few payments
that reach below their chunk's start (the debts).

spark/docs/chunked-sweep.md has the design and the proof.
"""
from itertools import groupby

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lifo_spark.build import materialize, plural
from lifo_spark.sweep import BLOCK, CHUNKS_SCHEMA, DEBT, PIECE, SUM, chunk_partitions

HEIGHTS_SCHEMA = "cluster long, chunk int, H long, L long"
STEPS_SCHEMA = "cluster long, chunk int, k int, lo long, hi long"


def giant_clusters(ev: DataFrame, chunk_events: int) -> DataFrame:
    """(cluster, n, lo, hi): the clusters with more than chunk_events events, their event
    counts, and their first and last seq."""
    return materialize(ev.groupBy("cluster")
                         .agg(F.count("*").alias("n"), F.min("seq").alias("lo"),
                              F.max("seq").alias("hi"))
                         .where(F.col("n") > chunk_events))


def with_chunks(ev: DataFrame, giant: DataFrame, chunk_events: int, bins_per_chunk=16):
    """The giant clusters' events, each with a chunk id, in chunks of about chunk_events
    events. A cluster's seq range is cut into bins_per_chunk bins of equal width per chunk,
    and the bins, whose event counts are exact, are grouped in order on the driver. A
    retry gets the same counts, so the same chunks."""
    nb = F.ceil(F.col("n") / chunk_events) * bins_per_chunk
    g = giant.select("cluster", "lo", nb.alias("nb"),
                     ((F.col("hi") - F.col("lo") + 1) / nb).alias("width"))
    binned = (ev.join(F.broadcast(g), "cluster")
                .withColumn("bin", F.least(F.floor((F.col("seq") - F.col("lo")) / F.col("width")),
                                           F.col("nb") - 1))
                .drop("lo", "nb", "width"))
    counts = binned.groupBy("cluster", "bin").count().orderBy("cluster", "bin").collect()
    ids = []
    for cluster, bins in groupby(counts, key=lambda r: r["cluster"]):
        chunk, filled = 0, 0
        for r in bins:
            if filled and filled + r["count"] > chunk_events:
                chunk, filled = chunk + 1, 0
            ids.append((cluster, r["bin"], chunk))
            filled += r["count"]
    ids = ev.sparkSession.createDataFrame(ids, "cluster long, bin long, chunk int")
    return binned.join(F.broadcast(ids), ["cluster", "bin"]).drop("bin")


def stack_scan(sums):
    """Driver side. sums: rows (cluster, chunk, D, P, deep) in (cluster, chunk) order, one
    per chunk, where the chunk pops D units below its start, leaves P, and owes debts down
    to depth deep. Composing the summaries gives each chunk's start height H and low-water
    mark L = H - D. Height h below chunk j's start was left by the last earlier chunk k
    with L_k <= h, so the heights below H_j form a staircase over the strict minima of L
    (all nearest smaller values, kept with a monotone stack). Returns heights, one
    (cluster, chunk, H, L) per chunk, and steps (cluster, chunk, k, lo, hi): for each chunk
    with debts, the heights [lo, hi) that chunk k left, down to its deepest debt."""
    heights, steps = [], []
    for cluster, chunks in groupby(sums, key=lambda r: r["cluster"]):
        h, mins = 0, []
        for r in chunks:
            if r["deep"] > h:
                raise ValueError(f"cluster {cluster} pays out more than it holds "
                                 f"(chunk {r['chunk']})")
            top, floor = h, h - r["deep"]
            for k, low_k in reversed(mins):
                if top <= floor:
                    break
                steps.append((cluster, r["chunk"], k, low_k, top))
                top = low_k
            low = h - r["D"]
            heights.append((cluster, r["chunk"], h, low))
            while mins and mins[-1][1] >= low:
                mins.pop()
            mins.append((r["chunk"], low))
            h = low + r["P"]
    return heights, steps


def settle(local: DataFrame, heights: DataFrame, steps: DataFrame) -> DataFrame:
    """Pieces for the debts. A debt at depths [o, o + n) below chunk j's start covers the
    heights [H_j - o - n, H_j - o); the staircase says which earlier chunk k left each of
    those heights, and chunk k's leftover blocks say which inflow and which of its units.
    Both joins have an equality key, (cluster, chunk) and then (cluster, k), and the few
    debts are broadcast. Returns pieces without k, which chunks.routes renumbers."""
    h = F.broadcast(heights)
    debts = (local.where(F.col("rec") == DEBT).join(h, ["cluster", "chunk"])
                  .select("cluster", "chunk", "t",
                          (F.col("H") - F.col("offset") - F.col("length")).alias("d0"),
                          (F.col("H") - F.col("offset")).alias("d1")))
    owed = (debts.join(F.broadcast(steps), ["cluster", "chunk"])
                 .where((F.col("lo") < F.col("d1")) & (F.col("d0") < F.col("hi")))
                 .drop("chunk"))
    blocks = (local.where(F.col("rec") == BLOCK).join(h, ["cluster", "chunk"])
                   .select("cluster", F.col("chunk").alias("k"), "s",
                           (F.col("L") + F.col("offset")).alias("b0"),
                           (F.col("L") + F.col("offset") + F.col("length")).alias("b1")))
    lo, hi = F.greatest("d0", "lo", "b0"), F.least("d1", "hi", "b1")
    return (blocks.join(F.broadcast(owed), ["cluster", "k"])
                  .where(lo < hi)
                  .select("cluster", "t", F.lit(None).cast("int").alias("k"), "s",
                          (lo - F.col("b0")).alias("offset"), (hi - lo).alias("length"),
                          (lo - F.col("d0")).alias("base")))


def routes(ev: DataFrame, giant: DataFrame, chunk_events: int, partitions: int, log=None):
    """Pieces of the giant clusters, equal to their one-task sweeps: each chunk swept in its
    own task, the summaries scanned on the driver, the debts settled, and the pieces of
    the payments that had debts renumbered by base."""
    local = materialize(with_chunks(ev, giant, chunk_events)
                        .repartition(partitions, "cluster", "chunk")
                        .sortWithinPartitions("cluster", "chunk", "seq")
                        .mapInPandas(chunk_partitions, CHUNKS_SCHEMA))
    sums = (local.where(F.col("rec") == SUM)
                 .select("cluster", "chunk", F.col("offset").alias("D"),
                         F.col("length").alias("P"), F.col("base").alias("deep"))
                 .orderBy("cluster", "chunk").collect())
    heights, steps = stack_scan(sums)
    spark = ev.sparkSession
    owed = settle(local, spark.createDataFrame(heights, HEIGHTS_SCHEMA),
                  spark.createDataFrame(steps, STEPS_SCHEMA))
    inside = (local.where(F.col("rec") == PIECE)
                   .select("cluster", "t", "k", "s", "offset", "length", "base"))
    owing = F.broadcast(local.where(F.col("rec") == DEBT).select("t").distinct())
    k = (F.row_number().over(Window.partitionBy("t").orderBy("base")) - 1).cast("int")
    if log:
        owe = sum(1 for r in sums if r["deep"])
        log(f"{plural(giant.count(), 'giant cluster')} swept in {plural(len(sums), 'chunk')}; "
            f"debts in {plural(owe, 'chunk')}, settled over "
            f"{plural(len(steps), 'staircase step')}")
    return (inside.join(owing, "t", "left_anti")
                  .unionByName(inside.join(owing, "t", "left_semi").unionByName(owed)
                                     .withColumn("k", k)))
