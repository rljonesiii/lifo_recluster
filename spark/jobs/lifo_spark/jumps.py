"""The jump table on Spark: labels by rounds over heads, and reclusterings in one join.

    jumps(t, head, offset, length)   pass-through payment t (a route of one piece) carries
                                     units [offset, offset + length) of head's block
    hpieces(t, k, s, offset, length, base)
                                     the routes of the heads (deposits, payments that mix
                                     funds, and seeds), each input resolved to a head

A pass-through payment's label is a slice of its head's label, so labels propagate in
rounds over heads only, and one join fills in everything else. The jump table records
positions, not labels, so a reclustering changes it only at the seeds: one join re-anchors
every entry that lies under a seed.

spark/docs/jump-table.md has the design, a worked example, and the proof.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lifo_spark.build import as_sets, events, materialize, relabel, routes


def build_jumps(pieces: DataFrame) -> DataFrame:
    """Pointer jumping: each pass-through points at its input, and each round composes a
    pointer with its target's (t = s[o, o + l) and s = h[o_s, ...) give t = h[o_s + o, ...)),
    until every pointer lands on a head. Takes about log2 of the longest chain rounds."""
    single = pieces.groupBy("t").count().where("count = 1")
    jumps = materialize(pieces.join(single, "t", "left_semi")
                              .select("t", F.col("s").alias("head"), "offset", "length"))
    while True:
        hop = jumps.select(F.col("t").alias("head"), F.col("head").alias("h2"),
                           F.col("offset").alias("o2"))
        nxt = materialize(jumps.join(hop, "head", "left")
                               .select("t", F.coalesce("h2", "head").alias("head"),
                                       (F.col("offset") + F.coalesce("o2", F.lit(0)))
                                       .alias("offset"), "length"))
        if nxt.join(jumps, ["t", "head"], "left_anti").isEmpty():
            return nxt
        jumps = nxt


def resolve(pieces: DataFrame, jumps: DataFrame) -> DataFrame:
    """Head pieces: the pieces of every payment without a jump row, each input that is a
    pass-through replaced by its head."""
    via = jumps.select(F.col("t").alias("s"), F.col("head").alias("h"), F.col("offset").alias("o"))
    return (pieces.join(jumps, "t", "left_anti").join(via, "s", "left")
                  .select("t", "k", F.coalesce("h", "s").alias("s"),
                          (F.col("offset") + F.coalesce("o", F.lit(0))).alias("offset"),
                          "length", "base"))


def fill_in(jumps: DataFrame, labels: DataFrame) -> DataFrame:
    """Labels of pass-through payments: the slice [offset, offset + length) of the head's
    label, shifted to start at 0."""
    runs = labels.select(F.col("t").alias("head"), F.col("start").alias("r0"),
                         F.col("length").alias("rl"), "label")
    lo = F.greatest("offset", "r0")
    hi = F.least(F.col("offset") + F.col("length"), F.col("r0") + F.col("rl"))
    return (jumps.join(runs, "head")
                 .select("t", "offset", "label", lo.alias("lo"), hi.alias("hi"))
                 .where("lo < hi")
                 .select("t", (F.col("lo") - F.col("offset")).alias("start"),
                         (F.col("hi") - F.col("lo")).alias("length"), "label"))


def propagate_from(pieces: DataFrame, labels: DataFrame, cand: DataFrame, max_rounds=100_000):
    """Recompute cand, keep the labels that changed (the equality cutoff), then recompute
    their readers, until nothing changes. Returns (labels, rounds, changed payments)."""
    touched = None
    for rounds in range(max_rounds):
        new = materialize(relabel(cand, pieces, labels))
        changed = materialize(
            cand.join(as_sets(new, "new"), "t", "left")
                .join(as_sets(labels.join(cand, "t", "left_semi"), "old"), "t", "left")
                .where(~F.col("new").eqNullSafe(F.col("old")))
                .select("t"))
        if changed.isEmpty():
            return labels, rounds, (changed if touched is None else touched)
        touched = changed if touched is None else materialize(touched.unionByName(changed))
        labels = materialize(labels.join(changed, "t", "left_anti")
                                   .unionByName(new.join(changed, "t", "left_semi")))
        cand = materialize(pieces.join(changed.withColumnRenamed("t", "s"), "s", "left_semi")
                                 .select("t").distinct())
    raise RuntimeError("label propagation did not converge")


def build(pieces: DataFrame, deposits: DataFrame):
    """The batch build with a jump table: rounds over heads, then one fill-in join."""
    jumps = build_jumps(pieces)
    hpieces = materialize(resolve(pieces, jumps))
    first = (hpieces.join(deposits.select(F.col("t").alias("s")), "s", "left_semi")
                    .select("t").distinct())
    heads, rounds, _ = propagate_from(hpieces, deposits, first)
    labels = materialize(heads.unionByName(fill_in(jumps, heads)))
    return dict(pieces=pieces, jumps=jumps, hpieces=hpieces, labels=labels), rounds


def cluster_keys(transfers: DataFrame, membership: DataFrame) -> DataFrame:
    """(wallet, cluster, key): each wallet's cluster and a hash of the cluster's member set.
    A wallet left out of membership is its own cluster, as in build.events."""
    wallets = (transfers.select(F.col("src").alias("wallet"))
                        .union(transfers.select(F.col("dst").alias("wallet"))).distinct())
    m = (wallets.join(membership, "wallet", "left")
                .withColumn("cluster", F.coalesce("cluster", -F.col("wallet") - 1)))
    members = F.array_sort(F.collect_list("wallet")).cast("array<string>")
    keys = m.groupBy("cluster").agg(F.sha2(F.concat_ws(",", members), 256).alias("key"))
    return m.join(keys, "cluster")


def repoint(entries: DataFrame, under: DataFrame, keys, head: str) -> DataFrame:
    """Send every entry that lies under a seed to the latest such seed. An entry lies under
    a seed when it is later than the seed and its range of the seed's old head falls inside
    the seed's old range: each unit moves on along one path. Seeds are few, so broadcast them."""
    e, u = entries.alias("e"), F.broadcast(under).alias("u")
    hits = (e.join(u, F.col(f"e.{head}") == F.col("u.head"))
             .where((F.col("u.so") <= F.col("e.offset"))
                    & (F.col("e.offset") + F.col("e.length") <= F.col("u.so") + F.col("u.sl"))
                    & (F.col("e.t") > F.col("u.seed")))
             .groupBy(*[F.col(f"e.{c}") for c in keys])
             .agg(F.max("u.seed").alias("seed"), F.max_by("u.so", "u.seed").alias("so")))
    return (entries.join(hits, keys, "left")
                   .withColumn(head, F.coalesce("seed", head))
                   .withColumn("offset", F.col("offset") - F.coalesce("so", F.lit(0)))
                   .drop("seed", "so"))


def recluster(transfers, sources, old_membership, new_membership, state, partitions=8):
    """Steps 1 to 4 of spark/docs/jump-table.md for one reclustering. Step 5, the holdings, is
    build.flows_and_holdings on the new labels. Returns the new state and some counts."""
    pieces, jumps, hpieces, labels = (state[k] for k in ("pieces", "jumps", "hpieces", "labels"))

    # 1. Re-sweep only the clusters whose member set changed; diff the routes for the seeds.
    k0 = cluster_keys(transfers, old_membership).select("wallet", F.col("key").alias("k0"))
    k1 = cluster_keys(transfers, new_membership).select("wallet", "cluster", F.col("key").alias("k1"))
    moved = materialize(k0.join(k1, "wallet").where("k0 <> k1").select("wallet", "cluster"))
    paid = (transfers.join(moved.select(F.col("wallet").alias("src")), "src", "left_semi")
                     .select(F.col("seq").alias("t")))
    swept = materialize(routes(events(transfers, sources, new_membership)
                               .join(moved.select("cluster").distinct(), "cluster", "left_semi"),
                               partitions))
    route = lambda df, name: df.groupBy("t").agg(
        F.sort_array(F.collect_list(F.struct("k", "s", "offset", "length", "base"))).alias(name))
    seeds = materialize(route(pieces.join(paid, "t", "left_semi"), "old")
                        .join(route(swept, "new"), "t", "full")
                        .where(~F.col("old").eqNullSafe(F.col("new"))).select("t"))
    pieces2 = materialize(pieces.join(paid, "t", "left_anti").unionByName(swept))

    # 2. Update the jump table in one join. Seeds become heads. Only a seed that was a
    #    pass-through with readers can have entries under it; a seed that was already a
    #    head keeps its entries, and an internal transfer has no readers.
    under = (jumps.join(seeds, "t", "left_semi")
                  .join(pieces.select(F.col("s").alias("t")).distinct(), "t", "left_semi")
                  .select(F.col("t").alias("seed"), "head",
                          F.col("offset").alias("so"), F.col("length").alias("sl")))
    jumps2 = materialize(repoint(jumps.join(seeds, "t", "left_anti"), under, ["t"], "head"))
    kept = repoint(hpieces.join(seeds, "t", "left_anti"), under, ["t", "k"], "s")
    fresh = resolve(swept.join(seeds, "t", "left_semi"), jumps2)
    hpieces2 = materialize(kept.unionByName(fresh))

    # 3. Propagate over heads only, starting from the seeds.
    heads, rounds, touched = propagate_from(hpieces2, labels, seeds)

    # 4. Fill in the pass-through payments whose head's label changed, in one join.
    stale = jumps2.join(touched.withColumnRenamed("t", "head"), "head", "left_semi")
    labels2 = materialize(heads.join(stale.select("t"), "t", "left_anti")
                               .unionByName(fill_in(stale, heads)))

    counts = dict(moved=moved.count(), seeds=seeds.count(), under=under.count(),
                  repointed=jumps2.join(jumps, ["t", "head"], "left_anti").count(),
                  repointed_pieces=kept.join(hpieces, ["t", "k", "s"], "left_anti").count(),
                  rounds=rounds, heads_changed=touched.count(), filled=stale.count())
    return dict(pieces=pieces2, jumps=jumps2, hpieces=hpieces2, labels=labels2), counts
