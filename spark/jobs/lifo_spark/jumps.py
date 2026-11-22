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
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lifo_spark.build import (as_sets, events, hot_keys, hot_wallets, keyed, materialize,
                              plural, relabel, round_line, routes, split_join)


def build_jumps(pieces: DataFrame, hot_rows=None) -> DataFrame:
    """Pointer jumping: each pass-through points at its input, and each round composes a
    pointer with its target's (t = s[o, o + l) and s = h[o_s, ...) give t = h[o_s + o, ...)),
    until every pointer lands on a head. Takes about log2 of the longest chain rounds.
    Pointers pile onto heads as they jump, so with hot_rows, each round's heads with more
    pointers than that are joined by broadcast."""
    single = pieces.groupBy("t").count().where("count = 1")
    jumps = materialize(pieces.join(single, "t", "left_semi")
                              .select("t", F.col("s").alias("head"), "offset", "length"))
    while True:
        hop = jumps.select(F.col("t").alias("head"), F.col("head").alias("h2"),
                           F.col("offset").alias("o2"))
        hot = hot_keys(jumps, "head", hot_rows)
        nxt = materialize(split_join(jumps, hop, "head", hot, "left")
                               .select("t", F.coalesce("h2", "head").alias("head"),
                                       (F.col("offset") + F.coalesce("o2", F.lit(0)))
                                       .alias("offset"), "length"))
        if nxt.join(jumps, ["t", "head"], "left_anti").isEmpty():
            return nxt
        jumps = nxt


def resolve(pieces: DataFrame, jumps: DataFrame, hot=None) -> DataFrame:
    """Head pieces: the pieces of every payment without a jump row, each input that is a
    pass-through replaced by its head. hot, if given, holds hot inputs s (split_join)."""
    via = jumps.select(F.col("t").alias("s"), F.col("head").alias("h"), F.col("offset").alias("o"))
    return (split_join(pieces.join(jumps, "t", "left_anti"), via, "s", hot, "left")
                  .select("t", "k", F.coalesce("h", "s").alias("s"),
                          (F.col("offset") + F.coalesce("o", F.lit(0))).alias("offset"),
                          "length", "base"))


def fill_in(jumps: DataFrame, labels: DataFrame, hot=None) -> DataFrame:
    """Labels of pass-through payments: the slice [offset, offset + length) of the head's
    label, shifted to start at 0. Path compression piles jump rows onto heads, so hot, if
    given, holds hot heads, joined by broadcast (split_join)."""
    runs = labels.select(F.col("t").alias("head"), F.col("start").alias("r0"),
                         F.col("length").alias("rl"), "label")
    lo = F.greatest("offset", "r0")
    hi = F.least(F.col("offset") + F.col("length"), F.col("r0") + F.col("rl"))
    return (split_join(jumps, runs, "head", hot)
                 .select("t", "offset", "label", lo.alias("lo"), hi.alias("hi"))
                 .where("lo < hi")
                 .select("t", (F.col("lo") - F.col("offset")).alias("start"),
                         (F.col("hi") - F.col("lo")).alias("length"), "label"))


def propagate_from(pieces: DataFrame, labels: DataFrame, cand: DataFrame, max_rounds=100_000,
                   partitions=None, log=None, hot=None):
    """Recompute cand, keep the labels that changed (the equality cutoff), then recompute
    their readers, until nothing changes. As in build.propagate, the head pieces are
    partitioned once by each join key, a coalesce caps the partitions of labels and of the
    changed set, and hot, if given, holds hot inputs for relabel. Returns (labels, rounds,
    changed payments)."""
    n = partitions or int(pieces.sparkSession.conf.get("spark.sql.shuffle.partitions"))
    by_s, by_t = keyed(pieces, n)
    touched = None
    try:
        for rounds in range(max_rounds):
            start = time.time()
            work = by_t.join(cand, "t", "left_semi")
            new = materialize(relabel(cand, by_t, labels, hot))
            old = as_sets(labels.join(cand, "t", "left_semi"), "old")
            changed = materialize(cand.join(as_sets(new, "new"), "t", "left")
                                      .join(old, "t", "left")
                                      .where(~F.col("new").eqNullSafe(F.col("old")))
                                      .select("t"))
            size = changed.count()
            if size == 0:
                if log:
                    log(round_line(rounds + 1, 0, work, labels, start))
                return labels, rounds, (changed if touched is None else touched)
            touched = (changed if touched is None
                       else materialize(touched.unionByName(changed).coalesce(n)))
            labels = materialize(labels.join(changed, "t", "left_anti")
                                       .unionByName(new.join(changed, "t", "left_semi"))
                                       .coalesce(n))
            cand = materialize(by_s.join(changed.withColumnRenamed("t", "s"), "s", "left_semi")
                                   .select("t").distinct())
            if log:
                log(round_line(rounds + 1, size, work, labels, start))
        raise RuntimeError("label propagation did not converge")
    finally:
        by_s.unpersist()
        by_t.unpersist()


def build(pieces: DataFrame, deposits: DataFrame, partitions=None, log=None, hot_key_rows=None):
    """The batch build with a jump table: rounds over heads, then one fill-in join. With
    hot_key_rows, hot inputs and hot heads are joined by broadcast (split_join)."""
    jumps = build_jumps(pieces, hot_key_rows)
    hpieces = materialize(resolve(pieces, jumps, hot_keys(pieces, "s", hot_key_rows)))
    first = (hpieces.join(deposits.select(F.col("t").alias("s")), "s", "left_semi")
                    .select("t").distinct())
    hot_s, hot_heads = hot_keys(hpieces, "s", hot_key_rows), hot_keys(jumps, "head", hot_key_rows)
    if log and hot_s is not None:
        log(f"{plural(hot_s.count(), 'hot input')}, each read by more than "
            f"{plural(hot_key_rows, 'head piece')}; {plural(hot_heads.count(), 'hot head')}, "
            f"each with more than {plural(hot_key_rows, 'jump row')}")
    heads, rounds, _ = propagate_from(hpieces, deposits, first, partitions=partitions, log=log,
                                      hot=hot_s)
    labels = materialize(heads.unionByName(fill_in(jumps, heads, hot_heads)))
    return dict(pieces=pieces, jumps=jumps, hpieces=hpieces, labels=labels), rounds


def cluster_keys(transfers: DataFrame, membership: DataFrame) -> DataFrame:
    """(wallet, cluster, key): each wallet's cluster and a hash of the cluster's member set.
    A wallet left out of membership is its own cluster, as in build.events. The hash is a
    member count and the XORs of two 64-bit hashes of the members, so it aggregates in
    pieces, and a giant cluster's member list is never built in one task."""
    wallets = (transfers.select(F.col("src").alias("wallet"))
                        .union(transfers.select(F.col("dst").alias("wallet"))).distinct())
    m = (wallets.join(membership, "wallet", "left")
                .withColumn("cluster", F.coalesce("cluster", -F.col("wallet") - 1)))
    parts = (F.count("*"), F.bit_xor(F.xxhash64("wallet")),
             F.bit_xor(F.xxhash64(F.lit(1), "wallet")))
    keys = m.groupBy("cluster").agg(F.concat_ws(":", *(p.cast("string") for p in parts))
                                     .alias("key"))
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


def recluster(transfers, sources, old_membership, new_membership, state, partitions=8,
              hot_key_rows=None, chunk_events=None):
    """Steps 1 to 4 of spark/docs/jump-table.md for one reclustering. Step 5, the holdings, is
    build.flows_and_holdings on the new labels. hot_key_rows and chunk_events are build's.
    Returns the new state and some counts."""
    pieces, jumps, hpieces, labels = (state[k] for k in ("pieces", "jumps", "hpieces", "labels"))

    # 1. Re-sweep only the clusters whose member set changed; diff the routes for the seeds.
    k0 = cluster_keys(transfers, old_membership).select("wallet", F.col("key").alias("k0"))
    k1 = (cluster_keys(transfers, new_membership)
          .select("wallet", "cluster", F.col("key").alias("k1")))
    moved = materialize(k0.join(k1, "wallet").where("k0 <> k1").select("wallet", "cluster"))
    hot = hot_wallets(transfers, hot_key_rows)
    hot_src = None if hot is None else hot.withColumnRenamed("wallet", "src")
    paid = (split_join(transfers, moved.select(F.col("wallet").alias("src")), "src", hot_src,
                       "left_semi")
            .select(F.col("seq").alias("t")))
    swept = materialize(routes(events(transfers, sources, new_membership, hot)
                               .join(moved.select("cluster").distinct(), "cluster", "left_semi"),
                               partitions, chunk_events))
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
    fresh = resolve(swept.join(seeds, "t", "left_semi"), jumps2,
                    hot_keys(swept, "s", hot_key_rows))
    hpieces2 = materialize(kept.unionByName(fresh))

    # 3. Propagate over heads only, starting from the seeds.
    heads, rounds, touched = propagate_from(hpieces2, labels, seeds,
                                            hot=hot_keys(hpieces2, "s", hot_key_rows))

    # 4. Fill in the pass-through payments whose head's label changed, in one join.
    stale = jumps2.join(touched.withColumnRenamed("t", "head"), "head", "left_semi")
    labels2 = materialize(heads.join(stale.select("t"), "t", "left_anti")
                               .unionByName(fill_in(stale, heads,
                                                    hot_keys(jumps2, "head", hot_key_rows))))

    counts = dict(moved=moved.count(), seeds=seeds.count(), under=under.count(),
                  repointed=jumps2.join(jumps, ["t", "head"], "left_anti").count(),
                  repointed_pieces=kept.join(hpieces, ["t", "k", "s"], "left_anti").count(),
                  rounds=rounds, heads_changed=touched.count(), filled=stale.count())
    return dict(pieces=pieces2, jumps=jumps2, hpieces=hpieces2, labels=labels2), counts
