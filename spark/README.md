# lifo-spark: the batch build on Spark and Kubernetes

A sketch of running the lifo_recluster trace on a full ledger. A PySpark job routes every
cluster in parallel, propagates stolen labels in rounds, and sums holdings. It is packaged
for the Kubeflow Spark Operator, with Delta Lake tables on S3-compatible storage. The job
covers the batch build. A second module, `jumps.py`, adds the jump table and incremental
reclustering. The job builds the table with `--jump-table`; the reclustering is checked
locally but not yet wired into a job, and the end of this file lists what an incremental
deployment still needs. The theorems it cites are in [the proofs](../docs/proofs.md).

## Layout

```
jobs/lifo_spark/sweep.py   per-cluster LIFO sweep (Numba), run inside mapInPandas
jobs/lifo_spark/build.py   the job: events, routes, stolen labels, flows, holdings
jobs/lifo_spark/jumps.py   the jump table: labels by rounds over heads, and reclusterings
jobs/check_local.py        local-mode exactness check against lifo_recluster
jobs/check_jumps.py        the same kind of check for jumps.py, through random regroups
jobs/check_inputs.py       the input rules, on one bad ledger per rule
results/check_local.log    three runs of check_local.py, all equal
docs/jump-table.md         the jump table: design, worked example, and proof
Dockerfile                 Spark 4.0.5, Delta 4.0.1, S3A, Numba; the jobs on the Python path
k8s/00-namespace-rbac.yaml                      namespace, service account, driver role
k8s/01-object-store-secret.example.yaml        store credentials (fill in, apply once)
k8s/10-build.sparkapplication.yaml             one full build
k8s/20-rebuild.scheduledsparkapplication.yaml  the same build every night
```

## How the job works

The job first checks its inputs (see [Inputs](#inputs)), then runs four steps.

1. **Events.** One row per cluster and transfer: an inflow, an outflow, or an internal read.
2. **Routes.** Repartition by cluster, sort by (cluster, seq), and sweep in `mapInPandas`.
   A partition's generator carries the current cluster's block stack across Arrow
   batches, so a task holds one cluster's stack at a time (Theorem 4, routing locality).
   Each route becomes rows `(cluster, t, k, s, offset, length, base)`.
3. **Stolen labels.** Only stolen runs are stored; clean is the default. Starting from the
   hack deposits, each round recomputes the readers of the labels that just changed (a
   join of pieces with their inputs' runs, the labels-from-pieces lemma) and keeps those
   whose label changed (the equality cutoff). Routes point back in time (Theorem 6), so the
   rounds reach a fixed point. Each round checkpoints its candidates, new labels, and changed
   set to the store, and the labels table every `--labels-checkpoint-every` rounds (1 by
   default). The pieces are hash-partitioned once by `s` and by `t` and cached, so a round
   shuffles the frontier rather than the pieces, and the job logs one line a round. With
   `--jump-table`, the rounds run over heads only, and one join fills in every other
   payment ([docs/jump-table.md](docs/jump-table.md)).
4. **Flows and holdings.** Net stolen flow per wallet, summed per cluster (Theorem 5).

Outputs under `--out`: `pieces` (partitioned by a hash bucket of the cluster), `labels`,
`flows`, and `holdings`. With `--jump-table`, they add `jumps` and `hpieces`, the state a
reclustering starts from.

## Inputs

| Table | Columns | Notes |
| --- | --- | --- |
| `transfers` | `seq long, src long, dst long, amount long` | `seq` is the global order (block, transaction, output); amounts are positive integers |
| `sources` | `wallet long, label int` | 0 clean, 1 or more stolen |
| `membership` | `wallet long, cluster long` | cluster ids 0 or more; a wallet left out is its own cluster |

The tables must also form a valid ledger and clustering: `seq` unique, no wallet paying
itself or paying out more than it holds, sources that only send and are never clustered,
wallet ids 0 or more, and no wallet listed twice in `sources` or in `membership`. These are
the rules `lifo.Ledger` enforces, plus the unique keys the joins assume. The job checks them
before it builds and stops with the number of rows that break each one. Without the checks,
a repeated `seq` or a wallet in two clusters gives wrong results with no error.

## Check it locally

From this folder, with PySpark 4.0.5, NumPy, Numba, pandas, and PyArrow installed:

```
PYTHONPATH=jobs python jobs/check_local.py --repo ..
```

Run it in an activated environment, or set `PYSPARK_PYTHON`, so that Spark's Python workers
use the same Python as the driver.

It joins a generated laundering network and hundreds of small random ledgers into one
ledger, and leaves half of its one-wallet clusters out of `membership`, so the job's own
singletons are checked as well as listed ones. It runs the build in local mode with tiny
Arrow batches (so clusters span batches), and requires the routes to equal `lifo.sweep`
and the labels and holdings to equal `lifo.fresh_trace`. A run with the defaults:

```
ledger: 15,081 transfers, 4,876 wallets, 2,987 clusters (1,257 left out of membership); 11,262 routes, 9,822 tainted transfers, 27 label rounds, 102 s
  routes          11,262 expected  all equal
  stolen labels    9,822 expected  all equal
  holdings         1,186 expected  all equal
```

[`results/check_local.log`](results/check_local.log) has this run and three more: two with
other seeds, partition counts, batch sizes, and labels checkpoint intervals, up to 28,957
transfers, and one with `--jump-table`, which takes 12 rounds over heads against 27. All
were run with PySpark 4.0.5 on Java 21 and Python 3.10. Separately, the job ran end to end
through `spark-submit` on Parquet files, with checkpoints on disk and
`--labels-checkpoint-every 3`, and again with `--jump-table`, and its labels and holdings
matched both times.

`PYTHONPATH=jobs python jobs/check_inputs.py` checks the input rules. It runs `violations`
on one bad ledger per rule, each of which must be flagged by exactly that rule, and on a
valid ledger, which must pass.

## The jump table

`jobs/lifo_spark/jumps.py` adds a jump table with one row per pass-through payment, a route
of one piece: `jumps(t, head, offset, length)` says that t carries units
`[offset, offset + length)` of head's block. Heads are deposits, payments that mix funds,
and seeds. `hpieces` holds the heads' own routes, with every input resolved to a head.
[docs/jump-table.md](docs/jump-table.md) has the design, a worked example, and the proof.

- `build(pieces, deposits)` builds the table by pointer jumping, in about log2 of the
  longest chain rounds. It then propagates labels in rounds over heads only and fills in
  every pass-through payment with one join, since its label is a slice of its head's.
- `recluster(transfers, sources, old_membership, new_membership, state)` handles one
  regroup. It re-sweeps only the clusters whose member set changed, found by hashing each
  cluster's members, and diffs the routes for the seeds. One join then re-points every
  jump row and resolved head piece that lies under a seed: it comes after the seed, its
  range of the seed's old head falls inside the seed's old range, and the seed was a
  pass-through with readers. The latest such seed wins. Labels then propagate over heads
  from the seeds, with the equality cutoff, and one join fills in the pass-through
  payments whose head's label changed. `flows_and_holdings` on the new labels gives the
  holdings.

Seeds stay heads, so the table compresses less after many regroups; an occasional `build`
restores it.

Check it locally, from this folder:

```
PYTHONPATH=jobs python jobs/check_jumps.py --repo ..
```

It builds a smaller version of check_local.py's ledger, runs `build`, then applies random
merges and splits one at a time with `recluster`. After the build and after every regroup,
it requires the labels and holdings to equal `lifo.fresh_trace` for the clustering at that
point, and the kept pieces to equal a full re-sweep. A run with the defaults:

```
ledger: 3,391 transfers, 1,205 wallets, 797 clusters; 1,740 jump rows, 816 payments with head routes
batch build with the jump table                            11 rounds over heads, against 20 over all payments
   labels 2,060: all equal; holdings 266: all equal; pieces equal a full re-sweep
1. split a 2-wallet cluster into 1 + 1                     seeds 7, re-pointed 0 rows and 4 head pieces, head rounds 7, heads changed 8, filled 1
   labels 2,061: all equal; holdings 266: all equal; pieces equal a full re-sweep
2. split a 2-wallet cluster into 1 + 1                     seeds 11, re-pointed 0 rows and 0 head pieces, head rounds 7, heads changed 30, filled 4
   labels 2,062: all equal; holdings 267: all equal; pieces equal a full re-sweep
3. merge clusters of 3 and 2 wallets (transfer 3142)       seeds 10, re-pointed 1 rows and 1 head pieces, head rounds 1, heads changed 3, filled 1
   labels 2,063: all equal; holdings 266: all equal; pieces equal a full re-sweep
4. split a 2-wallet cluster into 1 + 1                     seeds 2, re-pointed 0 rows and 0 head pieces, head rounds 3, heads changed 3, filled 0
   labels 2,063: all equal; holdings 267: all equal; pieces equal a full re-sweep
5. merge clusters of 15 and 1 wallets (transfer 418)       seeds 9, re-pointed 10 rows and 6 head pieces, head rounds 1, heads changed 1, filled 1
   labels 2,063: all equal; holdings 268: all equal; pieces equal a full re-sweep
6. merge clusters of 1 and 20 wallets (transfer 125)       seeds 15, re-pointed 4 rows and 5 head pieces, head rounds 2, heads changed 9, filled 19
   labels 2,067: all equal; holdings 267: all equal; pieces equal a full re-sweep
ALL EQUAL (85 s)
```

Across seeds and two ledger sizes, 62 random merges and splits on ledgers of 3,187 to 7,828
transfers all matched. On a 7,706-transfer ledger, the build took 11 rounds over heads
against 25 over all payments. A long local session runs thousands of small jobs: with the
default 1 GB driver, one ran out of heap after nine regroups, so the check asks for 3 GB
(`--driver-memory`) and keeps Spark's job history short.

## Run it on Kubernetes

1. Build and push the image from this folder, for the nodes' architecture:
   `docker buildx build --platform linux/amd64 -t registry.example.com/lifo-spark:0.1.0 --push .`
   On an Apple-silicon Mac, a plain `docker build` makes an arm64 image that amd64 nodes
   cannot run.
2. `kubectl apply -f k8s/00-namespace-rbac.yaml`, then fill in a copy of the secret example
   and apply it. The namespace has to exist before the operator is installed, because the
   chart creates a service account and its permissions in each namespace it watches.
3. Install the Kubeflow Spark Operator with Helm, watching the `lifo` namespace:
   ```
   helm repo add spark-operator https://kubeflow.github.io/spark-operator
   helm install spark-operator spark-operator/spark-operator \
     --namespace spark-operator --create-namespace --set "spark.jobNamespaces={lifo}"
   ```
4. Put the three input tables under `s3a://lifo-lake/input/` as Delta tables.
5. `kubectl apply -f k8s/10-build.sparkapplication.yaml`, then follow it with
   `kubectl get sparkapplication lifo-build -n lifo -w` and
   `kubectl logs -n lifo lifo-build-driver`.
6. For nightly rebuilds, `kubectl apply -f k8s/20-rebuild.scheduledsparkapplication.yaml`.

On AWS S3, drop the four MinIO settings under `hadoopConf` and the `secretKeyRef` lines, give
the `spark` service account an IAM role (IRSA), and set
`fs.s3a.aws.credentials.provider` to
`software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider`.

## Iterative algorithms and skew

A Spark loop builds a new plan every round, and a transaction graph has heavy-tailed
degrees. This is how the sketch handles the problems that follow from each, measured on
the local check's 15,081-transfer ledger.

- **Lineage.** Each round checkpoints its frames; see Checkpoints under Sizing.
- **The cost of a round.** A round runs about 21 small jobs, so the sketch cuts rounds:
  the equality cutoff limits each round to labels that change, and `--jump-table` runs
  the rounds over heads only, 12 against 27. The pieces are the static side of every
  round's joins, so they are partitioned once by `s` and by `t` and cached, the pattern
  PageRank uses for its links. That cut the rows a round shuffles from 35,747 to 18,648,
  from 1.9 to 1.0 times the pieces table.
- **Partition creep.** A union keeps both sides' partitions, so a broadcast update would
  add partitions to labels every round. The update shuffles instead, because the planner
  can't size a checkpointed frame, and labels held at 2 partitions over 27 rounds; a
  coalesce caps them anyway, without a shuffle. Repartitioning labels by `t` every round
  also holds the count, but it moved 36% more rows a round.
- **Leaks.** Nothing is cached per round, the partitioned pieces are dropped when the loop
  ends, and old checkpoints need the cleaner and a lifecycle rule (Sizing). A long local
  session also needs a bigger driver heap and a short job history, as `check_jumps.py`
  sets.
- **Retries.** A recomputed partition gives the same rows, because every step is
  deterministic: no random values or row ids, buckets hash the cluster, the sweep sorts
  by `(cluster, seq)` with `seq` unique (the input checks enforce it), run sets are sorted
  before they are compared, and re-pointing takes the latest seed by its unique id. The
  outputs are overwritten, so a failed run reruns whole.
- **Plans that change mid-loop.** The job logs one line a round: the labels that changed,
  the input the most pieces read, the labels' partitions, and the time.
- **Hub wallets and hot inputs.** An exchange's wallet, or a deposit read by many
  payments, is a hot key in the joins on wallet, on `s` (relabeling), and on `head` (the
  fill-in). AQE's skew join, on by default, splits a shuffled join's partition that is
  five times the median and over 256 MB. Unclustered wallets get ids before anything
  shuffles on the cluster, so nulls never pile into one partition. The per-round log
  names the hottest input, which is where a broadcast path for hot keys would start.
  With `--jump-table`, path compression concentrates reads on heads: in the first round,
  input 50 is read by 224 head pieces, against 18 pieces without the table.
- **Range joins inside a hot key.** Relabeling, the fill-in, and re-pointing join on a key
  and keep the overlapping ranges, so every pair inside one key is compared. Re-pointing
  broadcasts the seeds, which are few. Bucketing the axis would bound the pairs inside a
  hot key; the sketch doesn't do that yet.
- **Giant clusters.** A cluster's sweep, and a wallet's balance check, each run in one
  task; see Not in this sketch.
- **Skew that moves.** Each round is a new query, so AQE plans it on that round's sizes,
  and the log shows which input is hot in which round.

## Sizing

- **Executors.** Each task needs the largest cluster's block stack (16 bytes a block) plus
  its Arrow batches. The Python workers run outside the JVM heap, so they need
  `memoryOverhead`.
- **Partitions.** Enough that no partition is much larger than the largest cluster.
- **Rounds.** The rounds that change a label number one less than the most hops a stolen
  unit travels from the hack, and one more round finds nothing to change. The job reports
  the first count: 27 for the local check, whose deepest stolen unit travels 28 hops. The
  benchmark counts hops per transaction instead: a transaction is one deeper than its
  deepest stolen input, even when the units it takes from that input are shallower. That
  overcounts, so the 1M network's 53 bounds its rounds at 52. Each round joins only the
  changed labels against the pieces.
- **Checkpoints.** Each round checkpoints its candidates, new labels, and changed set. A round
  reads the last round's frames several times, so without that the plan grows about
  ninefold a round; in a local test the driver ran out of memory building the fourth.
  Checkpointing the labels table rewrites it whole, and `--labels-checkpoint-every N` does
  that every N rounds instead; in between, its plan grows by two joins and a union a round.
  With `cleanCheckpoints` on, as in the manifests, Spark deletes an old checkpoint only once
  the driver has garbage-collected it, so every run, failed or not, leaves some behind. Give
  the checkpoint prefix a lifecycle rule.
- **Validation.** The input checks shuffle the transfers once by `seq`, for uniqueness,
  and once by wallet, sorted by `seq`, for the running balances.
- **One writer.** Delta's default S3 log store allows one writer at a time, hence
  `concurrencyPolicy: Forbid`. Concurrent writers need the DynamoDB log store.

## Not in this sketch

- **An incremental job.** `jumps.recluster` updates the pieces, the jump table, and the
  labels as DataFrames. A job around it still has to read each regroup's new membership,
  write the changed pieces, jump rows, labels, and holdings to Delta with one MERGE each,
  in place of the nightly rebuild, and keep that state between runs.
- **Giant clusters.** An exchange with hundreds of millions of events runs in one task.
  Splitting it into time chunks whose summaries combine like matched parentheses removes
  that bottleneck.
- **Interactive edits.** A local regroup is under a millisecond of work, and a Spark job
  takes seconds to schedule. A small service running the compiled engine against the
  tables suits interactive edits better.
