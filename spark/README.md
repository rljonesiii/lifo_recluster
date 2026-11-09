# lifo-spark: the batch build on Spark and Kubernetes

A sketch of running the lifo_recluster trace on a full ledger. A PySpark job routes every
cluster in parallel, propagates stolen labels in rounds, and sums holdings. It is packaged
for the Kubeflow Spark Operator, with Delta Lake tables on S3-compatible storage. It covers
the batch build only; the end of this file lists what an incremental deployment adds. The
theorems it cites are in [the proofs](../docs/proofs.md).

## Layout

```
jobs/lifo_spark/sweep.py   per-cluster LIFO sweep (Numba), run inside mapInPandas
jobs/lifo_spark/build.py   the job: events, routes, stolen labels, flows, holdings
jobs/check_local.py        local-mode exactness check against lifo_recluster
results/check_local.log    three runs of that check, all equal
Dockerfile                 Spark 4.0.5, Delta 4.0.1, S3A, Numba; the jobs on the Python path
k8s/00-namespace-rbac.yaml                      namespace, service account, driver role
k8s/01-object-store-secret.example.yaml        store credentials (fill in, apply once)
k8s/10-build.sparkapplication.yaml             one full build
k8s/20-rebuild.scheduledsparkapplication.yaml  the same build every night
```

## How the job works

1. **Events.** One row per cluster and transfer: an inflow, an outflow or an internal read.
2. **Routes.** Repartition by cluster, sort by (cluster, seq), and sweep in `mapInPandas`.
   A partition's generator carries the current cluster's block stack across Arrow
   batches, so a task holds one cluster's stack at a time (Theorem 4, routing locality).
   Each route becomes rows `(cluster, t, k, s, offset, length, base)`.
3. **Stolen labels.** Only stolen runs are stored; clean is the default. Starting from the
   hack deposits, each round recomputes the readers of the labels that just changed (a
   join of pieces with their inputs' runs, the labels-from-pieces lemma) and keeps those
   whose label changed (the equality cutoff). Routes point back in time (Theorem 6), so the
   rounds reach a fixed point. Each round checkpoints its candidates, new labels and changed
   set to the store, and the labels table every `--labels-checkpoint-every` rounds (1 by
   default).
4. **Flows and holdings.** Net stolen flow per wallet, summed per cluster (Theorem 5).

Outputs under `--out`: `pieces` (partitioned by a hash bucket of the cluster), `labels`,
`flows` and `holdings`.

## Inputs

| Table | Columns | Notes |
| --- | --- | --- |
| `transfers` | `seq long, src long, dst long, amount long` | `seq` is the global order (block, transaction, output); amounts are integers |
| `sources` | `wallet long, label int` | 0 clean, 1 or more stolen |
| `membership` | `wallet long, cluster long` | cluster ids 0 or more; a wallet left out is its own cluster |

## Check it locally

From this folder, with PySpark 4.0.5, NumPy, Numba, pandas and PyArrow installed:

```
PYTHONPATH=jobs python jobs/check_local.py --repo ..
```

Run it in an activated environment, or set `PYSPARK_PYTHON`, so that Spark's Python workers
use the same Python as the driver.

It joins a generated laundering network and hundreds of small random ledgers into one
ledger, runs the build in local mode with tiny Arrow batches (so clusters span batches),
and requires the routes to equal `lifo.sweep` and the labels and holdings to equal
`lifo.fresh_trace`. A run with the defaults:

```
ledger: 15,081 transfers, 4,876 wallets, 2,987 clusters; 11,262 routes, 9,822 tainted transfers, 27 label rounds, 51 s
  routes          11,262 expected  all equal
  stolen labels    9,822 expected  all equal
  holdings         1,186 expected  all equal
```

[`results/check_local.log`](results/check_local.log) has this run and two more, with other
seeds, partition counts, batch sizes and labels checkpoint intervals, up to 28,957
transfers. All were run with PySpark 4.0.5 on Java 21 and Python 3.10. Separately, the job
ran end to end through `spark-submit` on Parquet files, with checkpoints on disk and
`--labels-checkpoint-every 3`, and its labels and holdings matched as well.

## Run it on Kubernetes

1. Build and push the image from this folder:
   `docker build -t registry.example.com/lifo-spark:0.1.0 . && docker push registry.example.com/lifo-spark:0.1.0`
2. Install the Kubeflow Spark Operator with Helm, watching the `lifo` namespace:
   ```
   helm repo add spark-operator https://kubeflow.github.io/spark-operator
   helm install spark-operator spark-operator/spark-operator \
     --namespace spark-operator --create-namespace --set "spark.jobNamespaces={lifo}"
   ```
3. `kubectl apply -f k8s/00-namespace-rbac.yaml`, then fill in a copy of the secret example
   and apply it.
4. Put the three input tables under `s3a://lifo-lake/input/` as Delta tables.
5. `kubectl apply -f k8s/10-build.sparkapplication.yaml`, then follow it with
   `kubectl get sparkapplication lifo-build -n lifo -w` and
   `kubectl logs -n lifo lifo-build-driver`.
6. For nightly rebuilds, `kubectl apply -f k8s/20-rebuild.scheduledsparkapplication.yaml`.

On AWS S3, drop the four MinIO settings under `hadoopConf` and the `secretKeyRef` lines, give
the `spark` service account an IAM role (IRSA), and set
`fs.s3a.aws.credentials.provider` to
`software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider`.

## Sizing

- **Executors.** Each task needs the largest cluster's block stack (16 bytes a block) plus
  its Arrow batches. The Python workers run outside the JVM heap, so they need
  `memoryOverhead`.
- **Partitions.** Enough that no partition is much larger than the largest cluster.
- **Rounds.** One less than the most hops a stolen unit travels from the hack, plus a last
  round that finds nothing to change. The local check takes 27, and its deepest stolen unit
  travels 28 hops. The benchmark counts hops per transaction instead: a transaction is one
  deeper than its deepest stolen input, even when the units it takes from that input are
  shallower. That overcounts, so the 1M network's 53 bounds its rounds at 52. Each round
  joins only the changed labels against the pieces.
- **Checkpoints.** Each round checkpoints its candidates, new labels and changed set. A round
  reads the last round's frames several times, so without that the plan grows about
  ninefold a round; in a local test the driver ran out of memory building the fourth.
  Checkpointing the labels table rewrites it whole, and `--labels-checkpoint-every N` does
  that every N rounds instead; in between, its plan grows by two joins and a union a round.
  With `cleanCheckpoints` on, as in the manifests, Spark deletes an old checkpoint only once
  the driver has garbage-collected it, so every run, failed or not, leaves some behind. Give
  the checkpoint prefix a lifecycle rule.
- **One writer.** Delta's default S3 log store allows one writer at a time, hence
  `concurrencyPolicy: Forbid`. Concurrent writers need the DynamoDB log store.

## Not in this sketch

- **Incremental regroups.** Re-sweep only the clusters an operation creates, replace their
  pieces in one Delta commit, diff old and new pieces to get the seeds, and call
  `propagate` from those seeds. It is the same function the build uses.
- **Giant clusters.** An exchange with hundreds of millions of events runs in one task.
  Splitting it into time chunks whose summaries combine like matched parentheses removes
  that bottleneck.
- **Interactive edits.** A local regroup is under a millisecond of work, and a Spark job
  takes seconds to schedule. A small service running the compiled engine against the
  tables suits interactive edits better.
