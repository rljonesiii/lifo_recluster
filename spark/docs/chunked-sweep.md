# A giant cluster's sweep in time chunks

[← The Spark sketch](../README.md) · [Documentation](../../docs/README.md)

The batch build sweeps each cluster in one task, because a cluster's LIFO stack changes with every event, in order. A giant cluster, such as an exchange with millions of events, makes that task a straggler, and neither AQE's skew join nor salting can split it: the events of one cluster have to be swept in order. Time can. [`chunks.py`](../jobs/lifo_spark/chunks.py) sweeps a giant cluster's time chunks in parallel, then settles what the chunks owe each other with a scan on the driver and two keyed joins, and the pieces equal those of one sequential sweep. The job does this with `--chunk-events N` for the clusters with more than N events, and [`check_skew.py`](../jobs/check_skew.py) and [`check_local.py`](../jobs/check_local.py) check it against the reference engine.

## Why one task is the cost

[`one_task`](../jobs/lifo_spark/build.py#L60) repartitions the events by cluster, sorts them by (cluster, seq), and sweeps each partition in `mapInPandas`. Routing locality (Theorem 4) makes that exact, since a payment's route depends only on its own cluster's events. It also makes the largest cluster one task's work. On the skewed check ledger, the task that sweeps the exchange's 27,836 events reads 29,418, while the median task reads 2,062. AQE splits a skewed partition only in a join, and a salted key would scatter a cluster's events across tasks that each need the whole stack.

## LIFO is parenthesis matching

Each unit pushed onto a cluster's stack is matched with the event that pops it, the way an open parenthesis is matched with its close. So a time chunk can be swept from an empty stack, as if nothing came before it, and summarized by (D, P): the D units its outflows pop below its own start, and the P units it leaves. Summaries compose associatively, by the law of the bicyclic monoid:

```
(D₁, P₁) then (D₂, P₂)  =  (D₁ + max(0, D₂ − P₁), P₂ + max(0, P₁ − D₂))
```

A scan over the summaries gives each chunk j its start height H_j and its low-water mark L_j = H_j − D_j, the lowest the stack gets during the chunk.

## What a chunk's sweep records

[`_chunk_batch`](../jobs/lifo_spark/sweep.py#L153) is the sequential sweep with one change: an outflow or internal read that runs its chunk's stack dry records a **debt** instead of an overdraft. A debt of n units at depth o means the payment took the heights [H_j − o − n, H_j − o) of the stack the chunk started on, where o counts what the chunk's earlier outflows popped below its start. An internal read takes units without removing them, so its debt leaves o as it was. A debt holds the payment's deepest units, so the payment's own pieces start at base n. When a chunk ends, the sweep records its leftover blocks, bottom up, each with its position above L_j, and its sum: D, P, and the depth of its deepest debt.

## The scan

[`stack_scan`](../jobs/lifo_spark/chunks.py#L61) runs on the driver, over one row per chunk. The unit at height h below chunk j's start was left by the last earlier chunk k with L_k ≤ h, because no chunk after k reached down to h. So the heights below H_j form a staircase over the strict minima of the earlier low-water marks: chunk j − 1's leftovers on top, then those of the next lower minimum, and so on. A monotone stack keeps the minima as the scan moves forward (the all nearest smaller values problem), and the scan lists a chunk's steps only down to its deepest debt.

## Settling the debts

[`settle`](../jobs/lifo_spark/chunks.py#L92) turns debts into pieces with two keyed interval joins. The first joins each debt with its chunk's steps on (cluster, chunk) and keeps the steps it overlaps. The second joins those with the leftover blocks of each step's chunk on (cluster, k) and keeps the overlaps. Where a debt's heights starting at d₀, a step, and a block at heights [b₀, b₁) overlap in [lo, hi), the payment has the piece (s, lo − b₀, hi − lo) at base lo − d₀. The debts are few, so they are broadcast. [`routes`](../jobs/lifo_spark/chunks.py#L118) then numbers the pieces of each payment that had a debt by base, which puts the settled pieces first, as the sequential sweep does.

## Chunk boundaries

[`with_chunks`](../jobs/lifo_spark/chunks.py#L36) cuts each giant cluster's seq range into bins of equal width, 16 per chunk, counts the events in each bin, and groups consecutive bins on the driver into chunks of about N events. The counts are exact and the grouping is deterministic, so a retried task gets the same chunks.

## A worked example

One cluster with seven events, cut into two chunks after the third. A piece is (s, offset, length), with s the inflow's seq, and a payment's pieces are listed bottom first.

| seq | Event | One sweep | Chunk sweep |
| --- | --- | --- | --- |
| 1 | inflow of 5 | | chunk 0 |
| 2 | inflow of 3 | | |
| 3 | outflow of 2 | (2, 1, 2) | (2, 1, 2) |
| 4 | inflow of 4 | | chunk 1 |
| 5 | outflow of 6 | (1, 4, 1), (2, 0, 1), (4, 0, 4) | (4, 0, 4) at base 2, and a debt of 2 at depth 0 |
| 6 | internal read of 3 | (1, 1, 3) | a debt of 3 at depth 2 |
| 7 | inflow of 2 | | |

Chunk 0 leaves the blocks (1, 5) at heights [0, 5) and (2, 1) at [5, 6), so D₀ = 0 and P₀ = 6. Chunk 1 pops 2 units below its start and leaves (7, 2), so D₁ = 2 and P₁ = 2, and its deepest debt reaches depth 5. The scan gives H₁ = 6 and L₁ = 4, and one step: chunk 0 left the heights [0, 6) below chunk 1's start. Payment 5's debt covers the heights [4, 6): block (1, 5) gives (1, 4, 1) at base 0, and block (2, 1) gives (2, 0, 1) at base 1, under its own piece at base 2. Payment 6's read covers the heights [1, 4) and gets (1, 1, 3) at base 0. Both match the sequential sweep.

## Why it is exact

The claim is that for every payment of a giant cluster, `chunks.routes` gives the same pieces (s, offset, length), bases, and piece numbers as one sequential sweep. Number a stack's units by height from the bottom. A block (s, r) at heights [y, y + r) holds units [0, r) of inflow s, unit u at height y + u. An outflow of a units takes the heights [h − a, h) from a stack of height h and removes them; an internal read takes the same heights and keeps them.

**Lemma 1 (a chunk's own sweep).** During chunk j, the sequential stack is the stack the chunk started on, cut to the heights [0, H_j − o), with the chunk's own stack on top, where o is what the chunk's outflows have popped below its start so far. By induction over the chunk's events: an inflow pushes onto both. An outflow or read takes from the top, which is the chunk's own stack until it runs dry, and then the heights [H_j − o − n, H_j − o), which is the debt. An outflow then removes them, so o grows by n, and a read keeps them. So the pieces a chunk's sweep emits are the sequential sweep's, and a debt is exactly the heights the sequential sweep takes below the chunk's start. At the chunk's end, the sequential stack is [0, L_j) of the stack the chunk started on, with the chunk's leftovers at [L_j, L_j + P_j), so H_{j+1} = L_j + P_j, which is the composition law.

**Lemma 2 (who left a height).** At chunk j's start, the unit at height h < H_j is the unit at height h of chunk k's leftovers, where k is the last chunk before j with L_k ≤ h. Chunk k left the heights [L_k, L_k + P_k), by Lemma 1. Every chunk i between k and j has L_i > h, so it never took height h, and h lies below L_k + P_k: if k = j − 1, because h < H_j = L_k + P_k, and otherwise because h < L_{k+1} ≤ H_{k+1} = L_k + P_k. Chunk 0 starts on an empty stack, so L₀ = 0 ≤ h, and some such k exists.

The staircase lists, for each chunk with debts, every such k with the heights it covers, so `settle` sends each height of a debt to the right chunk's leftovers and the right block, at unit h − b₀ of its inflow. By Lemma 2, chunk k's step stops below the low-water marks of the chunks after it, so a piece never runs into units a later chunk took. A debt that spans several blocks gets one piece per block, as the sequential sweep does when it takes several blocks. Finally, the sequential sweep lists a payment's pieces bottom first, and every height below a chunk's start lies under the chunk's own stack, so numbering by base gives the same piece numbers. ∎

## Cost

The chunk sweeps run in parallel, about N events each, and a task holds one chunk's stack rather than the cluster's. The scan reads one row per chunk. The settling joins touch only the debts, the payments that reach below their chunk's start, and the leftover blocks of the chunks they reach. On the skewed check ledger, with chunks of 2,048 events, the largest sweep task read 4,016 events instead of 29,418. With `--chunk-events 16`, the local check's ledger sweeps 399 clusters in 1,013 chunks, and 496 chunks have debts, settled over 571 staircase steps.

## The overdraft check

The input checks compute every wallet's running balance, and one window per wallet puts a hub wallet's moves in one task. With `--balance-bins B`, [`overdrafts`](../jobs/lifo_spark/build.py#L285) cuts the seq range into B bins and, for each wallet and bin, sums the moves and finds the lowest running balance. Those pairs compose associatively too:

```
(S₁, m₁) then (S₂, m₂)  =  (S₁ + S₂, min(m₁, S₁ + m₂))
```

So a scan over each wallet's bins gives each bin's opening balance, and only the bins whose opening balance plus lowest point is negative are checked row by row. It flags the same rows. On the skewed check ledger, with 64 bins, the largest task read 5,103 moves instead of 18,638.

## How it is checked

- [`check_local.py`](../jobs/check_local.py) with `--chunk-events 16`, with and without `--jump-table`, sweeps 399 of its ledger's clusters in chunks and requires the routes to equal `lifo.sweep` and the labels and holdings to equal `lifo.fresh_trace` ([results/check_local.log](../results/check_local.log)).
- [`check_jumps.py`](../jobs/check_jumps.py) with `--chunk-events 16` re-sweeps the regrouped clusters in chunks through six random merges and splits, and all match.
- [`check_skew.py`](../jobs/check_skew.py) sweeps random clusters in chunks of 1 to 400 events against the one-task sweep, then builds a skewed ledger, checks the outputs against the reference with chunks of 2,048 events, and measures the tasks ([results/check_skew.log](../results/check_skew.log)).
- [`check_inputs.py`](../jobs/check_inputs.py) runs the binned overdraft check against the window on random moves: the same 1,151 rows in 1, 3, 64, and 4,000 bins.
- Through `spark-submit` on Parquet, a run with `--chunk-events 16` wrote the same pieces as one without.
