# 3. The update

[← 2. Routes and labels](02-routes-and-labels.md) · [Index](README.md)

Every reclustering is one call, [`regroup(blocks)`](../lifo.py#L396): each given set of wallets becomes a cluster, and whatever is left of the clusters they cut stays together. [`merge`](../lifo.py#L376) and [`split`](../lifo.py#L387) are the two common cases, and an overlapping change (part of one cluster joining part of another) is a single `regroup` too.

## Four steps

**1. Work out the new clusters** ([lines 400–410](../lifo.py#L400-L410)). Validate the input before changing anything. Drop each given set that is already a cluster, since it changes nothing, and stop if none is left. Find the clusters the remaining sets touch (the *outgoing* clusters), and form the new ones: those sets plus the remainder of each outgoing cluster.

**2. Retire the outgoing clusters** ([lines 412–423](../lifo.py#L412-L423)). Before anything changes, save each outgoing cluster's routes and readers in the cache under its member set, so a later undo can bring them back without a sweep. Then drop its readers.

**3. Route the new clusters** ([lines 424–443](../lifo.py#L424-L443)). For each new cluster, take its routes and readers from the cache if this member set has been seen, or [`sweep`](../lifo.py#L214) its events. Point its wallets at it, set its holdings to the sum of its members' net flows, and install its readers. Compare each of its routes with the old one: every transaction whose route changed is a **seed**.

**4. Propagate labels** ([`_propagate`](../lifo.py#L466)). Keep the seeds in a min-heap and process them in time order. For each transaction:

- recompute its label from its pieces ([lines 500–505](../lifo.py#L500-L505));
- if the label is **unchanged, stop here**: its readers cannot be affected ([lines 516–517](../lifo.py#L516-L517));
- if it changed, move the sender's and receiver's net flows and their clusters' holdings by the change in stolen units ([lines 506–515](../lifo.py#L506-L515)), and push its readers onto the heap ([lines 518–524](../lifo.py#L518-L524)).

Every input comes before its reader in time, so popping in time order means each label is computed from inputs that are already final.

## Two safeguards

- **The linear pass.** If the wave grows past n/4 recomputed transactions, propagation switches to one plain pass over every later transaction ([lines 493–497](../lifo.py#L493-L497)). That caps the number of label recomputations at about 1.25 n ([Theorem 9](proofs.md#the-algorithm)), but not their cost: each recomputation walks its route's pieces, and a ledger can have far more pieces than labels have runs, so the worst update can cost much more than a fresh trace ([the worst case](report.md#5-the-update-algorithm)).
- **The cache budget.** The routing cache is least-recently-used and holds at most n routes in total ([`_cache_put`](../lifo.py#L455)). Evicting an entry only costs a future sweep; it never changes a result.

## Three levels of "does this change matter?"

Each level is detected with only the work it needs, never by retracing downstream.

| Level | What happens | Work done | Example in the challenge |
| --- | --- | --- | --- |
| Routing-neutral | Re-sweeping reproduces every old route | Only the changed clusters' events | B+E: 5 events, 0 routes |
| Label-neutral | Routes move, but the moved funds carry the same labels | Plus one relabel per moved route | t10 under D+F |
| Attribution change | Labels change and spread until they stop changing | Plus the changed transactions and their direct readers | t11 → t13 → t14 under D+F |

## Walking through the challenge ledger

[`toy.py`](../toy.py) prints the engine's work counters for each event ([`results/toy.log`](../results/toy.log)).

**Merge B and E: 5 events swept, 0 routes changed.** The new cluster {B, E} has five events: t2, t4 and t5 (B) and t6 and t8 (E). Re-sweeping them gives the same routes as before: E's $40 lands on top of B's remaining $30, and E's payment at t8 takes exactly that block. No seeds, so nothing downstream is touched.

**Merge D and F: 6 events swept, 2 routes changed, 4 relabeled, 3 changed.** In {D, F}, t7 (D → F) becomes internal. Two routes change:

- **t10** (F → G, $80) used to take its bottom $10 from t7; now the same units are traced back to t5. The label is the same (clean $10), so the wave stops there. This is the *label-neutral* level.
- **t11** (D → H, $20) used to take stolen money; now it takes clean money. With t7 internal, D no longer pays out t5's clean $40 at t7, so clean money is still on top of t5's stolen $30 when D pays H. Its label changes, so its reader **t13** (H → I) is recomputed and changes, then **t13**'s reader **t14** (I → X) changes. X has no readers, so the wave ends.

That is 4 transactions relabeled (t10, t11, t13, t14), 3 of them changed. G, Y and t12 are never looked at.

**Split B from E again: 0 events swept, 2 cache hits.** The member sets {B} and {E} were saved in the cache when B and E were merged, so their routes come back without a sweep. No route differs, so nothing is relabeled.

## What an update costs

One pass over the events of the clusters it creates (none on a cache hit), plus one pass to save each outgoing cluster that is not already cached, plus one label recomputation for each transaction the wave reaches: the transactions whose route changed, then the readers of every label that changed. Under D+F that is t10 and t11, then t13 and t14: 4 recomputations. A fresh trace costs all *n* transactions every time. [Page 7](07-benchmarks.md) shows what that means at 1M transactions.

[Theorem 8](proofs.md#the-algorithm) proves that after every `regroup` the routes, readers, labels, net flows and holdings equal a from-scratch build of the new clustering, and every cache entry still holds the routing of its member set.

---
[← 2. Routes and labels](02-routes-and-labels.md) · Next: [4. Reading the reference engine →](04-reference-engine.md)
