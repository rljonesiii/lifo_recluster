# Appendix: Proofs

[← The report](report.md) · [Documentation](README.md)

Nine theorems and their lemmas establish three things for every input. The formulation has the properties the challenge's rules require. Each data structure represents what it claims to. The incremental update equals a fresh trace for every valid ledger, clustering and regrouping. Each theorem's mechanical checks are listed at the end.

## Definitions

**Ledger.** Transactions *t* = 0, …, *n* − 1 execute in that order. Each is a triple (srcₜ, dstₜ, aₜ) with srcₜ ≠ dstₜ and aₜ a positive integer. Wallets are either *sources*, each with a label λ(*s*), a non-negative integer (0 clean, ≥ 1 stolen), or *ordinary wallets*, the set *W*. Every dstₜ is in *W*: a source only sends. Write bal(*w*, *t*) for what *w* received before *t* minus what it sent before *t*. The ledger is *valid* if bal(srcₜ, *t*) ≥ aₜ whenever srcₜ ∈ *W*, so no ordinary wallet ever pays more than it holds. The `Ledger` constructor rejects self-transfers, non-positive amounts, transfers into a source and overdrafts.

**Clustering.** A partition 𝒫 of *W*; c(*w*) is the cluster containing *w*. Relative to 𝒫, transaction *t* is a *deposit* if srcₜ is a source, *internal* if c(srcₜ) = c(dstₜ), and *external* otherwise. For a cluster *C*:

- an *inflow* of *C* is a deposit or external transfer with dstₜ ∈ *C*;
- an *outflow* of *C* is an external transfer with srcₜ ∈ *C*;
- an *internal read* of *C* is an internal transfer within *C*;
- the *events* of *C* are its inflows, outflows and internal reads.

**Units and the specification.** Transaction *t* owns the units (*t*, 0), …, (*t*, aₜ − 1). Each cluster *C* has a stack σ\_C, a finite sequence of units listed bottom first, initially empty. Transactions execute in order, and an event *t* of *C* acts on σ\_C as follows.

- **Inflow:** append (*t*, 0), …, (*t*, aₜ − 1).
- **Outflow or internal read:** with *m* = |σ\_C|, the transaction *carries* the units ρ(*t*) = (σ\_C\[*m* − aₜ\], …, σ\_C\[*m* − 1\]), bottom first. An outflow also removes them. The receiving cluster then sees *t* as an inflow and appends *t*'s own units.

Labels are defined along ρ: ℓ(*t*, *x*) = λ(srcₜ) for a deposit, and ℓ(*t*, *x*) = ℓ(ρ(*t*)ₓ) otherwise. The outputs are Lₜ, the run-length encoding of (ℓ(*t*, 0), …, ℓ(*t*, aₜ − 1)), and the holdings hold(*C*), the number of stolen units on σ\_C at the end. Write H\_C(τ) = |σ\_C| after transaction τ, and σ(*t*) for the number of stolen units *t* carries.

An inflow appends *t*'s own units rather than the units *t* carried. Those units inherit their labels through ρ, so for labels this is the same as moving the carried units across in the same order. It also gives every unit a fixed identity, which is what makes routing local (T4). The specification is executed literally, unit by unit, as [`spec_trace`](../verify/exhaustive.py#L151) in [`verify/exhaustive.py`](../verify/exhaustive.py).

## The formulation

**Theorem 1 (well-definedness and conservation).** If the ledger is valid, then under every clustering:

- every outflow and internal read is defined, that is |σ\_C| ≥ aₜ;
- after every transaction, |σ\_C| = Σ\_{w ∈ C} bal(*w*);
- every unit is on at most one stack at a time and is removed by at most one outflow.

*Proof.* By induction on transactions; initially both sides of the equation are 0. An inflow of *C* adds aₜ to |σ\_C| and to bal(dstₜ), with dstₜ ∈ *C*. An outflow subtracts aₜ from both, with srcₜ ∈ *C*. An internal read leaves σ\_C unchanged and moves aₜ between two wallets of *C*, so the sum is unchanged. A transaction that is not an event of *C* changes neither side. Validity keeps every balance non-negative, again by induction. So before an outflow or internal read of *C*, |σ\_C| = Σ\_{w ∈ C} bal(*w*) ≥ bal(srcₜ) ≥ aₜ. A unit is created once, by the inflow that owns it, and appended to exactly one stack. It leaves a stack only when removed, so it is on at most one stack and is removed at most once. ∎

**Theorem 2 (first passage).** Suppose a unit *u* sits at index *y* of σ\_C (0 is the bottom) after transaction τ, and define κ(*u*) as below, with κ(*u*) = ∞ when no such τ′ exists. Then *u* keeps index *y* after every transaction τ″ with τ < τ″ < κ(*u*). If κ(*u*) is finite, it is an outflow of *C* and it removes *u*; otherwise *u* is still on σ\_C at the end.

```math
\kappa(u) = \min\{\, \tau' > \tau \;:\; H_C(\tau') \le y \,\}
```

*Proof.* Appending changes no existing index. An internal read changes nothing, and neither does a transaction that is not an event of *C*. An outflow of amount *a* from height *m* removes exactly the indices *m* − *a* = H\_C(τ′) through *m* − 1 and leaves every lower index in place. So the unit survives an event exactly when *y* < H\_C(τ′), and is removed exactly when H\_C(τ′) ≤ *y*. Only an outflow lowers the height, so the first event with H\_C(τ′) ≤ *y* is an outflow. Induction over the events of *C*. ∎

**Theorem 3 (internal transfers and push order).** Suppose an internal transfer were modelled as removing the top *a* units and appending them again in the order given by a permutation π of {0, …, *a* − 1}. Unit *i* of the result is then unit π(*i*) of the removed sequence. This leaves the stack's labels unchanged for every stack and every labelling if and only if π is the identity. With the identity, an internal transfer is exactly the specification's no-op: it neither creates nor consumes units, so nothing is double-counted.

*Proof.* If π is the identity, every unit returns to the index it left. Otherwise pick *i* with π(*i*) ≠ *i*. Label unit π(*i*) of the top *a* stolen and all others clean. After the transfer, index *i* of the top holds a stolen unit, where before it held a clean one. ∎

For external transfers the same order is a modelling choice, not a theorem: the specification appends a transfer's units at the receiver in the order they were carried. Reversed pushes are also self-consistent, but they change the answers. Exchange X then receives $40, $40, $60 and $60 across the four states of the challenge ledger, instead of $30, $30, $10 and $10 ([`toy.py`](../toy.py) prints both).

## Factorization

**Theorem 4 (routing locality).** Let cluster *C* have members *M*. The stack σ\_C at every moment, and the units ρ(*t*) carried by each of its outflows and internal reads, are determined by *M* and the ledger alone. They do not depend on how wallets outside *M* are clustered.

*Proof.* Whether *t* is an inflow, an outflow, an internal read of *C* or none of these depends only on whether srcₜ is a source and whether srcₜ and dstₜ lie in *M*. The effect of an event on σ\_C uses only *t*, aₜ and σ\_C itself, because an inflow appends *t*'s own units, not the contents of another stack. By induction over the events of *C*, σ\_C and ρ are functions of *M* and the ledger. ∎

Labels therefore factor in two. ρ is computed cluster by cluster, never looks at a label, and depends on membership alone. Labels are then propagated through ρ in time order, from the source labels outwards.

**Theorem 5 (holdings identity).** For every cluster *C*:

```math
\mathrm{hold}(C) = \sum_{w \in C} \Big( \sum_{t:\, \mathrm{dst}_t = w} \sigma(t) \; - \sum_{t:\, \mathrm{src}_t = w} \sigma(t) \Big)
```

*Proof.* Both sides start at 0, and they change by the same amount at every transaction. The stolen units on σ\_C change only at events of *C*:

- An inflow adds σ(*t*): the units it appends carry the labels it carries. On the right, the dst term of a member adds σ(*t*), and the sender is not a member.
- An outflow removes exactly the units it carries, σ(*t*). On the right, the src term of a member subtracts σ(*t*).
- An internal read changes nothing. On the right, one member's dst term adds σ(*t*) and another's src term subtracts it.
- A transaction that is not an event of *C* touches no member, so both sides are unchanged.

∎

The inner sums depend on the clustering only through the labels. Holdings therefore need no stack at all: the engine keeps one net stolen flow per wallet and sums it over members.

## Data structures

**Theorem 6 (run-length stack).** The sweep represents σ\_C by blocks β = ((s₁, r₁), …, (sₖ, rₖ)), bottom first. The blocks mean the unit sequence

```math
\alpha(\beta) = (s_1, 0), \dots, (s_1, r_1 - 1),\; (s_2, 0), \dots,\; (s_k, r_k - 1)
```

and satisfy the invariant rᵢ ≥ 1 and s₁ < s₂ < … < sₖ. A route is a tuple of pieces (*s*, *o*, ℓ), each meaning the units (*s*, *o*), …, (*s*, *o* + ℓ − 1), concatenated bottom first. At every event the sweep preserves both α(β) = σ\_C and the invariant. The route it emits for an outflow or internal read expands to exactly ρ(*t*).

*Proof.* By cases on the sweep's branches, with induction over events.

- **Inflow.** Appending (*t*, aₜ) appends exactly *t*'s units. *t* is later than every earlier inflow, so the ids stay increasing.
- **Read or pop within the top block** (rₖ ≥ *a*). The top *a* units of α(β) are (sₖ, rₖ − *a*), …, (sₖ, rₖ − 1), which is the single piece (sₖ, rₖ − *a*, *a*). A pop with rₖ > *a* sets rₖ to rₖ − *a*, which removes exactly those units and keeps the block's bottom units. A pop with rₖ = *a* removes the block. A read changes nothing.
- **Several blocks** (rₖ < *a*, so at least two blocks are involved). The loop starts at the top block with need = *a*. While need ≥ rᵢ it emits the full piece (sᵢ, 0, rᵢ), lowers need by rᵢ and moves to the block below. It stops in one of two ways. *Boundary:* need reaches 0 just after a full block; no partial piece is emitted, and the loop's index rests on the highest block it did not touch. *Partial:* need < rⱼ at some block *j*; it emits (sⱼ, rⱼ − need, need), the top need units of block *j*, and the index rests on *j*. Reversing the list gives bottom-first order, so the pieces expand to the top *a* units of α(β) in order. A pop then shrinks block *j* to rⱼ − need ≥ 1 in the partial case, and in both cases deletes every block above the index, which removes exactly the carried units. A read changes nothing. The loop never passes the bottom block, because the blocks hold |σ\_C| ≥ *a* units by Theorem 1. That bound is load-bearing in the code: an index below 0 would not fail but silently wrap round to the top of the Python list, or of the NumPy array in the compiled engine, where even Numba's bounds checks accept an index of −1.

In every branch each block keeps r ≥ 1 and the ids stay increasing. ∎

Three consequences follow. Every block holds the bottom rᵢ units of its inflow, so two stacks with the same units have the same blocks. Every piece of route(*t*) points into an inflow earlier than *t*, so a transaction's inputs always precede it. Adjacent pieces of a route lie in different blocks, and so in different inflows, so a route is the unique decomposition of ρ(*t*) into maximal runs; the exhaustive check compares routes in this form.

**Lemma (labels from pieces).** Let route(*t*) = ((*s*₁, *o*₁, ℓ₁), …, (*s*ₖ, *o*ₖ, ℓₖ)). Then

```math
L_t = \mathrm{cut}(L_{s_1}, o_1, \ell_1) \oplus \cdots \oplus \mathrm{cut}(L_{s_k}, o_k, \ell_k)
```

where cut(*L*, *o*, ℓ) keeps the runs of *L* that overlap its units *o*, …, *o* + ℓ − 1, clipped to them, and ⊕ concatenates two run sequences, merging the last run of the first into the first run of the second when their labels are equal. Both engines compute exactly this: [`_label_from_pieces`](../lifo.py#L277) and the compiled [`_label_runs`](../lifo_nb.py#L229) walk each *L*\_{*s*ᵢ} to offset *o*ᵢ, emit the clipped runs, and merge each run into the previous one when the labels agree. The fast path for one piece of a one-run inflow is the case *k* = 1, |*L*\_{*s*₁}| = 1. In particular, *L*ₜ is a function of route(*t*) and the labels of its inputs.

*Proof.* By Theorem 6, ρ(*t*) is the concatenation, bottom first, of the units (*s*ᵢ, *o*ᵢ), …, (*s*ᵢ, *o*ᵢ + ℓᵢ − 1). Unit *x* of *t* has the label of the unit ρ(*t*)ₓ, by the definition of labels along ρ. So the sequence of *t*'s unit labels is the concatenation over *i* of units *o*ᵢ to *o*ᵢ + ℓᵢ − 1 of *s*ᵢ's unit labels. Run-length encoding commutes with both operations: a contiguous slice encodes as the clipped runs that overlap it, and a concatenation encodes as the two encodings joined, with the runs at the seam merged when their labels agree. The right-hand side is therefore the run-length encoding of *t*'s unit labels, which is *L*ₜ. ∎

**Theorem 7 (readers locality).** If a piece of route(*t*) points into inflow *s*, then *t* is sent by the cluster that received *s*. The readers of a cluster's inflows are therefore exactly the transactions that cluster sends whose pieces point into them, and that cluster's own sweep builds them.

*Proof.* The pieces of route(*t*) point into blocks of σ for c(srcₜ), by Theorem 6. The blocks of a cluster's stack are created only by that cluster's inflows, by the inflow case of Theorem 6. So *s* is an inflow of c(srcₜ). ∎

**Lemma (readers output).** The readers map a sweep returns is the exact inverse of the routes it returns: it sends each inflow *s* to the transactions *t*, in increasing order, that have a piece of route(*t*) in *s*. *Proof.* Each branch that emits a route records *t* once under the inflow of each of its pieces, and nothing else writes the map. The pieces of one route lie in distinct blocks, whose ids differ (Theorem 6), so no *t* is recorded twice under one inflow. Events are processed in increasing order. ∎

**The other two structures.** Per-wallet net flows are the inner sums of Theorem 5, so they are exact whenever the labels are. The routing cache maps a membership set *M* to the routing and readers of a cluster with members *M*. By Theorem 4 these do not depend on the rest of the clustering, and the engine never modifies a stored routing or readers list, so an entry stays valid for as long as it is kept. Eviction affects speed only, never results.

## The algorithm

Call the engine's state *exact* for a clustering when all of the following hold:

- its routes equal the sweep routes of that clustering;
- its readers index is their exact inverse;
- its labels equal the specification's;
- each transaction's stored stolen count is σ(*t*) under those labels;
- its per-wallet net flows are the inner sums of Theorem 5 under those labels;
- every wallet's owner is the cluster that contains it, and each cluster's holdings equal the sum of its members' flows;
- every cache entry for a membership *M* holds the routing and readers of *M*, exactly as a sweep of *M* returns them.

**Lemma (build).** Building the engine for a clustering 𝒫 gives a state that is exact for 𝒫. This is the base case of the induction that Theorem 8 completes.

*Proof.* *Routes and readers.* The reference engine sweeps each cluster once, so its routes are the sweep routes of 𝒫 and its readers are their inverse (Theorem 6 and the readers-output lemma). The compiled engine instead makes one pass over the ledger, with one block stack per cluster. Transaction *t* acts only on the stacks of c(srcₜ) and c(dstₜ), and the transactions that touch a cluster, taken in increasing order, are exactly that cluster's events in order. Each stack therefore goes through the same operations as in a sweep of its cluster alone and emits the same routes; the readers are then built by inverting the routes. *Labels.* Labels are computed for *t* = 0, 1, … in increasing order. A deposit gets (λ(srcₜ), *a*ₜ). Every other *t* gets its label from its pieces, which point into inflows earlier than *t* (Theorem 6) whose labels are final by induction, so by the label lemma it gets *L*ₜ. *The rest.* Stolen counts are read off the labels, and the net flows are the inner sums of Theorem 5 accumulated over all transactions. Every wallet's owner is its cluster, and each cluster's holdings are the sum of its members' net flows, which is hold(*C*) by Theorem 5. The cache starts empty. ∎

**Theorem 8 (incremental correctness).** If the state is exact for 𝒫 and `regroup(blocks)` turns 𝒫 into 𝒫′, the resulting state is exact for 𝒫′.

*Proof.* A block that is already a cluster of 𝒫 is dropped first: that cluster belongs to both 𝒫 and 𝒫′, so nothing changes for it. Let *A* be the clusters of 𝒫 that meet the remaining blocks, and *A*′ the clusters that replace them. They cover the same wallets, and every other cluster belongs to both 𝒫 and 𝒫′. Say "old" for labels under 𝒫 and "new" for labels under 𝒫′.

*Routes.* The engine writes route(*t*) only for transactions sent by the new clusters. A transaction sent from outside *A* therefore keeps its route, which is still its 𝒫′ route because its sending cluster is unchanged (Theorem 4). For a transaction sent from inside *A*, the engine installs the route of its new cluster, with members *M*. That routing comes either from a sweep of *M*, which reads only the events of *M* and is correct by Theorems 4 and 6, or from a cache entry for *M*, which equals it by the cache invariant. So every route is now the 𝒫′ route. Let *D* be the transactions whose route changed; all of them are sent from inside *A*.

*Cache.* Entries are added in two ways. First, before any route changes, the engine snapshots each outgoing cluster that is not cached: its routes and its readers, read from a state that is exact for 𝒫, so they are the routing and readers of its membership. A cluster's readers are deleted only after its own snapshot is taken, and the readers deleted for another outgoing cluster belong to inflows that cluster received, so they are never among them. Second, each new cluster's sweep result is stored as it is computed. Existing entries are only reordered or evicted. So every entry still holds the routing and readers of its membership.

*Readers.* Every key the engine deletes is an inflow received by a member of an outgoing cluster: the keys of that cluster's readers, taken from its cache entry or collected by receiver. Every key it installs comes from a new cluster's readers. Because *A* and *A*′ cover the same wallets, this replaces the readers of every transaction received inside *A*, including those whose kind changes: a former inflow that becomes internal loses its entry, and a former internal transfer that becomes an inflow gains one if it has readers. The installed entries are the inverse of the new clusters' routes (readers-output lemma), and by Theorem 7 they hold every reader those inflows have under 𝒫′. An inflow received outside *A* has its readers sent by its unchanged receiving cluster (Theorem 7), so their routes, and its entry, are unchanged and still exact.

*Labels.* Call *t* dirty if its old and new labels differ. Three lemmas carry the argument.

- **Lemma A.** If *t* is dirty, then *t* ∈ *D* or some input of its new route is dirty. *Reason:* if *t* ∉ *D* its route is unchanged, and a label is a function of the route and the inputs' labels (the label lemma), so if no input is dirty neither is *t*.
- **Lemma B.** Every transaction pushed on the heap is later than the one being processed, because inputs precede their readers (Theorem 6). Transactions therefore leave the heap in strictly increasing order, each at most once.
- **Lemma C.** Let *m* be the smallest pending transaction when the linear pass starts, or *m* = *n* if it never starts. Every recomputation of a transaction *t* < *m* reads inputs that already hold their new labels, so it produces the new label; and every dirty *t* < *m* enters the heap. *Reason, by strong induction on t < m:* *t* is not pending when the pass starts, because *m* is the smallest pending transaction, so if *t* enters the heap it is popped and recomputed in heap mode. Let *u* < *t* be an input of *t*'s new route. If *u* ever enters the heap, it leaves before *t* does: while *t* is pending the heap would pop *u* first, and once *t* has left only transactions later than *t* are pushed (Lemma B). By induction *u* then received its new label. If *u* never enters the heap, it is clean by induction and still holds its old label, which is its new one. So *t* is recomputed to its new label. If *t* is dirty and not a seed, Lemma A gives a dirty input *u*, which by induction entered the heap and, being smaller than *m*, was popped before the pass. When *u* left, its stored label was still the old one, because a label is written only when its transaction leaves the heap and each leaves once. The recomputed label differed, so *u*'s readers were pushed, and they include *t*: the readers index is already exact for 𝒫′, and *t*'s new route points into *u*.

The restriction to *t* < *m* is needed: once the pass starts, dirty transactions after *m* are recomputed by the pass without ever entering the heap. So a transaction before *m* that never enters the heap is clean, and its stored label is already its new label; if the pass never starts, that covers every transaction. If it starts, every transaction before *m* is final by Lemma C, and the pass recomputes *m*, *m* + 1, … in increasing order, each from inputs that are earlier and therefore final: those before *m* by Lemma C, the others because the pass has just recomputed them. The cutoff skips only the readers of a transaction whose label did not change, and by Lemma A that cannot make any reader dirty; with the cutoff off, extra readers are recomputed to unchanged labels. The fast path for a single piece of a one-label inflow, and the stolen-count shortcut for a one-run label, compute the same values as the general code.

*Holdings.* Before propagation starts, the engine points every wallet of *A*′ at its new cluster and sets that cluster's holdings to the sum of its members' net flows, which still reflect the old labels. Net flows depend only on the stolen counts σ(*t*), which change only when labels change. For each changed label the engine updates the stored stolen count, moves its sender's and receiver's flows by the difference, and moves the holdings of their owners, already the 𝒫′ clusters, by the same difference. So at the end every wallet's flow is its new inner sum, and every cluster, inside *A*′ or not, holds the sum of its members' new flows, which is its 𝒫′ holdings by Theorem 5. ∎

By induction over operations, the state is exact after any sequence of merges, splits and regroups, including undo through the cache.

**Theorem 9 (termination and cost).** `regroup` terminates, and its cost is bounded as follows. Write *n* for the transactions in the ledger. For one regroup, let *M* be the members of the outgoing clusters and *e* their events; the new clusters have the same members, so the same events. Let *P*\_old and *P*\_new be the pieces in the outgoing and the new clusters' routes, *Q* the transactions recomputed, *q* the largest number on the heap at once, and *r*(*t*) the number of readers of *t*. Recomputing *L*ₜ walks each input's runs up to the piece's offset, so it costs at most

```math
c(t) = \sum_{(s,\,o,\,\ell)\,\in\,\mathrm{route}(t)} \big(1 + |L_s|\big)
```

| Step | Time | Extra memory |
| --- | --- | --- |
| 1. Form the new clusters | O(\|blocks\| + \|*M*\|) | O(\|*M*\|) |
| 2. Retire the outgoing clusters (snapshot, unless cached) | O(*e*) in the reference, O(*e* + *P*\_old) in the compiled engine | the same |
| 3. Route the new clusters | O(*e* log *e* + *P*\_old + *P*\_new) with a sweep, O(*P*\_old + *P*\_new) on a cache hit | O(*e* + *P*\_new) |
| 4. Propagate labels | O(Σ\_{*t* ∈ *Q*} (*c*(*t*) + *r*(*t*) log *q*)) | O(*q*) |

To list a cluster's events, the compiled engine sorts them when the cluster is small, O(*e* log *e*), and otherwise marks them and scans the ledger once, O(*n*). In heap mode \|*Q*\| ≤ ⌊*n*/4⌋ + 1, because the switch is tested before each pop; the linear pass then recomputes at most the *n* − *m* transactions from *m* on. In the worst case a regroup therefore costs O(*n* log *n* + *P* + *C*) time and O(*n* + *P*) extra memory, where *P* = Σₜ \|route(*t*)\| and *C* = Σₜ *c*(*t*) over the whole ledger.

For comparison, building the engine costs O(*n* log *n* + *P* + *C*) in the reference, whose per-cluster sorts add up to O(*n* log *n*), and O(*n* + *P* + *C*) in the compiled engine's single pass. The engine's state takes O(*n* + *P* + *R*) memory, where *R* = Σₜ \|*L*ₜ\| counts label runs: routes and readers O(*P*), labels O(*R*), and per-transaction stolen counts and per-wallet flows and owners O(*n*). The cache holds at most *n* routes with their readers, O(*P*). A fresh trace costs Θ(*n* + *R*) time and memory, because it works on runs: each transaction pops, reads or pushes exactly the runs of its own label.

*Proof.* Each iteration of the sweep's inner loop emits one piece of length at least 1 and lowers need by that length, so the loop ends after at most *a* iterations, and it never runs off the stack (Theorem 6). Each transaction enters the heap at most once, because the heap admits only transactions not seen before, and leaves it at most once (Lemma B). The linear pass is a single pass over the remaining transactions. So `regroup` terminates. For the costs: step 1 works on member sets; step 2 visits each event of an outgoing cluster once, and the compiled engine also copies its pieces; step 3 lists and sweeps the events, the sweep's loop running once per piece emitted, then compares each new route with the old one and installs the routes and readers; step 4 recomputes each transaction of *Q* at cost *c*(*t*) and pushes each of its readers once. ∎

**The worst case is not a small multiple of a fresh trace.** The linear pass bounds the number of recomputations, not their cost against a fresh trace: an update works on pieces, a fresh trace on runs, and *P* and *C* can exceed *R* by a factor of order *n*. Take *K* one-unit deposits into a wallet A, then *K* transfers of the whole balance back and forth between A and B. Apart, every transfer after the first reads a single block. Merged, each one is an internal read of all *K* deposits, so *P* = *K*², while every label stays a single run and *R* = 2*K*. [`experiments.py`](../experiments.py) measures that merge at 40–673 times the time of a fresh trace, depending on *K* and the engine. On the benchmark networks *P* and *R* both stay within 1.3*n*, so there the bounds are O(*n* log *n*), and the slowest update with the default settings, merging all four exchanges at 10k in the compiled engine, took 1.5 times a fresh trace ([the report](report.md#8-scaling-benchmarks)).

## Mechanical checks

Every theorem is backed by at least one machine check. The Z3 checks are proofs about a transcription of the code. They cover the sweep's event code for all inflow ids, block sizes and amounts at stack depths up to 12 blocks, and Theorem 3 for every permutation of up to 5 units. The exhaustive run executes both shipped engines, the pure-Python reference and the compiled one, on every case in a stated small scope. After every operation it compares the engine's whole state with a reference built from the unit-level specification alone.

| Theorem | Z3 ([`verify/smt_checks.py`](../verify/smt_checks.py)) | Exhaustive, both engines ([`verify/exhaustive.py`](../verify/exhaustive.py)) |
| --- | --- | --- |
| T1 Well-definedness and conservation | — | Asserted on the specification in all 1,944,228 cases |
| T2 First passage | — | Asserted for every unit in every case |
| T3 Push order | All 153 permutations of up to 5 units | — |
| T4 Routing locality | — | Asserted on the specification: a membership carries the same units in every clustering of its ledger; every cache entry is checked against it |
| T5 Holdings identity | — | Asserted on the specification for every cluster; the engine's net flows and holdings equal it after every operation |
| T6 Run-length stack | All 313 feasible paths of the sweep's event code, stacks of up to 12 blocks, every id, block size and amount | Every route equals the specification's carried units, as maximal runs, after every operation |
| T7 Readers locality | — | Asserted for every piece; the readers index equals the inverse of the routes after every operation |
| Lemmas: labels from pieces, build | The one-pass build's routes equal the sweep's (fidelity check) | Every label equals the specification's after every build and every operation |
| T8 Incremental correctness | — | 347,056,276 engine states per engine, each compared in full: fresh builds, every single-block regroup with undo and a walk through every ordered pair of clusterings under 5 engine settings, and every merge and split with undo under the default settings |
| T9 Termination | No feasible sweep path runs off the stack | Every run terminates |

**Scope of the exhaustive run.** Two disjoint scopes, both with amounts 1–2 and a stolen and a clean source, enumerated up to renaming of wallets. The first is every valid ledger of up to 5 transactions using at most 3 wallets: 192,286 ledgers. The second is every valid ledger of up to 5 transactions using exactly 4 wallets: 76,234 ledgers. Four wallets is the fewest that allows two multi-wallet clusters, so the second scope includes regroups that cut two clusters and leave two remainders. Each ledger runs under every clustering, 1,944,228 cases in all. The five engine settings are the defaults, heap-only propagation with and without the cutoff, and an immediate linear pass with no cache or a one-route cache. The fallback, eviction, cache-hit and undo paths therefore all run inside the scope. Each engine was run over the whole scope. The reference took 65 minutes on 2 cores. The compiled engine took 4 hours 19 minutes, in five runs, because on ledgers this small each operation's cost is the fixed overhead of calling the kernels, plus exporting the state to Python for each comparison and checking the hidden state of every 16th. For the compiled engine, two of the settings also compact its arenas whenever dead rows outnumber live ones.

**Guards against vacuous checks.**

- **Z3 self-test.** Nine bugs are injected into the transcribed sweep code, and each is refuted by a counterexample.
- **Fidelity.** The transcription Z3 verifies computes the same routes as `lifo.sweep` and as both compiled code paths, the regroup sweep and the one-pass build of `lifo_nb`, on all 127,399 routes of 3,000 random ledgers.
- **Harness mutation test.** Twelve bugs injected into [`lifo.py`](../lifo.py), and twenty-two into [`lifo_nb.py`](../lifo_nb.py), are each caught by the random suite ([`mutate.py`](../mutate.py) `--exhaustive`, with `--nb` for the compiled engine). The exhaustive run, on its quick scopes, catches every one it can reach. It skips the three that weaken a self-check, which the random suite's negative tests cover, and its scope never grows the holdings array past 16 cluster ids. Among the bugs are a dropped remainder cluster, stale readers left behind on a cache hit, a compaction that loses a row, heap marks never cleared, and a live-row counter that drifts and drifts back, which only the mid-run hidden-state checks catch. One candidate was set aside as an equivalent mutant: making the compiled fresh trace pop and push back on an internal read changes nothing, by Theorem 3.
- **Bounds checks.** The compiled kernels run without array bounds checks, so a stray index would not fail but read or corrupt memory. The lockstep suite was also run with bounds checks compiled in, 6,126 operations plus the targeted tests, and none fired. The checks follow NumPy in accepting negative indices, so they cannot catch an index of −1.

**What remains trusted.** Beyond the scope, Theorems 2, 4, 7 and 8 rest on the textual proofs above, which the exhaustive run checks only within its bounds. Theorem 6 is proved by Z3 only for stacks of up to 12 blocks, and Theorem 3 only for transfers of up to 5 units; their textual proofs cover any depth and amount. Theorems 1 and 5 have no Z3 check: once Part 1 fixes each transaction's effect on the stack, their induction steps are plain arithmetic, so beyond the scope they rest on their textual proofs. Theorem 9's termination has no machine check beyond Z3's proof that no sweep path runs off the stack and the termination of every run; its cost bounds are measured by the benchmarks, not checked. The compiled engine computes in 64-bit integers; its refusal of ledgers whose total volume reaches 2⁶² rests on the bound argued in [the report](report.md#9-limits-and-what-does-not-get-faster), every quantity being at most twice that volume, which the tests probe just below the limit but no proof checks. The Z3 transcription is tested against the shipped code, not proved identical to it. The specification itself (`spec_trace`) is trusted as a literal transcription of the definitions. So are Z3, the Python interpreter, and Numba with LLVM, which compile the kernels.

---
[← The report](report.md) · [Documentation](README.md)
