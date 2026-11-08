# 1. The problem

[← Index](README.md)

## LIFO tracing in one paragraph

When stolen money moves through many wallets and mixes with clean money, an investigator needs a rule for deciding which dollars went where. LIFO ("last in, first out") is one such rule: every entity spends the money it received most recently first. Picture each entity holding a stack of banknotes. Money that arrives goes on top, and a payment takes notes off the top. Each note remembers whether it came from the stolen source or a clean one, so following the notes tells you how much stolen money every payment carried.

## Clusters, and why they change

An entity is not one wallet. Investigators group wallets that they believe share an owner into a *cluster*, and each cluster has one shared stack. Money moving between two wallets of the same cluster is an *internal* transfer: the owner just moved it between their own pockets, so it changes nothing.

Clusters change as an investigation learns more. New evidence may show that two clusters are one entity (a *merge*), or that a grouping was wrong (a *split*). Either change can alter which notes later payments take, and so how much stolen money reaches each exchange.

## The challenge

[The Heights Labs challenge](challenge.pdf) asks for an algorithm that updates a LIFO trace after clusters merge or split, without retracing the whole network, and with results *exactly* equal to a fresh trace under the new clustering. It also asks to tell apart changes that really move attribution from changes that only reshape the graph.

## The worked example

The challenge gives 15 transactions in which $100 of stolen funds is split, mixed with $70 of clean funds, and paid into two exchanges, X and Y. The ledger is in [`toy.py`](../toy.py#L12). Three events follow: B and E turn out to be one entity, then D and F do too, and finally the B–E grouping is reversed while D–F stays.

| State | Stolen to X | Stolen to Y | What changed |
| --- | --- | --- | --- |
| Every wallet on its own | $30 | $20 | Stolen still held: B 30, F 10, I 10 |
| + B–E merged | $30 | $20 | Nothing |
| + D–F merged | $10 | $20 | D's last $20 payment, to H, now carries clean money instead of stolen |
| − B–E split again | $10 | $20 | Nothing further |

Merging B and E changes nothing because E's money arrives and leaves while B sits idle: in the shared stack, E's $40 lands on top of B's $30 and E's payment takes exactly it. Merging D and F matters because D's stolen $20 now stays inside D+F instead of leaving through H. [`results/toy.log`](../results/toy.log) shows every transaction in all four states, and [page 3](03-the-update.md#walking-through-the-challenge-ledger) walks through how the engine reaches these answers.

## The rules, made exact

The challenge's rules leave a few things open. The code fixes them as follows (the model at the top of [`lifo.py`](../lifo.py#L4-L15)).

- Transactions run in ledger order. Amounts are positive integers (cents, satoshis or wei), so there is no floating point anywhere and "exact" means integer equality.
- Each cluster has one LIFO stack of labelled units. Sources (stolen, clean) are not clusters; exchanges are ordinary clusters.
- A deposit from a source pushes its amount with the source's label. Several hacks can use several stolen labels.
- An external transfer pops the top *a* units of the sender's stack and pushes them onto the receiver's stack **in the same order**.
- An internal transfer changes nothing. It still reports what it carried: the top *a* units.
- The outputs are, for every transaction, the labels of the funds it moved (as runs, bottom to top), and for every cluster, the stolen units left on its stack at the end.

**Assumption: a transfer's funds keep their order.** The challenge does not say in what order a transfer's funds land on the receiver's stack, so the code fixes it: the unit that was on top at the sender is on top at the receiver. You can see it in [`fresh_trace`](../lifo.py#L149), which pushes the carried runs bottom first ([line 200](../lifo.py#L200)), and in [`sweep`](../lifo.py#L214), which pushes an inflow as one block in its route's order ([lines 232–234](../lifo.py#L232-L234)). The choice matters: pushing funds upside down would send $40–$60 of stolen money to X instead of $30 and $10. Keeping the order is the only choice under which an internal transfer, seen as "pop, then push back", leaves the stack unchanged ([Theorem 3](proofs.md#the-formulation)), which is what the challenge's rule about internal transfers requires. [Section 3 of the report](report.md#3-semantics-fixed-precisely) lists every place in the code that implements it.

## Where "correct" is defined in code

Two pieces of code define the problem, and everything else is measured against them.

- [`Ledger`](../lifo.py#L67) validates a ledger: no self-transfers, no payments into a source, no overdrafts, integer amounts only. It also lists, for every wallet, the transactions that touch it.
- [`fresh_trace`](../lifo.py#L149) is the from-scratch trace: one stack of (label, units) runs per cluster, run over the whole ledger. The incremental engine must always equal it.

An even more literal tracer, one that moves single units, lives in the tests ([`unit_trace`](../test_engine.py#L80) and [`spec_trace`](../verify/exhaustive.py#L151)) and checks `fresh_trace` itself.

---
[← Index](README.md) · Next: [2. The idea: routes and labels →](02-routes-and-labels.md)
