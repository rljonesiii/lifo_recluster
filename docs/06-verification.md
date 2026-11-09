# 6. How we know it is right

[← 5. The compiled engine](05-compiled-engine.md) · [Index](README.md)

"Exact" is a strong claim, so it rests on several independent kinds of evidence. Each catches mistakes the others could miss. The theorems themselves, with full proofs, are in the [appendix](proofs.md).

| Check | Code | What it shows | Log |
| --- | --- | --- | --- |
| The challenge ledger | [`toy.py`](../toy.py), [`verify/toy_independent.py`](../verify/toy_independent.py) | Both engines, the fresh trace, a unit-by-unit tracer and a separately written tracer agree on all four states | [toy](../results/toy.log), [independent](../results/toy_independent.log) |
| Random differential tests | [`test_engine.py`](../test_engine.py) | After 36,190 random merges, splits and regroups, every state equals a fresh trace and a from-scratch rebuild | [reference](../results/tests.log), [compiled](../results/tests_nb.log) |
| Z3 proofs | [`verify/smt_checks.py`](../verify/smt_checks.py) | Every path through the sweep's code is correct for all ids and amounts, on stacks of up to 12 blocks | [smt](../results/smt.log) |
| Bounded exhaustive check | [`verify/exhaustive.py`](../verify/exhaustive.py) | Every small ledger, every clustering, every operation: 347 million engine states, all equal to the unit-level specification | [reference](../results/exhaustive.log), [compiled](../results/exhaustive_nb.log) |
| Mutation tests | [`mutate.py`](../mutate.py) | The checks above are not vacuous: 34 planted bugs, all caught | [reference](../results/mutation.log), [compiled](../results/mutation_nb.log) |

## 1. The challenge ledger, three ways

[`toy.py`](../toy.py) runs the 15 transactions through all three events and checks, in every state, that the compiled engine, the reference engine, [`fresh_trace`](../lifo.py#L149) and a unit-by-unit tracer give the same answer. [`verify/toy_independent.py`](../verify/toy_independent.py) is a second tracer written from the challenge's rules alone, with lots of (label, dollars) and no code shared with the package; its [`run`](../verify/toy_independent.py#L40) is the whole algorithm in about 20 lines. It agrees too.

## 2. Random differential tests

[`test_engine.py`](../test_engine.py) builds random ledgers ([`random_ledger`](../test_engine.py#L37)) and clusterings ([`random_partition`](../test_engine.py#L69)), then applies random merges, splits and regroups ([`operate`](../test_engine.py#L168)). After every operation, [`check`](../test_engine.py#L111) requires that:

- labels and holdings equal a fresh trace;
- routes, readers and net flows equal an engine rebuilt from scratch;
- readers are exactly the inverse of the routes;
- on small amounts, the fresh trace itself equals a unit-by-unit tracer ([`unit_trace`](../test_engine.py#L80)).

The engine settings rotate (defaults, an aggressive linear pass, a tiny or empty cache, no cutoff), so each mechanism is also tested on its own.

With `--nb`, the compiled engine runs **in lockstep** with the reference ([`lockstep`](../test_engine.py#L151)): after every step the two must agree on their whole state, cache order and work counters included, and the compiled engine's hidden state must stay consistent. Every run, with or without `--nb`, then goes on to [`targeted`](../test_engine.py#L239) tests of the edges (amounts near the 64-bit limit, every kind of threshold, the holdings array growing), and to [`validation`](../test_engine.py#L365), which checks that bad input is rejected before anything changes.

## 3. Z3 proofs of the sweep

[`verify/smt_checks.py`](../verify/smt_checks.py) uses the Z3 SMT solver on a line-by-line transcription of the sweep's per-event code ([`inflow_step`](../verify/smt_checks.py#L46), [`sent_step`](../verify/smt_checks.py#L50)):

- **Part 1** ([`explore_sent`](../verify/smt_checks.py#L139)) runs every feasible path symbolically, with unbounded integer ids, block sizes and amounts, for stacks of 0 to 12 blocks: 313 paths. On each, Z3 proves that the pieces are exactly the top units of the stack and that the stack left behind is right ([Theorem 6](proofs.md#data-structures)).
- **Part 2** ([`part2`](../verify/smt_checks.py#L214)) checks all 153 ways of pushing up to 5 units back: only the order-preserving push leaves every label unchanged ([Theorem 3](proofs.md#the-formulation)).

Conservation and holdings ([Theorem 1](proofs.md#the-formulation) and [Theorem 5](proofs.md#factorization)) have no Z3 check. Once Part 1 fixes each transaction's effect on the stack, their induction steps are plain arithmetic, so they rest on their proofs and on the exhaustive check below, which asserts both in every case.

Two guards keep the proofs honest. [`self_test`](../verify/smt_checks.py#L242) plants 9 bugs in the transcription, and Z3 must refute each with a counterexample. [`fidelity`](../verify/smt_checks.py#L267) runs the transcription on 3,000 random ledgers and requires the same 127,399 routes as `lifo.sweep` and both compiled code paths.

## 4. The bounded exhaustive check

[`verify/exhaustive.py`](../verify/exhaustive.py) tries *everything* in a small scope: every valid ledger of up to 5 transactions over up to 4 wallets, with amounts 1–2 ([`ledgers`](../verify/exhaustive.py#L76)), under every clustering ([`partitions`](../verify/exhaustive.py#L108)), followed by every single-block regroup with undo, every merge and split with undo, and a walk through every ordered pair of clusterings. That is 268,520 ledgers and 1,944,228 (ledger, clustering) cases.

The yardstick is [`spec_trace`](../verify/exhaustive.py#L151), a literal unit-by-unit reading of the definitions, from which [`reference`](../verify/exhaustive.py#L245) builds the expected state without using the engine. [`check_state`](../verify/exhaustive.py#L296) compares the engine's whole state with it after every operation: 347,056,276 states per engine, all equal. Along the way the lemmas T1, T2, T4, T5 and T7 are asserted on the specification itself.

Small scopes find most bugs, because most bugs already show up in small cases. This is Daniel Jackson's small-scope hypothesis, the idea behind Alloy.

## 5. Mutation tests

A test suite that passes proves little if it would also pass with a bug in the code. [`mutate.py`](../mutate.py) plants bugs one at a time (12 in the reference engine, [`MUTS_PY`](../mutate.py#L29), and 22 in the compiled one, [`MUTS_NB`](../mutate.py#L45)), such as stale readers left behind after a cache hit, a dropped remainder cluster, or an arena compaction that loses a row. The random suite catches all 34. The exhaustive check catches every one its small scope can reach.

## What is still trusted

Beyond the exhaustive scope, Theorems 2, 4, 7 and 8 rest on their written proofs. Z3 proves Theorem 6 only for stacks of up to 12 blocks, and Theorem 3 only for up to 5 units, though their written proofs cover any size. Theorems 1 and 5 have no Z3 check and, beyond the scope, rest on their written proofs. Theorem 9's termination has no machine check beyond Z3's proof that no sweep path runs off the stack and the fact that every run terminates; its cost bounds are measured by the benchmarks, not checked. The compiled engine's 64-bit bound is argued, not proved; the tests probe it just below the limit. The Z3 transcription is tested against the shipped code, not proved identical to it. The specification itself, `spec_trace`, is trusted as a literal reading of the definitions, and so are Z3, Python and Numba. The [appendix](proofs.md#mechanical-checks) lists exactly which check backs which theorem.

---
[← 5. The compiled engine](05-compiled-engine.md) · Next: [7. Benchmarks →](07-benchmarks.md)
