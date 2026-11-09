# 8. Running it

[← 7. Benchmarks](07-benchmarks.md) · [Index](README.md)

The project uses [uv](https://docs.astral.sh/uv/) to manage Python and its dependencies. [`pyproject.toml`](../pyproject.toml) lists them, [`uv.lock`](../uv.lock) pins exact versions, and [`.python-version`](../.python-version) selects Python 3.12. uv downloads that Python if you do not have it.

## Setup

Install uv once (see [its installation guide](https://docs.astral.sh/uv/getting-started/installation/); on macOS `brew install uv` also works), then, from the repository root:

```
uv sync
```

This creates `.venv/` with everything the code needs:

| Package | Used for |
| --- | --- |
| `numpy` | the compiled engine's arrays |
| `numba` | compiling the engine's kernels |
| `z3-solver` | the SMT proofs in [`verify/smt_checks.py`](../verify/smt_checks.py) |
| `psutil` | memory readings in [`bench.py`](../bench.py) |
| `matplotlib` (the `docs` group only) | redrawing [the figures](figures/make_figures.py) |

Everything else is the Python standard library. Without NumPy and Numba the code still runs, on the reference engine only.

## The first run

```
uv run python toy.py
```

This traces the challenge ledger through all three events and prints every transaction's attribution, the stolen totals reaching X and Y, and the work each event took. The first run takes 10–15 seconds because Numba compiles the kernels; later runs load them from its cache and finish in about a second. The output should match [`results/toy.log`](../results/toy.log).

## Reproducing every result

Each command prints what the logs in [`results/`](../results/) recorded; [`results/README.md`](../results/README.md) gives the exact command behind each log. The benchmark commands also write the JSON file named by `--out`. Times are from the 2-core cloud machine described in [the report](report.md#8-scaling-benchmarks).

| Command | What it checks | Time |
| --- | --- | --- |
| `uv run python toy.py` | The challenge ledger, both engines and both tracers | ~1 s |
| `uv run python verify/toy_independent.py` | The same ledger, traced by a separate implementation | ~1 s |
| `uv run python verify/smt_checks.py` | The Z3 proofs, their self-test and the fidelity check | ~10 s |
| `uv run python test_engine.py 2000 11` | The reference engine on 2,000 random ledgers (also seeds 12 and 13) | ~25 s |
| `uv run python test_engine.py 2000 11 --nb` | The compiled engine in lockstep with the reference | ~65 s |
| `uv run python verify/exhaustive.py --quick` | A short exhaustive run (up to 4 transactions) | ~80 s |
| `uv run python verify/exhaustive.py` | The full bounded exhaustive check, reference engine | ~65 min |
| `uv run python verify/exhaustive.py --nb S1` | The same for the compiled engine; then `--nb S2 0/4` through `--nb S2 3/4` | ~4 h in all |
| `uv run python mutate.py --exhaustive` | 12 planted bugs in the reference engine | ~35 s |
| `uv run python mutate.py --exhaustive --nb` | 22 planted bugs in the compiled engine | ~20 min |
| `uv run python bench.py 1000000 --nb --repeat 5 --mem --ablate --out results/nb_n1000000.json` | The 1M benchmark, compiled engine: five timed runs, two ablations, one memory run | ~4 min |
| `uv run python bench.py 1000000 --repeat 5 --mem --ablate --out results/n1000000.json` | The same on the reference engine | ~20 min |
| `uv run python experiments.py` | Compile versus vectorize, the worst case, and the route-stability check, on the 1M network | ~1 min |

The benchmarks also run at `10000` and `100000`. To redraw the figure after a new benchmark run:

```
uv run --group docs python docs/figures/make_figures.py
```

`NUMBA_BOUNDSCHECK=1`, with a separate `NUMBA_CACHE_DIR`, compiles the kernels with array bounds checks; [`results/tests_nb_boundscheck.log`](../results/tests_nb_boundscheck.log) is the lockstep suite run that way.

## Using the engine in your own code

```python
from engine import Ledger, Engine

L = Ledger(src, dst, amt, sources={STOLEN: 1, CLEAN: 0}, n_wallets=W)
eng = Engine(L, clusters)        # disjoint wallet sets; other wallets start on their own
eng.merge([b, e])                # merge the clusters containing wallets b and e
eng.split([e])                   # split {e} out of its cluster
eng.regroup([{d, f}, {b}])       # any regrouping in one call; the sets must be disjoint
eng.labels[t]                    # funds moved by transaction t: (label, units, label, units, ...)
eng.cluster_of()                 # wallet -> cluster key, and stolen holdings per cluster
eng.reset_stats()                # the work done since the last call
```

Wallet ids, amounts and labels must be integers. Anything invalid (an unknown wallet, overlapping sets, a source inside a cluster, an overdraft) raises `ValueError` before anything changes. [Page 4](04-reference-engine.md#the-engines-state) describes the state and the work counters.

## Without uv

`pip install numpy numba z3-solver psutil` into any Python 3.10–3.13 environment, then run the same commands with `python` in place of `uv run python`.

---
[← 7. Benchmarks](07-benchmarks.md) · [Index](README.md) · The full write-up: [the report](report.md)
