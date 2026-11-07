# Results

Raw output of every run reported in the report, on the final code, on a 2-core machine.
Each log starts with the command line (`$ ...`) and ends with the exit status and wall
time. All commands run from the package root.

| Log | Command | What it shows |
| --- | --- | --- |
| `toy.log` | `python3 toy.py` | The challenge ledger in its four states; both engines, the reference tracer and the unit-by-unit tracer agree |
| `toy_independent.log` | `python3 verify/toy_independent.py` | The same ledger traced by a separate implementation written from the rules alone, compared with the package |
| `tests.log` | `python3 test_engine.py 2000 S` for seeds 11, 12, 13 | The reference engine on random ledgers and operations, then the targeted and validation tests |
| `tests_nb.log` | `python3 test_engine.py 2000 S --nb` for seeds 11, 12, 13 | The compiled engine in lockstep with the reference |
| `tests_nb_boundscheck.log` | `NUMBA_BOUNDSCHECK=1 NUMBA_CACHE_DIR=<separate dir> python3 test_engine.py 1000 11 --nb` | The lockstep suite with array bounds checks compiled into every kernel |
| `mutation.log` | `python3 mutate.py --exhaustive` | 12 bugs injected into `lifo.py`, each against the random suite and the exhaustive quick scopes |
| `mutation_nb.log` | `python3 mutate.py --exhaustive --nb` | 22 bugs injected into `lifo_nb.py` |
| `smt.log` | `python3 verify/smt_checks.py` | The Z3 proofs, their self-test and the fidelity check |
| `exhaustive.log` | `python3 verify/exhaustive.py` | The reference engine over the whole bounded scope (S1 and S2) |
| `exhaustive_nb.log` | `python3 verify/exhaustive.py --nb S1`, then `--nb S2 0/4`, `1/4`, `2/4` and `3/4` | The compiled engine over the same scope, in five runs |
| `n10000.*`, `n100000.*`, `n1000000.*` | `python3 bench.py N --mem --ablate --out results/nN.json` | The reference engine's benchmark; `.json` holds every timing, work counter and memory reading |
| `nb_n10000.*`, `nb_n100000.*`, `nb_n1000000.*` | `python3 bench.py N --nb --mem --ablate --out results/nb_nN.json` | The compiled engine's benchmark |
| `experiments.log` | `python3 experiments.py` | The micro-benchmarks behind "compile rather than vectorize", on the 1M network |

The benchmarks ran one at a time with nothing else on the machine. Each operation is timed
once, and single timings vary from run to run: the 14 compiled fresh traces at 1M, nearly the
same computation, span 199-225 ms, and operations that the routing cache cannot affect differ
by up to 30% between the main and cache-off runs. Work counters and results do not vary.
