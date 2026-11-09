# Results

Raw output of every run reported in the report, all on the final code and on the machine
described below. Each log starts with the command line (`$ ...`) and ends with the exit
status and wall time. All commands run from the package root, in the environment `uv sync`
installs.

| Log | Command | What it shows |
| --- | --- | --- |
| `toy.log` | `uv run python toy.py` | The challenge ledger in its four states; both engines, the reference tracer and the unit-by-unit tracer agree |
| `toy_independent.log` | `uv run python verify/toy_independent.py` | The same ledger traced by a separate implementation written from the rules alone, compared with the package |
| `tests.log` | `uv run python test_engine.py 2000 S` for seeds 11, 12, 13 | The reference engine on random ledgers and operations, then the targeted and validation tests |
| `tests_nb.log` | `uv run python test_engine.py 2000 S --nb` for seeds 11, 12, 13 | The compiled engine in lockstep with the reference |
| `tests_nb_boundscheck.log` | `NUMBA_BOUNDSCHECK=1 NUMBA_CACHE_DIR=<separate dir> uv run python test_engine.py 1000 11 --nb` | The lockstep suite with array bounds checks compiled into every kernel |
| `mutation.log` | `uv run python mutate.py --exhaustive` | 12 bugs injected into `lifo.py`, each against the random suite and the exhaustive quick scopes |
| `mutation_nb.log` | `uv run python mutate.py --exhaustive --nb` | 22 bugs injected into `lifo_nb.py` |
| `smt.log` | `uv run python verify/smt_checks.py` | The Z3 proofs, their self-test and the fidelity check |
| `exhaustive.log` | `uv run python verify/exhaustive.py` | The reference engine over the whole bounded scope (S1 and S2) |
| `exhaustive_nb.log` | `uv run python verify/exhaustive.py --nb S1`, then `--nb S2 0/4`, `1/4`, `2/4` and `3/4` | The compiled engine over the same scope, in five runs |
| `n10000.*`, `n100000.*`, `n1000000.*` | `uv run python bench.py N --repeat 5 --mem --ablate --out results/nN.json` | The reference engine's benchmark; `.json` holds every timing, work counter and memory reading, and the machine |
| `nb_n10000.*`, `nb_n100000.*`, `nb_n1000000.*` | `uv run python bench.py N --nb --repeat 5 --mem --ablate --out results/nb_nN.json` | The compiled engine's benchmark |
| `experiments.log` | `uv run python experiments.py` | The micro-benchmarks behind "compile rather than vectorize", the worst case, and the route-stability check, on the 1M network |

[`SOURCES.md5`](SOURCES.md5) holds the checksums of the code these logs ran on
(`md5sum -c results/SOURCES.md5` from the package root).

**Machine.** A cloud virtual machine with 2 vCPUs of an Intel Xeon at 2.10 GHz (with AVX-512
and AMX, so Sapphire Rapids or later) and 7.8 GB of RAM, running Ubuntu 24.04.5 on Linux 6.18;
Python 3.12.3, NumPy 2.5.4, Numba 0.68.1 (llvmlite 0.50.0), z3-solver 5.3.3 and psutil
7.2.2, installed by uv 0.11.32 from `uv.lock`. Each benchmark
result file records the machine and versions under `machine`.

**Timings.** The benchmarks ran one at a time with nothing else on the machine. In the main
run, each operation and each fresh trace is the median of 5 runs of the whole script on
fresh engines; the JSON keeps all 5 (`inc_runs`, `fresh_runs`). The two ablation runs and
the memory run are single runs. Work counters and results never vary between runs, and
the benchmark checks that they do not.

**Memory.** The reference engine's figures come from tracemalloc: what the ledger and the
engine hold, and each step's peak above what was held before it. tracemalloc cannot see
memory that compiled code allocates, so for the compiled engine the benchmark records the
exact size of its arrays, and each step's peak as the rise of the process's peak resident
set (VmHWM, reset before the step). For that run every allocation over 64 KiB is served by
mmap, and free heap memory is handed back to the system before each step, so memory freed
by earlier steps cannot be reused unseen. That reading is accurate to about half a
megabyte.
