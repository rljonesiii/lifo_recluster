"""The 15-transaction challenge ledger: fresh trace vs incremental engine for
baseline, +B-E, +D-F, -B-E.  Prints per-transaction attribution and holdings.
Runs the compiled engine when Numba is installed (engine.py) and checks every state
against the reference engine, the fresh trace and the unit-by-unit tracer."""
import lifo
from lifo import Ledger, fresh_trace, stolen_of
from engine import Engine, COMPILED
from test_engine import unit_trace

NAMES = ["Stolen", "Clean", "A", "B", "C", "D", "E", "F", "G", "H", "I", "X", "Y"]
IDX = {n: i for i, n in enumerate(NAMES)}
ROWS = [  # time, sender, receiver, amount
    (1, "Stolen", "A", 100), (2, "A", "B", 60), (3, "A", "C", 40), (4, "Clean", "B", 40),
    (5, "B", "D", 70), (6, "C", "E", 40), (7, "D", "F", 50), (8, "E", "F", 40),
    (9, "Clean", "F", 30), (10, "F", "G", 80), (11, "D", "H", 20), (12, "G", "I", 50),
    (13, "H", "I", 20), (14, "I", "X", 60), (15, "G", "Y", 30),
]


def ledger():
    src = [IDX[r[1]] for r in ROWS]
    dst = [IDX[r[2]] for r in ROWS]
    amt = [r[3] for r in ROWS]
    return Ledger(src, dst, amt, {IDX["Stolen"]: 1, IDX["Clean"]: 0}, len(NAMES), NAMES)


def fmt_runs(runs):
    return "[" + ", ".join(("S" if runs[i] else "C") + str(runs[i + 1])
                           for i in range(0, len(runs), 2)) + "]"


def clustering_names(eng):
    groups = {}
    for st in eng.clusters():
        groups[min(st.members)] = "".join(sorted(NAMES[w] for w in st.members))
    return groups


def main(verbose=True):
    L = ledger()
    eng = Engine(L)
    ref = lifo.Engine(L)  # the reference, run in step
    snapshots = []

    def snap(title):
        co, hold = eng.cluster_of()
        out, fhold = fresh_trace(L, co)
        assert out == eng.labels, title
        assert list(eng.labels) == ref.labels and list(eng.route) == ref.route, title
        uout, uhold = unit_trace(L, co)  # independent unit-by-unit tracer
        assert uout == out and {k: v for k, v in uhold.items() if v} == \
            {k: v for k, v in fhold.items() if v}, title
        names = clustering_names(eng)
        holds = {names[k]: v for k, v in hold.items() if v}
        assert {names[k]: v for k, v in fhold.items() if v} == holds
        snapshots.append((title, list(eng.labels), holds, co))

    snap("Baseline (all singletons)")
    eng.reset_stats()
    eng.merge([IDX["B"], IDX["E"]]); ref.merge([IDX["B"], IDX["E"]])
    s1 = eng.reset_stats(); snap("Event 1: merge B+E")
    eng.merge([IDX["D"], IDX["F"]]); ref.merge([IDX["D"], IDX["F"]])
    s2 = eng.reset_stats(); snap("Event 2: merge D+F")
    eng.split([IDX["E"]]); ref.split([IDX["E"]])
    s3 = eng.reset_stats(); snap("Event 3: split B|E, D+F kept")

    if verbose:
        hdr = f"{'t':>3} {'tx':<12}{'amt':>4}  " + "  ".join(f"{s[0][:22]:<24}" for s in snapshots)
        print(hdr)
        for t, r in enumerate(ROWS):
            cells = []
            for title, labels, holds, co in snapshots:
                internal = co[L.src[t]] is not None and co[L.src[t]] == co[L.dst[t]]
                c = f"{stolen_of(labels[t]):>3} {fmt_runs(labels[t])}" + (" int" if internal else "")
                cells.append(f"{c:<24}")
            print(f"{r[0]:>3} {r[1]+'->'+r[2]:<12}{r[3]:>4}  " + "  ".join(cells))
        which = "compiled engine (lifo_nb)" if COMPILED else "engine (lifo)"
        print(f"\nEvery state above: {which} = reference engine = fresh trace = unit-by-unit tracer.\n")
        for title, labels, holds, co in snapshots:
            ex = {n: stolen_of(labels[t]) for t, n in ((13, "X"), (14, "Y"))}
            print(f"{title:<32} stolen to X={ex['X']:>3}  Y={ex['Y']:>3}   stolen still held: {holds}")
        print()
        for name, s in (("B+E", s1), ("D+F", s2), ("split B|E", s3)):
            print(f"{name:<10} events_swept={s['swept']} routes_changed={s['route_changed']} "
                  f"relabeled={s['relabeled']} labels_changed={s['label_changed']} "
                  f"cache_hits={s['cache_hits']}")
        # semantics sensitivity: push popped funds upside down
        print("\nSensitivity: if a transfer's funds were pushed upside down (pop order):")
        for title, _, _, co in snapshots:
            out, fh = fresh_trace(L, co, order="reverse")
            print(f"  {title:<32} X={stolen_of(out[13]):>3}  Y={stolen_of(out[14]):>3}")
    return snapshots


if __name__ == "__main__":
    main()
