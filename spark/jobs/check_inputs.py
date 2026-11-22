"""Check of the input rules, in local mode: build.violations on one bad ledger per rule
and on a valid one.

    PYTHONPATH=jobs python jobs/check_inputs.py [--balance-bins N]

Each bad ledger must be flagged by exactly its rule, and the valid one by none. Wallet 0 is
a stolen source and wallet 9 a clean one. Then, on a random ledger full of overdrafts, the
overdraft check in time bins must flag the same rows as the one window per wallet.
--balance-bins is the job's; the cases run with it.
"""
import argparse
import random
import sys

from pyspark.sql import SparkSession

from lifo_spark.build import overdrafts, violations

SOURCES = [(0, 1), (9, 0)]
CASES = [  # (name, transfers as (seq, src, dst, amount), sources, membership, rules flagged)
    ("valid ledger",
     [(0, 0, 1, 10), (1, 9, 1, 5), (2, 1, 2, 6), (3, 1, 3, 2)], SOURCES, [], set()),
    ("two transfers share seq 2",
     [(0, 0, 1, 10), (1, 9, 1, 5), (2, 1, 2, 6), (2, 1, 3, 2)], SOURCES, [], {"seq not unique"}),
    ("a zero amount",
     [(0, 0, 1, 10), (1, 9, 1, 0), (2, 1, 2, 3)], SOURCES, [], {"amount not positive"}),
    ("a self-transfer",
     [(0, 0, 1, 10), (1, 1, 1, 4)], SOURCES, [], {"self-transfer"}),
    ("a negative wallet id",
     [(0, 0, 1, 10), (1, 1, -2, 4)], SOURCES, [], {"negative wallet id"}),
    ("an overdraft its cluster covers",
     [(0, 0, 1, 10), (1, 2, 3, 5)], SOURCES, [(1, 0), (2, 0)], {"wallet overdraws"}),
    ("an overdraft after several moves",
     [(0, 0, 1, 10), (1, 1, 2, 6), (2, 9, 3, 1), (3, 3, 4, 1), (4, 9, 5, 1), (5, 1, 2, 6)],
     SOURCES, [], {"wallet overdraws"}),
    ("a source receives",
     [(0, 0, 1, 10), (1, 1, 0, 4), (2, 0, 2, 3)], SOURCES, [], {"source receives"}),
    ("a source listed twice",
     [(0, 0, 1, 10), (1, 1, 2, 4)], [(0, 1), (0, 2)], [], {"source listed twice"}),
    ("a negative label",
     [(0, 0, 1, 10)], [(0, -1), (9, 0)], [], {"negative label"}),
    ("a wallet in two clusters",
     [(0, 0, 1, 10), (1, 1, 2, 4)], SOURCES, [(1, 0), (1, 1)], {"wallet in two clusters"}),
    ("a negative cluster id",
     [(0, 0, 1, 10), (1, 1, 2, 4)], SOURCES, [(1, -3)], {"negative cluster id"}),
    ("a source clustered",
     [(0, 0, 1, 10), (1, 1, 2, 4)], SOURCES, [(0, 0), (1, 0)], {"source clustered"}),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--balance-bins", type=int, help="the job's --balance-bins")
    args = ap.parse_args()
    spark = (SparkSession.builder.master("local[2]").appName("lifo-inputs-check")
             .config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    frame = lambda rows, schema: spark.createDataFrame(rows, schema)
    ok = True
    for name, transfers, sources, membership, want in CASES:
        found = violations(frame(transfers, "seq long, src long, dst long, amount long"),
                           frame(sources, "wallet long, label int"),
                           frame(membership, "wallet long, cluster long"),
                           args.balance_bins)
        got = {rule for rule, df in found.items() if not df.isEmpty()}
        ok &= got == want
        verdict = "ok" if got == want else "WRONG"
        print(f"{name:34s} {', '.join(sorted(got)) or 'valid':26s} {verdict}")

    rng = random.Random(1)
    moves = frame([(rng.randrange(50), seq, rng.randint(-9, 10)) for seq in range(4000)],
                  "wallet long, seq long, d long")
    rows = lambda bins: sorted(map(tuple, overdrafts(moves, bins).collect()))
    want = rows(None)
    same = [bins for bins in (1, 3, 64, 4000) if rows(bins) == want]
    ok &= len(same) == 4
    print(f"random moves: {len(want):,} rows overdrawn; the same rows in 1, 3, 64, and 4,000 "
          f"time bins: {'yes' if len(same) == 4 else f'only in {same}'}")
    spark.stop()
    print("ALL AS EXPECTED" if ok else "MISMATCH")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
