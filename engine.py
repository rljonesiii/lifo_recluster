"""The engine to use: the compiled lifo_nb.Engine when NumPy and Numba are installed and
the ledger fits its int64 arithmetic, otherwise the pure-Python reference lifo.Engine.
Both have the same interface and results.

    from engine import Engine, Ledger, COMPILED
    eng = Engine(L, clusters)
"""
import lifo
from lifo import Ledger, stolen_of, fresh_trace  # noqa: F401

try:
    import lifo_nb
    COMPILED = True
except ImportError:  # no NumPy/Numba: everything runs on the reference
    lifo_nb = None
    COMPILED = False


def Engine(L, clusters=(), **kw):
    """A compiled engine when possible, else the reference (for example for a ledger whose
    total volume exceeds 2**62, such as large wei amounts)."""
    if COMPILED and lifo_nb.fits(L):
        return lifo_nb.Engine(L, clusters, **kw)
    return lifo.Engine(L, clusters, **kw)
