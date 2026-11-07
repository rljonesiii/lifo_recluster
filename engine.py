"""The engine to use: the compiled lifo_nb.Engine when NumPy and Numba are installed and
the ledger fits its int64 arithmetic, otherwise the pure-Python reference lifo.Engine.
Both have the same interface and results.

    from engine import Engine, Ledger, COMPILED
    eng = Engine(L, clusters)          # an instance of lifo_nb.Engine or lifo.Engine
    isinstance(eng, Engine)            # True for either
"""
from abc import ABC

import lifo
from lifo import Ledger, stolen_of  # noqa: F401

try:
    import lifo_nb
    COMPILED = True
except ImportError:  # no NumPy/Numba: everything runs on the reference
    lifo_nb = None
    COMPILED = False


def _compiled(L):
    return COMPILED and lifo_nb.fits(L)


class Engine(ABC):
    """Engine(L, clusters=(), sweep_after=None, cache_budget=None, cutoff=True) returns a
    compiled engine when possible, else the reference (for example for a ledger whose
    total volume reaches 2**62, such as large wei amounts)."""

    def __new__(cls, L, *args, **kw):
        return (lifo_nb.Engine if _compiled(L) else lifo.Engine)(L, *args, **kw)


Engine.register(lifo.Engine)
if COMPILED:
    Engine.register(lifo_nb.Engine)


def fresh_trace(L, cluster_of, order="preserve"):
    """A fresh trace, compiled when possible; same results either way."""
    return (lifo_nb.fresh_trace if _compiled(L) else lifo.fresh_trace)(L, cluster_of, order)
