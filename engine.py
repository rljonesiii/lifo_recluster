"""The engine to use: the compiled lifo_nb.Engine when Numba is installed, otherwise
the pure-Python reference lifo.Engine.  Both have the same interface and results.

    from engine import Engine, Ledger, COMPILED
"""
from lifo import Ledger, stolen_of  # noqa: F401

try:
    from lifo_nb import Engine, fresh_trace  # noqa: F401
    COMPILED = True
except ImportError:  # no NumPy/Numba: fall back to the reference
    from lifo import Engine, fresh_trace  # noqa: F401
    COMPILED = False
