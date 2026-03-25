"""Import shim for configs/saturn-server.py.

This keeps the existing executable filename intact while exposing an importable
module name for validation scripts and internal callers.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path


_SOURCE = Path(__file__).with_name("saturn-server.py")
_SPEC = importlib.util.spec_from_file_location("configs.saturn_server_runtime", _SOURCE)
if _SPEC is None or _SPEC.loader is None:
    raise ImportError(f"Unable to load {_SOURCE}")

_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

for _name in dir(_MODULE):
    if _name.startswith("__") and _name not in {"__all__", "__doc__"}:
        continue
    globals()[_name] = getattr(_MODULE, _name)

__all__ = getattr(_MODULE, "__all__", [name for name in globals() if not name.startswith("_")])
