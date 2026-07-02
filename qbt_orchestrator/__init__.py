"""Namespace wrapper for running src-layout releases without pip install."""
from __future__ import annotations

from pathlib import Path
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)  # type: ignore[name-defined]
_src_pkg = Path(__file__).resolve().parents[1] / "src" / __name__
if _src_pkg.is_dir():
    _src_s = str(_src_pkg)
    if _src_s not in __path__:
        __path__.append(_src_s)
