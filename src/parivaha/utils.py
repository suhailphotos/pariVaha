# ─────────────────────────── src/parivaha/utils.py ──────────────────────────
"""Generic helpers (path expansion, hashing, etc.) – keep lightweight."""
from __future__ import annotations

import os
from pathlib import Path

def expand_path(p: str | Path) -> Path:
    """Expand ~ and $VARS inside paths."""
    return Path(os.path.expandvars(str(p))).expanduser()
