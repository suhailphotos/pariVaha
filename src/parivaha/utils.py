# ─────────────────────────── src/parivaha/utils.py ──────────────────────────
"""Generic helpers (path expansion, hashing, etc.) – keep lightweight."""

from __future__ import annotations

__all__ = ["expand_path", "notion_prop"]


import os
from pathlib import Path

def expand_path(p: str | Path) -> Path:
    """Expand ~ and $VARS inside paths."""
    return Path(os.path.expandvars(str(p))).expanduser()

# ---------------------------------------------------------------------------
def notion_prop(local_key: str, back_map: dict[str, dict]) -> str:
    """
    Return the Notion property NAME for a given local_key
    according to back_mapping.
    """
    try:
        return back_map[local_key]["target"]
    except KeyError as exc:
        raise KeyError(f"'{local_key}' not found in back_mapping") from exc
# ---------------------------------------------------------------------------
