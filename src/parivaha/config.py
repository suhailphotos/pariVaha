# ────────────────────────── src/parivaha/config.py ──────────────────────────
"""User‑level config helpers."""
from __future__ import annotations

import json, os, shutil
from pathlib import Path
from typing import Any, Dict

CONFIG_DIR = Path.home() / ".parivaha"
ENV_FILE   = CONFIG_DIR / ".env"
SYNC_FILE  = CONFIG_DIR / "sync_config.json"
PACKAGE_CONFIG = Path(__file__).parent / ".config"

__all__ = ["CONFIG_DIR", "ENV_FILE", "SYNC_FILE", "bootstrap_user_config", "load_sync_config"]


def bootstrap_user_config(overwrite: bool = False) -> None:
    """Copy template .env & sync_config.json on first run."""
    CONFIG_DIR.mkdir(exist_ok=True)
    for name in ("env.example", "sync_config.json"):
        src = PACKAGE_CONFIG / name
        dest = CONFIG_DIR / (".env" if name == "env.example" else name)
        if dest.exists() and not overwrite:
            continue
        shutil.copy2(src, dest)


def load_sync_config(path: Path | str = SYNC_FILE) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    # Expand $ENV vars in vault paths
    for v in data.get("vaults", []):
        raw = v.get("path", "")
        v["path"] = os.path.expandvars(raw)
    return data
