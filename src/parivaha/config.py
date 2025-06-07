# ────────────────────────── src/parivaha/config.py ──────────────────────────
"""User‑level config helpers."""
from __future__ import annotations

import json, os, shutil
from pathlib import Path
from typing import Any, Dict

CONFIG_DIR = Path.home() / ".parivaha"
ENV_FILE   = CONFIG_DIR / ".env"
SYNC_FILE  = CONFIG_DIR / "sync_config.json"
SYNC_LOG_FOLDER = ".sync"
SYNC_LOG_FILE = "sync_log.json"
PACKAGE_CONFIG = Path(__file__).parent / ".config"

__all__ = ["CONFIG_DIR", "ENV_FILE", "SYNC_FILE", "bootstrap_user_config", "load_sync_config", SYNC_LOG_FOLDER]


TEMPLATE_FILES = {
    "env.example": ".env",
    "sync_config.json": "sync_config.json",
    "sample_payload.json": "sample_payload.json",
}

def bootstrap_user_config(overwrite: bool = False) -> None:
    CONFIG_DIR.mkdir(exist_ok=True)
    for src_name, dest_name in TEMPLATE_FILES.items():
        src  = PACKAGE_CONFIG / src_name
        dest = CONFIG_DIR     / dest_name
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

def get_sync_log_path(vault_path: Path) -> Path:
    return vault_path / SYNC_LOG_FOLDER / SYNC_LOG_FILE

