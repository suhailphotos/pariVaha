# src/parivaha/vault.py
"""Dataclass representing a single vault from sync_config.json"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
import os

from notionmanager.backends import NotionSyncBackend, NotionDBConfig

@dataclass
class Vault:
    name: str
    path: Path
    db_type: str
    db_cfg: Dict[str, Any]
    backend: NotionSyncBackend  # currently only Notion

    # NOTE: notionmanager<=0.1.24 lacks a `default_cover` arg, so we attach it
    # post‑instantiation as an ad‑hoc attribute.
    @classmethod
    def from_cfg(cls, raw: Dict[str, Any]):
        name = raw["name"]
        path = Path(raw["path"]).expanduser()
        db   = raw["database"]
        if (db_type := db["type"]) != "notion":
            raise NotImplementedError("Only Notion backend implemented")

        notion_cfg  = db["notion"]
        notion_conf = NotionDBConfig(
            database_id    = notion_cfg["id"],
            forward_mapping = notion_cfg["forward_mapping"],
            back_mapping    = notion_cfg["back_mapping"],
            default_icon    = notion_cfg.get("icon", {}),
        )
        # monkey‑patch until notionmanager adds native support
        setattr(notion_conf, "default_cover", notion_cfg.get("cover", {}))

        api_key = os.getenv("NOTION_API_KEY")
        backend = NotionSyncBackend(api_key, notion_conf)
        return cls(name, path, db_type, db, backend)
