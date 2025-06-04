# ────────────────────────── src/parivaha/vault.py ───────────────────────────
"""Dataclass representing a single vault from sync_config.json"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from notionmanager.backends import NotionSyncBackend, NotionDBConfig

@dataclass
class Vault:
    name: str
    path: Path
    db_type: str
    db_cfg: Dict[str, Any]
    backend: NotionSyncBackend  # current only notion backend supported

    @classmethod
    def from_cfg(cls, raw: Dict[str, Any]):
        name = raw["name"]
        path = Path(raw["path"]).expanduser()
        db = raw["database"]
        db_type = db["type"]
        if db_type != "notion":
            raise NotImplementedError("Only Notion backend implemented")

        notion_cfg = db["notion"]
        notion_db_cfg = NotionDBConfig(
            database_id   = notion_cfg["id"],
            forward_mapping= notion_cfg["forward_mapping"],
            back_mapping   = notion_cfg["back_mapping"],
            default_icon   = notion_cfg.get("icon", {}),
            default_cover  = notion_cfg.get("cover", {}),
        )
        import os
        api_key = os.getenv("NOTION_API_KEY")
        backend = NotionSyncBackend(api_key, notion_db_cfg)
        return cls(name=name, path=path, db_type=db_type, db_cfg=db, backend=backend)
