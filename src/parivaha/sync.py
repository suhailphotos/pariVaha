# ─────────────────────────── src/parivaha/sync.py ───────────────────────────
"""High‑level orchestration between ObsidianIO and backend."""
from __future__ import annotations

from typing import Any, Dict, Optional

from parivaha.vault import Vault
from parivaha.obsidian_io import ObsidianReader, ObsidianWriter

class SyncService:
    def __init__(self, cfg: Dict[str, Any]):
        self.vaults = [Vault.from_cfg(v) for v in cfg.get("vaults", [])]

    def run(self, *, vault_name: Optional[str] = None, direction: str = "bidirectional") -> None:
        for v in self.vaults:
            if vault_name and v.name != vault_name:
                continue

            reader = ObsidianReader(v.path)
            writer = ObsidianWriter(v.path)
            backend = v.backend

            if direction in ("pull", "bidirectional"):
                remote = backend.fetch_existing_entries()
                for page in remote.values():
                    writer.write_remote_page(page)

            if direction in ("push", "bidirectional"):
                local = reader.scan()
                backend.sync_from_local(local)  # provided by NotionSyncBackend
