# src/parivaha/sync.py
"""Push / pull orchestration – now with a CLI progress bar."""
from __future__ import annotations

from typing import Any, Dict, Optional, List

import time
import click
from datetime import date
try:
    from tqdm import tqdm  # nicer progress bar
except ModuleNotFoundError:  # graceful fallback
    tqdm = None  # type: ignore

from parivaha.vault import Vault
from parivaha.obsidian_io import ObsidianReader, ObsidianWriter, MdDoc

BAR_FORMAT = "{l_bar}{bar}| {n_fmt}/{total_fmt} • {rate_fmt}{postfix}"

class SyncService:
    def __init__(self, cfg: Dict[str, Any]):
        self.vaults = [Vault.from_cfg(v) for v in cfg.get("vaults", [])]

    # ────────────────────────────────────────────────────────────────────
    # Public API
    # ────────────────────────────────────────────────────────────────────
    def run(self, *, vault_name: Optional[str] = None, direction: str = "bidirectional") -> None:
        for v in self.vaults:
            if vault_name and v.name != vault_name:
                continue

            click.echo(f"📁  Vault: {v.name} → {direction}")
            start = time.perf_counter()

            reader  = ObsidianReader(v.path)
            writer  = ObsidianWriter(v.path, back_map=v.backend.notion_db_config.back_mapping)
            backend = v.backend

            if direction in ("pull", "bidirectional"):
                self._pull(backend, writer)

            if direction in ("push", "bidirectional"):
                self._push(reader, backend, writer)

            elapsed = time.perf_counter() - start
            click.echo(f"Completed in {elapsed:0.1f}s")

    # ────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ────────────────────────────────────────────────────────────────────
    def _pull(self, backend, writer):
        remote = backend.fetch_existing_entries()
        for page in remote.values():
            writer.write_remote_page(page)

    def _push(self, reader: ObsidianReader, backend, writer: ObsidianWriter):
        docs = reader.scan()
        # ignore any markdown stored at the vault root
        docs = {p: d for p, d in docs.items() if d.path.parent != reader.root}

        if not docs:
            click.echo(f"No markdown files found under {reader.root}")
            return
        ordered: List[MdDoc] = sorted(docs.values(), key=lambda d: len(d.path.parts))

        click.echo(f"{len(ordered)} markdown files discovered (root-level files skipped)")

        iterator = ordered
        bar_ctx = None
        if tqdm:
            iterator = tqdm(
                ordered,
                desc="Pushing pages",
                unit="pg",
                leave=False,
                colour="green",
                bar_format=BAR_FORMAT                  # ← correct kwarg
            )
        else:
            bar_ctx = click.progressbar(length=len(ordered), label="Pushing pages")
            bar_ctx.__enter__()

        path_to_page: Dict[str, str] = {}

        for idx, doc in enumerate(iterator, 1):
            if not tqdm and bar_ctx:
                bar_ctx.update(1)

            if doc.notion_id:  # already synced – skip for prototype
                path_to_page[str(doc.path)] = doc.notion_id
                continue

            parent_page_id = self._find_parent_page_id(doc, path_to_page)

            flat = {
                "name": doc.title,
                "path": str(doc.path.relative_to(reader.root)),
                "tags": doc.front.get("tags", ["#branch"]),
                "status": "Not Synced",
                "last_synced": date.today().isoformat(),     # mapping turns into “Last Synced”
                "icon": backend.notion_db_config.default_icon,
                "cover": getattr(backend.notion_db_config, "default_cover", {}),
            }
            payload = backend.notion_manager.build_notion_payload(
                flat, backend.notion_db_config.back_mapping
            )

            if parent_page_id:
                payload.setdefault("properties", {})["Parent item"] = {
                    "type": "relation",
                    "relation": [{"id": parent_page_id}],
                }

            created = backend.notion_manager.add_page(payload)
            writer.update_doc(doc, notion_url=created.get("url"))
            path_to_page[str(doc.path)] = created["id"]

        if tqdm:
            iterator.close()  # type: ignore[attr-defined]
        elif bar_ctx:
            bar_ctx.__exit__(None, None, None)

    @staticmethod
    def _find_parent_page_id(doc: MdDoc, cache: Dict[str, str]) -> Optional[str]:
        parent_dir = doc.path.parent
        while parent_dir != parent_dir.parent:
            hub_md = parent_dir / f"{parent_dir.name}.md"
            if (id_ := cache.get(str(hub_md))):
                return id_
            parent_dir = parent_dir.parent
        return None
