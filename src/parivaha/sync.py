# src/parivaha/sync.py
"""Push / pull orchestration – now with a CLI progress bar."""
from __future__ import annotations

from typing import Any, Dict, Optional, List

import time
import click
from datetime import date, datetime, timezone
from pathlib import Path
try:
    from tqdm import tqdm  # nicer progress bar
except ModuleNotFoundError:  # graceful fallback
    tqdm = None  # type: ignore

from parivaha.vault import Vault
from parivaha.progress import progress
from notionmanager.backends import NotionSyncBackend
from parivaha.utils import notion_prop
from parivaha.obsidian_io import ObsidianReader, ObsidianWriter, MdDoc
import frontmatter

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

            click.echo(f"Vault: {v.name} → {direction}")
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
    def _pull(self, backend: NotionSyncBackend, writer: ObsidianWriter):
        """
        Pull every page from the target Notion DB and mirror it to the vault.
        After the file is written we *patch* “Obsidian Path” and “Last Synced”
        back into Notion.  **No page is ever created here** – we use
        `update_page()` exclusively.  Re-running is idempotent.
        """
        # 1) download the full record-set once
        raw_pages = backend.notion_manager.get_pages(retrieve_all=True)
        if not raw_pages:
            click.echo("Nothing to pull – remote DB is empty.")
            return

        # 2) map page-id → list(children) and find roots
        children: dict[str, list[dict]] = {}
        roots: list[dict] = []
        for pg in raw_pages:
            rel = (
                pg.get("properties", {})
                  .get("Parent item", {})
                  .get("relation", [])
            )
            if rel:
                parent_id = rel[0]["id"]
                children.setdefault(parent_id, []).append(pg)
            else:
                roots.append(pg)

        # ─── helpers ---------------------------------------------------
        def _title(p: dict) -> str:                         # Notion page → str
            return p["properties"]["Name"]["title"][0]["plain_text"]

        # sort root pages purely by Notion's Created timestamp (oldest first)
        roots.sort(key=lambda p: p["created_time"])

        nm = backend.notion_manager

        trunk_md: list[Path] = []  # collect root hub notes for stitching

        # ─── recursive writer with progress in closure ─────────────────
        def make_writer(bar):
            def _write(node: dict, parent_dir: Path | None):
                bar.update(1)  # count *every* page

                title   = _title(node)
                rel_dir = (parent_dir / title) if parent_dir else Path(title)
                md_path  = rel_dir / f"{title}.md"
                obs_path = md_path.as_posix()

                body = f"# {title}\n"
                if parent_dir:
                    body += f"*Parent*: [[{parent_dir.name}/{parent_dir.name}]]\n"

                writer.write_remote_page({
                    "path": obs_path,
                    "url":  f"https://www.notion.so/{node['id'].replace('-','')}",
                    "tags": [],
                    "content": body,
                })

                path_prop = notion_prop("path", writer.back_map)
                sync_prop = notion_prop("last_synced", writer.back_map)
                nm.update_page(node["id"], {
                    path_prop: {
                        "type": "rich_text",
                        "rich_text": [{
                            "type": "text",
                            "text": {"content": obs_path},
                            "annotations": ({"code": True}
                                if writer.back_map["path"].get("code") else {}),
                        }],
                    },
                    sync_prop: {"type": "date", "date": {"start": datetime.now(timezone.utc).date().isoformat()}},
                })

                if parent_dir is None:
                    trunk_md.append(writer.root / md_path)

                for child in children.get(node["id"], []):
                    _write(child, rel_dir)

            return _write

        with progress(len(raw_pages), "Pulling pages") as bar:
            writer_fn = make_writer(bar)
            for r in roots:
                writer_fn(r, None)

        # ─── build map: root-title → list of its immediate children ----
        id_to_children: Dict[str, List[str]] = {}
        for root in roots:
            id_to_children[_title(root)] = sorted(
                [_title(c) for c in children.get(root["id"], [])],
                key=lambda s: s.lower(),
            )

        # ─── stitch the trunk into prev / next chain + branch bullets --
        for i, hub in enumerate(trunk_md):
            post = frontmatter.load(hub)
            title_line = post.content.splitlines()[0]


            links: list[str] = []
            if i > 0:
                prev = trunk_md[i-1].parent.name
                links.append(f"- [[{prev}/{prev}]]")
            if i < len(trunk_md) - 1:
                nxt  = trunk_md[i+1].parent.name
                links.append(f"- [[{nxt}/{nxt}]]")

            # append real side-branch children (created-time order)
            for child in id_to_children.get(hub.parent.name, []):
                links.append(f"- [[{hub.parent.name}/{child}/{child}]]")

            post.content = "\n".join([title_line, "", "## Children"] + links)
            hub.write_text(frontmatter.dumps(post), encoding="utf-8")


    def _push(self, reader: ObsidianReader, backend, writer: ObsidianWriter):
        for folder in reader.root.iterdir():
            if folder.is_dir():
                hub_md = folder / f"{folder.name}.md"
                if not hub_md.exists():
                    hub_md.write_text(f"# {folder.name}\n", encoding="utf-8")

        docs = reader.scan()
        # ignore any markdown stored at the vault root
        docs = {p: d for p, d in docs.items() if d.path.parent != reader.root}

        if not docs:
            click.echo(f"No markdown files found under {reader.root}")
            return
        ordered: List[MdDoc] = sorted(docs.values(), key=lambda d: len(d.path.parts))

        click.echo(f"{len(ordered)} markdown files discovered (root-level files skipped)")

        path_to_page: Dict[str, str] = {}
        # ─── unified progress bar (tqdm or click) ────────────────────────
        with progress(len(ordered), "Pushing pages") as bar:
            for doc in ordered:
                # advance bar every iteration (object is either tqdm or click)
                bar.update(1)

                # already synced? just cache the mapping and continue
                if doc.notion_id:
                    path_to_page[str(doc.path)] = doc.notion_id
                    continue

                parent_page_id = self._find_parent_page_id(doc, path_to_page)

                flat = {
                    "name": doc.title,
                    "path": str(doc.path.relative_to(reader.root)),
                    "tags": doc.front.get("tags"),
                    "status": doc.front.get("status"),
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

        # context manager closes / refreshes the bar automatically – no manual cleanup

    @staticmethod
    def _find_parent_page_id(doc: MdDoc, cache: Dict[str, str]) -> Optional[str]:
        parent_dir = doc.path.parent
        while parent_dir != parent_dir.parent:
            hub_md = parent_dir / f"{parent_dir.name}.md"
            if (id_ := cache.get(str(hub_md))):
                return id_
            parent_dir = parent_dir.parent
        return None
