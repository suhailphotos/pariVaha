# src/parivaha/sync.py
"""Push / pull orchestration – now with a CLI progress bar."""

from __future__ import annotations

from typing import Any, Dict, Optional, List

import time
import click
import frontmatter
import secrets
import hashlib
import json
import re
from datetime import datetime, timezone
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
from parivaha.config import get_sync_log_path

BAR_FORMAT = "{l_bar}{bar}| {n_fmt}/{total_fmt} • {rate_fmt}{postfix}"

def mark_sync_complete(notion_manager, page_id, back_map):
    """
    Mark the given Notion page as 'Sync Complete' using the flexible back_mapping.
    """
    status_prop = notion_prop("status", back_map)
    notion_manager.update_page(page_id, {
        status_prop: {
            "type": "status",
            "status": {"name": "Sync Complete"}
        }
    })

def generate_canvas_id():
    """Generate a random 16-char hex string for canvas node IDs."""
    return secrets.token_hex(8)

def write_canvas_file(canvas_path: Path, title: str):
    canvas_path.parent.mkdir(parents=True, exist_ok=True)
    node_id = generate_canvas_id()
    content = {
        "nodes": [
            {
                "id": node_id,
                "x": -125,
                "y": -30,
                "width": 250,
                "height": 60,
                "type": "text",
                "text": title
            }
        ],
        "edges": []
    }
    canvas_path.write_text(json.dumps(content, indent=1), encoding="utf-8")

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
        sync_log_path = get_sync_log_path(writer.root)
        sync_log_path.parent.mkdir(parents=True, exist_ok=True)
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    
        def _title(p: dict) -> str:
            try:
                return p["properties"]["Name"]["title"][0]["plain_text"]
            except Exception:
                return p.get("id", "<untitled>")
    
        # Load sync log if exists, else start fresh
        if sync_log_path.exists():
            sync_log = json.loads(sync_log_path.read_text())
            last_pull = sync_log.get("last_pull")
            pages_log = sync_log.get("pages", {})
        else:
            sync_log = {}
            last_pull = None
            pages_log = {}
    
        log_diff = {"created": [], "updated": [], "deleted": []}
        nm = backend.notion_manager
    
        # Always get all pages for correct tree relationships
        all_pages = nm.get_pages(retrieve_all=True)
        if not all_pages:
            click.echo("Nothing to pull – remote DB is empty.")
            return
    
        # Build parent-child and id-to-page mapping
        children: dict[str, list[dict]] = {}
        id_to_page: dict[str, dict] = {}
        for pg in all_pages:
            id_to_page[pg["id"]] = pg
            rel = pg.get("properties", {}).get("Parent item", {}).get("relation", [])
            if rel:
                parent_id = rel[0]["id"]
                children.setdefault(parent_id, []).append(pg)
    
        # Find roots
        roots = [pg for pg in all_pages if not pg.get("properties", {}).get("Parent item", {}).get("relation")]
        roots.sort(key=lambda p: p["created_time"])
    
        # Always maintain full trunk_md for all roots (sorted)
        trunk_md: list[Path] = []
        for root in roots:
            title = _title(root)
            path = writer.root / Path(title) / f"{title}.md"
            trunk_md.append(path)
    
        # Incremental logic
        affected_roots = set()
        if last_pull:
            filter_payload = {
                "filter": {
                    "timestamp": "last_edited_time",
                    "last_edited_time": {"after": last_pull}
                }
            }
            changed = nm.get_pages(**filter_payload)
            unseen = [pg for pg in all_pages if pg["id"] not in pages_log]
            merged = {pg["id"]: pg for pg in changed + unseen}.values()
            changed = list(merged)
    
            if not changed:
                click.echo("No changes detected since last pull.")
                return  # Nothing to do
            for pg in changed:
                cur = pg
                while True:
                    rel = cur.get("properties", {}).get("Parent item", {}).get("relation", [])
                    if rel:
                        parent_id = rel[0]["id"]
                        parent = id_to_page.get(parent_id)
                        if parent:
                            cur = parent
                            continue
                    break
                affected_roots.add(cur["id"])
        else:
            affected_roots = set(root["id"] for root in roots)
    
        if not affected_roots:
            click.echo("No affected subtrees to sync.")
            return
    
        # Recursive write: always from the root, building correct path
        def write_tree(node, parent_dir: Path | None):
            title = _title(node)
            rel_dir = (parent_dir / title) if parent_dir else Path(title)
            md_path = rel_dir / f"{title}.md"
            obs_path = md_path.as_posix()
    
            notion_id = node["id"]
            notion_last_edit = node.get("last_edited_time", node.get("created_time"))
            old_entry = pages_log.get(notion_id)
            local_hash = None
    
            update_needed = (
                not old_entry or
                old_entry["notion"].get("last_edited") != notion_last_edit or
                old_entry["obsidian"].get("path") != obs_path
            )
    
            node_tags = ["#root"] if parent_dir is None else ["#branch"]
    
            if update_needed:
                body_lines = [
                    f"[Open in Notion](https://www.notion.so/{notion_id.replace('-','')})"
                ]
                if parent_dir:
                    body_lines += [f"*Parent*: [[{parent_dir.name}/{parent_dir.name}]]"]
                body_lines += ["", "---", ""]
                body = "\n".join(body_lines)
                writer.write_remote_page({
                    "path": obs_path,
                    "url": f"https://www.notion.so/{notion_id.replace('-','')}",
                    "id": notion_id,
                    "tags": node_tags,
                    "content": body,
                })
                log_diff["created" if not old_entry else "updated"].append(obs_path)
                # Canvas and Notion properties (same as before)
                canvas_prop = notion_prop("canvas", writer.back_map)
                canvas_value = (
                    node.get("properties", {})
                        .get(canvas_prop, {})
                        .get("checkbox", False)
                )
                canvas_file = writer.root / rel_dir / f"{title}.canvas"
                if canvas_value and not canvas_file.exists():
                    write_canvas_file(canvas_file, title)
                path_prop = notion_prop("path", writer.back_map)
                sync_prop = notion_prop("last_synced", writer.back_map)
                tags_prop = notion_prop("tags", writer.back_map)
                backend.notion_manager.update_page(node["id"], {
                    path_prop: {
                        "type": "rich_text",
                        "rich_text": [{
                            "type": "text",
                            "text": {"content": obs_path},
                            "annotations": {"code": True, "color": "purple"},
                        }],
                    },
                    sync_prop: {"type": "date", "date": {"start": datetime.now(timezone.utc).date().isoformat()}},
                    tags_prop: {
                        "type": "multi_select",
                        "multi_select": [{"name": t} for t in node_tags],
                    },
                })
                mark_sync_complete(nm, notion_id, writer.back_map)
                # refresh page meta to capture the NEW last_edited_time
                notion_last_edit = nm.get_page(notion_id)["last_edited_time"]
                md_file = writer.root / md_path
                post = frontmatter.load(md_file)
                local_hash = hashlib.md5(post.content.encode()).hexdigest()
                pages_log[notion_id] = {
                    "notion": {
                        "id": notion_id,
                        "last_edited": notion_last_edit,
                        "synced_at": now,
                        "parent_id": node.get("parent", {}).get("id"),
                    },
                    "obsidian": {
                        "path": obs_path,
                        "hash": local_hash,
                    }
                }
            # Recursively write children
            for child in children.get(notion_id, []):
                write_tree(child, rel_dir)
    
        # Only write affected root subtrees, but always update trunk chain for all roots
        todo = [id_to_page[rid] for rid in affected_roots]
        with progress(len(todo), "Syncing subtrees") as bar:
            for root in todo:
                write_tree(root, None)
                bar.update(1)
    
        # For root entries: update siblings (trunk chain) and children (never duplicated)
        id_to_children: Dict[str, List[str]] = {}
        for root in roots:
            id_to_children[_title(root)] = sorted(
                {_title(c) for c in children.get(root["id"], [])},  # set for dedupe
                key=lambda s: s.lower(),
            )
    
        for i, hub in enumerate(trunk_md):
            if not hub.exists():
                continue
            post = frontmatter.load(hub)
            body = post.content
    
            # Siblings: previous and next in sorted trunk_md
            sibling_links = []
            if i > 0:
                prev = trunk_md[i-1].parent.name
                sibling_links.append(f"- [[{prev}/{prev}]]")
            if i < len(trunk_md) - 1:
                nxt  = trunk_md[i+1].parent.name
                sibling_links.append(f"- [[{nxt}/{nxt}]]")
            # Remove duplicates while preserving order
            sibling_links = list(dict.fromkeys(sibling_links))
    
            # Children: all direct child pages, deduped
            child_links = [
                f"- [[{hub.parent.name}/{c}/{c}]]"
                for c in id_to_children.get(hub.parent.name, [])
            ]
            child_links = list(dict.fromkeys(child_links))
    
            # Upsert blocks
            def upsert(block: str, new_lines: list[str], body: str) -> str:
                patt = re.compile(
                    rf"^### {block}\n(?:[^\n]*\n)*?",
                    re.MULTILINE,
                )
                repl = "### " + block + "\n" + "\n".join(new_lines) + "\n"
                return patt.sub(repl, body) if patt.search(body) else body.rstrip() + "\n\n" + repl
    
            body = upsert("Siblings", sibling_links, body)
            body = upsert("Children", child_links, body)
    
            # Add canvas link at end (if not present)
            canvas_path = hub.with_suffix(".canvas")
            if canvas_path.exists():
                canvas_bullet = f"- [[{hub.parent.name}/{hub.stem}.canvas]]"
                if canvas_bullet not in body:
                    body = body.rstrip() + "\n" + canvas_bullet + "\n"
    
            post.content = body
            hub.write_text(frontmatter.dumps(post), encoding="utf-8")
    
        # record pull-completion time *after* all page updates
        sync_log["last_pull"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        sync_log["pages"] = pages_log
        sync_log_path.write_text(json.dumps(sync_log, indent=2))
    
        def print_diff():
            print("\nSync summary:")
            for k, v in log_diff.items():
                if v:
                    print(f"  {k.capitalize()}:")
                    for item in v:
                        print(f"    - {item}")
        print_diff()

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

                # colour-consistent Obsidian Path (purple) -----------------
                path_prop = notion_prop("path", writer.back_map)
                backend.notion_manager.update_page(created["id"], {
                    path_prop: {
                        "type": "rich_text",
                        "rich_text": [{
                            "type": "text",
                            "text": {"content": flat["path"]},
                            "annotations": {"code": True, "color": "purple"},
                        }],
                    }
                })

                writer.update_doc(
                    doc,
                    notion_url=created["url"],
                    notion_id=created["id"],
                )
                path_to_page[str(doc.path)] = created["id"]
                mark_sync_complete(backend.notion_manager, created["id"], writer.back_map)

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
