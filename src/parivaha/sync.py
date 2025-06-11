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
import shutil
import textwrap
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse as parse_date
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

# ────────────────────────────────────────────────────────────────────
# helpers
# ────────────────────────────────────────────────────────────────────
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

def _purge_empty_dirs(start: Path) -> None:
    """
    Recursively remove empty directories up to—but **not** including—the vault root.
    """
    try:
        while start != start.parent and not any(start.iterdir()):
            start.rmdir()
            start = start.parent
    except OSError:
        # Directory not empty or permission issues – just stop
        pass

def _safe_remove(path: Path):
    try:
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
    except Exception:
        pass  # Already gone or permission denied

def update_inbound_links(vault_root: Path, old_path: str, new_path: str):
    old_ref = old_path.replace('.md', '')
    new_ref = new_path.replace('.md', '')
    for md_file in vault_root.rglob('*.md'):
        text = md_file.read_text(encoding='utf-8')
        new_text = re.sub(
            rf'\[\[{re.escape(old_ref)}\]\]',
            f'[[{new_ref}]]',
            text
        )
        # Also handle any direct .md links, just in case
        new_text = re.sub(
            rf'\[\[{re.escape(old_path)}\]\]',
            f'[[{new_path}]]',
            new_text
        )
        if new_text != text:
            md_file.write_text(new_text, encoding='utf-8')

# ------------------------------ End Helpers ---------------------------------------

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
        Efficient incremental pull:
        - Only pulls changed or new pages since last sync (using last_edited_time).
        - Keeps parent_id in log to track moves.
        - Reconstructs only affected trees.
        - Handles deletions and moves, updates log and local vault.
        """
    
        sync_log_path = get_sync_log_path(writer.root)
        sync_log_path.parent.mkdir(parents=True, exist_ok=True)
    
        # Load sync log, or initialize empty
        if sync_log_path.exists():
            sync_log = json.loads(sync_log_path.read_text())
            last_pull = sync_log.get("last_pull")
            pages_log = sync_log.get("pages", {})
        else:
            sync_log, last_pull, pages_log = {}, None, {}
    
        # keep the previous last_pull in case no real delta is found
        prev_last_pull = last_pull
    
        nm = backend.notion_manager
        back_map = backend.notion_db_config.back_mapping
        log_diff = {"created": [], "updated": [], "deleted": [], "moved": []}
    
        # 1. Fetch only changed/new pages since last pull (or all on first run)
        if last_pull:
            delta_pages = nm.get_pages(
                filter={
                    "timestamp": "last_edited_time",
                    "last_edited_time": {"after": last_pull}
                },
                retrieve_all=True
            )
        else:
            delta_pages = nm.get_pages(retrieve_all=True)
    
        hygiene_due = (
            not last_pull or
            parse_date(last_pull) < datetime.now(timezone.utc) - timedelta(hours=24)
        )

        if not delta_pages and last_pull and not hygiene_due:
            click.echo("No changes detected since last pull.")
            return           # FAST-EXIT, we skip deletion scan & sibling rebuild
    
        # 2. Build lookup map for affected pages (parents pulled on-demand)
        def title(page) -> str:
            try:
                return page["properties"]["Name"]["title"][0]["plain_text"]
            except Exception:
                return page["id"][:8]
    
        page_map: dict[str, dict] = {}
    
        queue = list(delta_pages)
        while queue:
            pg = queue.pop()
            pid = pg["id"]
            if pid in page_map:
                continue
            page_map[pid] = pg
            rel = pg.get("properties", {}).get("Parent item", {}).get("relation", [])
            if rel:
                parent_id = rel[0]["id"]
                if parent_id not in page_map:
                    parent_pg = nm.get_page(parent_id)
                    if parent_pg:
                        queue.append(parent_pg)
    
        writer_root = writer.root
    
        # 3. Create/update markdown files and detect moves
        def write_page(pg):
            pid = pg["id"]
            title_str = title(pg)
            # --- capture TRUE direct parent ----------------------------------
            first_rel = pg.get("properties", {}).get("Parent item", {}).get("relation", [])
            direct_parent_id: Optional[str] = first_rel[0]["id"] if first_rel else None

            # Build Obsidian path by walking up to the root -------------------
            parts: list[str] = [title_str]
            cursor = pg
            while True:
                rel = cursor.get("properties", {}).get("Parent item", {}).get("relation", [])
                if not rel:
                    break                         # reached a real root
                parent_id_tmp = rel[0]["id"]
                cursor = page_map.get(parent_id_tmp) or nm.get_page(parent_id_tmp)
                parts.insert(0, title(cursor))
            # OLD behaviour: **every** page lives in a folder named after itself.
            # So:
            #   root        →  data/data.md
            #   child       →  data/dataProcessing/dataProcessing.md
            #   grandchild  →  ml/neuralNetworks/cnn/cnn.md
            rel_dir = Path(*parts)                # parts already ends with title
            md_path = rel_dir / f"{title_str}.md"
            abs_md = writer_root / md_path
    
            # Detect move / rename -------------------------------------------
            old_meta = pages_log.get(pid)
            old_parent_id = old_meta["parent_id"] if old_meta else None
            old_path = old_meta["obsidian"]["path"] if old_meta else None


            if old_meta and (old_path != md_path.as_posix() or
                            old_parent_id != direct_parent_id):
               old_abs = writer_root / old_path
               try:
                   # ───── decide whether we are moving a WHOLE FOLDER ──────
                   is_root_rename   = (old_parent_id is None and direct_parent_id is None)
                   has_children     = old_abs.parent.exists() and any(old_abs.parent.iterdir())
                   folder_move_need = has_children  # rename OR parent-move
           
                   if folder_move_need:
                       old_dir = old_abs.parent
                       new_dir = abs_md.parent
           
                       if old_dir.resolve() != new_dir.resolve():
                           # ensure target’s *parent* exists but NOT the folder itself
                           new_dir.parent.mkdir(parents=True, exist_ok=True)
                           if new_dir.exists():
                               shutil.rmtree(new_dir)
                           shutil.move(str(old_dir), str(new_dir))
           
                           old_prefix = old_dir.relative_to(writer_root).as_posix()
                           new_prefix = new_dir.relative_to(writer_root).as_posix()

                           # PATCH: Remove any old .md file in the new_dir that has the old folder's name
                           old_md_in_new = new_dir / f"{old_dir.name}.md"
                           if old_md_in_new.exists() and old_md_in_new != abs_md:
                               _safe_remove(old_md_in_new)
           
                           # patch every path in pages_log
                           for meta in pages_log.values():
                               p = meta["obsidian"]["path"]
                               if p.startswith(old_prefix + "/"):
                                   meta["obsidian"]["path"] = p.replace(old_prefix,
                                                                        new_prefix, 1)
                           log_diff["moved"].append(f"{old_prefix}/ → {new_prefix}/")
                           # -- PATCH: Remove the old directory if any remains
                           if old_dir.exists():
                               _safe_remove(old_dir)
                   else:
                       # simple single-file move
                       abs_md.parent.mkdir(parents=True, exist_ok=True)
                       shutil.move(str(old_abs), str(abs_md))
                       log_diff["moved"].append(f"{old_path} → {md_path}")
                       # -- PATCH: Remove the old file if any remains (should not, but extra safety)
                       if old_abs.exists() and old_abs != abs_md:
                           _safe_remove(old_abs)
                       # -- PATCH: Remove old parent dir if empty
                       _purge_empty_dirs(old_abs.parent)
               except Exception as e:
                   print(f"Move failed: {e}")
           
               update_inbound_links(writer_root, old_path, md_path.as_posix())

            # Write markdown if needed (never rewrite if unchanged)
            need_write = (
                not abs_md.exists()
                or pg["last_edited_time"] != (old_meta or {}).get("last_edited")
                or old_path != md_path.as_posix()          # path / move change
            )
            if need_write:
                # — compute checkbox early for inline Canvas line —
                canvas_checked = (
                    pg.get("properties", {})
                      .get(notion_prop("canvas", back_map), {})
                      .get("checkbox", False)
                )
                abs_md.parent.mkdir(parents=True, exist_ok=True)
                is_root = (direct_parent_id is None)
                # ─── new aesthetic body ─────────────────────────────
                body = []
                # extra separator so you get two '---' in a row
                body.append("---")
    
                if not is_root:
                    parent_md_rel = rel_dir.parent / rel_dir.parent.name
                    body.append(f"*Parent*: [[{parent_md_rel.as_posix()}]]")
    
                # inline canvas link if the checkbox is set
                if canvas_checked:
                    body.append(f"*Canvas:* [[{rel_dir.name}/{title_str}.canvas]]")
    
                # blank line + separator before the Notion link
                body.extend(["", "---"])
    
                # show Open in Notion as a bullet
                body.append(f"- [Open in Notion](https://www.notion.so/{pid.replace('-','')})")
    
                # trailing separator
                body.append("---")
                writer.write_remote_page({
                    "id": pid,
                    "url": f"https://www.notion.so/{pid.replace('-','')}",
                    "path": md_path.as_posix(),
                    "tags": ["#root"] if is_root else ["#branch"],
                    "content": "\n".join(body)
                })
                # ── update Notion bookkeeping fields -------------------------
                path_prop  = notion_prop("path",  back_map)
                sync_prop  = notion_prop("last_synced", back_map)
                tags_prop  = notion_prop("tags", back_map)
                # Only hit the API if path OR tag set changed
                if old_path != md_path.as_posix() or is_root != (old_parent_id is None):
                    backend.notion_manager.update_page(pid, {
                        path_prop: {
                            "type": "rich_text",
                            "rich_text": [{
                                "type": "text",
                                "text": {"content": md_path.as_posix()},
                                "annotations": {"code": True, "color": "purple"},
                            }],
                        },
                        sync_prop: {"type": "date",
                                    "date": {"start": datetime.now(timezone.utc)
                                                         .date().isoformat()}},
                        tags_prop: {
                            "type": "multi_select",
                            "multi_select": [{"name": "#root" if is_root else "#branch"}],
                        },
                    })
                    mark_sync_complete(nm, pid, back_map)
                # refresh metadata to capture the NEW last_edited_time  ← ★
                pg = nm.get_page(pid)

            # Update / record log entry (inside or outside need_write) ---------
            pages_log[pid] = {
                "parent_id": direct_parent_id,
                "last_edited": pg["last_edited_time"],
                "obsidian": {
                    "path": md_path.as_posix(),
                    "hash": hashlib.md5((writer_root / md_path).read_bytes()).hexdigest()
                }
            }
    
            # Canvas
            canvas_checked = (
                pg.get("properties", {})
                  .get(notion_prop("canvas", back_map), {})
                  .get("checkbox", False)
            )
            canvas_file = writer_root / rel_dir / f"{title_str}.canvas"
            if canvas_checked and not canvas_file.exists():
                write_canvas_file(canvas_file, title_str)
        
            # ─── remove on un-check ────────────────────────────────────────
            if not canvas_checked and canvas_file.exists():
                # delete the .canvas file
                canvas_file.unlink()
                # strip out the link bullet from the .md
                post = frontmatter.load(abs_md)
                # remove any "- [[folder/file.canvas]]" lines
                post.content = re.sub(
                    rf"^- \[\[{re.escape(rel_dir.name)}/{re.escape(title_str)}\.canvas\]\]\n?",
                    "",
                    post.content,
                    flags=re.MULTILINE
                )
                abs_md.write_text(frontmatter.dumps(post), encoding="utf-8")
    
        # 4. Write / move pages  ── roots first, leaves last
        def _depth(p):
            d = 0
            cur = p
            while cur.get("properties", {}).get("Parent item", {}).get("relation"):
                pid = cur["properties"]["Parent item"]["relation"][0]["id"]
                cur = page_map.get(pid) or {}
                d += 1
            return d

        # show progress as we write each affected page
        total = len(page_map)
        with progress(total, "Syncing pages") as bar:
            for pg in sorted(page_map.values(), key=_depth):
                write_page(pg)
                bar.update(1)
    
        # 5. Handle deletions (clean up files & log, leave .canvas intact) -----
        # run this scan only if we processed deltas **or** hygiene is due
        if delta_pages or hygiene_due:
            alive_ids = {
                p["id"] for p in nm.get_pages(retrieve_all=True) if not p.get("archived")
            }
            for pid, meta in list(pages_log.items()):
                if pid not in alive_ids:
                    md_abs = writer_root / meta["obsidian"]["path"]
                    if md_abs.exists():
                        md_abs.unlink()
                        _purge_empty_dirs(md_abs.parent)
                    pages_log.pop(pid, None)
                    log_diff["deleted"].append(meta["obsidian"]["path"])

        # 6. Rebuild root navigation (*Siblings:* chain) -----------------------
        def _write_sibling_block(root_ids_sorted: list[str]) -> None:
            for i, rid in enumerate(root_ids_sorted):
                md_rel = Path(pages_log[rid]["obsidian"]["path"])
                md_abs = writer_root / md_rel
                if not md_abs.exists():
                    continue
                post = frontmatter.load(md_abs)
                # clean old bullets / block
                post.content = re.sub(
                    r"^[ \t]*-\s+\[\[.*?\/.*?\]\]\s*\n?", "",
                    post.content, flags=re.MULTILINE
                )
                post.content = re.sub(
                    r"\*Siblings:\*[\s\S]*?(?:\n{2,}|$)", "",
                    post.content, flags=re.MULTILINE
                )
                post.content = re.sub(r"\n{3,}", "\n\n", post.content)
                # write single sibling if not tail
                if i < len(root_ids_sorted) - 1:
                    sib_id = root_ids_sorted[i + 1]
                    sib_title = title(page_map.get(sib_id) or nm.get_page(sib_id))
                    post.content = post.content.rstrip() + \
                        f"\n\n*Siblings:*\n- [[{sib_title}/{sib_title}]]\n"
                md_abs.write_text(frontmatter.dumps(post), encoding="utf-8")

        root_ids = [pid for pid, meta in pages_log.items()
                    if meta.get("parent_id") is None]
        root_ids_sorted = sorted(
            root_ids,
            key=lambda rid: title(page_map.get(rid) or nm.get_page(rid)).lower()
        )
        _write_sibling_block(root_ids_sorted)
        # 7. Save log and summary  (timestamp AFTER all update_page() calls) ---
        if delta_pages:
            latest_edited = max(parse_date(pg["last_edited_time"]) for pg in delta_pages)
            # subtract 1 second to ensure no overlap is lost
            new_last_pull = (latest_edited - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
            sync_log["last_pull"] = new_last_pull
        else:
            # no delta → keep previous last_pull (avoids micro “blind window”)
            sync_log["last_pull"] = prev_last_pull or datetime.now(timezone.utc).isoformat(timespec="seconds")
        sync_log["pages"] = pages_log
        sync_log_path.write_text(json.dumps(sync_log, indent=2))
    
        click.echo("Sync summary:")
        for k, items in log_diff.items():
            if items:
                click.echo(f"  {k.capitalize()} – {len(items)}")


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
