# ───────────────────────── src/parivaha/seed.py ─────────────────────────
"""
Seed a Notion database from trunk / branch JSON.

    $ parivaha seed                 # uses ~/.parivaha/sample_payload.json
    $ parivaha seed --payload my.json
"""
from __future__ import annotations
import json, os, time
from pathlib import Path
import click
from dotenv import load_dotenv
from parivaha.progress import progress

try:
    from tqdm import tqdm
except ModuleNotFoundError:
    tqdm = None  # type: ignore

from parivaha.config import CONFIG_DIR, ENV_FILE, SYNC_FILE, load_sync_config


# ──────────────────────────────────────────────────────────────────────────
def run(payload_path: Path | None = None) -> None:
    if payload_path is None:
        payload_path = CONFIG_DIR / "sample_payload.json"

    # ─── config & Notion helper ───────────────────────────────────────────
    if ENV_FILE.exists():
        load_dotenv(ENV_FILE, override=False)

    cfg = load_sync_config(SYNC_FILE)
    v   = cfg["vaults"][0]                       # single-vault assumption

    notion_cfg = v["database"]["notion"]
    api_key    = os.getenv("NOTION_API_KEY")
    if not api_key:
        raise click.ClickException("NOTION_API_KEY missing in ~/.parivaha/.env")

    from notionmanager.notion import NotionManager        # late import
    nm      = NotionManager(api_key, notion_cfg["id"])
    backmap = notion_cfg["back_mapping"]
    icon    = notion_cfg.get("icon",  {})
    cover   = notion_cfg.get("cover", {})

    # ─── load trunk / branches JSON ──────────────────────────────────────
    data  = json.loads(payload_path.read_text())
    trunk = data["trunk"]                  # list[dict]
    id_map: dict[str,str] = {}             # title → page_id for quick lookup

    # ● helper ------------------------------------------------------------
    def build_payload(name: str, tags=None):
        flat = {
            "name":   name,
            "tags":   tags or ["#branch"],
            "status": "Not Synced",
            "icon":   icon,
            "cover":  cover,
        }
        return nm.build_notion_payload(flat, backmap)

    # ● resolve an existing page – by stored id, or by title --------------
    def resolve_or_create(node: dict) -> str:
        title = node["name"]
    
        # if JSON carries an id, try to fetch it
        page = None
        pid  = node.get("id")
        if pid:
            try:
                page = nm.get_page(pid)
                # trash? -> pretend it does not exist
                if page.get("archived") or page.get("in_trash"):
                    page = None
            except Exception:
                page = None
    
        # else look up by Name
        if page is None:
            found = nm.get_pages(
                filter={
                    "property": "Name",
                    "title": {"equals": title}
                }
            )
            page = found[0] if found else None
    
        # still nothing? create fresh root-level page
        if page is None:
            payload = build_payload(title, node.get("tags"))
            page    = nm.add_page(payload)
    
        # ensure it’s detached from any parent relation
        if (
            page.get("properties", {})
               .get("Parent item", {})
               .get("relation")
        ):
            nm.update_page(
                page["id"],
                {"properties": {"Parent item": {"relation": []}}}
            )
    
        # store & return
        node["id"]      = page["id"]
        id_map[title]   = page["id"]
        return page["id"]
    # ────────────────────────────────────────────────────────────────────
    # 1. Seed / repair trunk
    # ────────────────────────────────────────────────────────────────────
    t_iter = tqdm(trunk, desc="Trunk", unit="pg") if tqdm else trunk
    for node in t_iter:
        resolve_or_create(node)

    # ────────────────────────────────────────────────────────────────────
    # 2. Seed branches (recursive)
    # ────────────────────────────────────────────────────────────────────
    def count_nodes(d: dict) -> int:
        return sum(count_nodes(v) for v in d.values()) + len(d)

    total = sum(count_nodes(n.get("branches", {})) for n in trunk)
    pbar  = tqdm(total=total, desc="Branches", unit="pg") if tqdm and total else None

    def add_branch(tree: dict, parent_name: str):
        parent_id = id_map[parent_name]
        for child, subtree in tree.items():
            if pbar: pbar.update(1)

            payload = build_payload(child)
            payload.setdefault("properties", {})["Parent item"] = {
                "type": "relation", "relation": [{"id": parent_id}],
            }
            page   = nm.add_page(payload)
            id_map[child] = page["id"]
            add_branch(subtree, child)

    for node in trunk:
        if node.get("branches"):
            add_branch(node["branches"], node["name"])

    if pbar: pbar.close()

    # ─── write new ids back for idempotency ─────────────────────────────
    payload_path.write_text(json.dumps(data, indent=2))
    click.echo("✅  Seed complete.")
