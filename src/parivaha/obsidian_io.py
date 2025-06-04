# ──────────────────────── src/parivaha/obsidian_io.py ───────────────────────
"""Utilities to read & write Obsidian markdown with YAML front‑matter."""
from __future__ import annotations

import hashlib, datetime as dt
from pathlib import Path
from dataclasses import dataclass
from typing import Dict

import frontmatter

@dataclass
class MdDoc:
    path: Path
    front: dict
    content: str
    hash: str

class ObsidianReader:
    def __init__(self, root: Path):
        self.root = root

    def scan(self) -> Dict[str, MdDoc]:
        docs = {}
        for md in self.root.rglob("*.md"):
            post = frontmatter.load(md)
            body_hash = hashlib.md5(post.content.encode()).hexdigest()
            docs[body_hash] = MdDoc(md, post.metadata, post.content, body_hash)
        return docs

class ObsidianWriter:
    def __init__(self, root: Path):
        self.root = root

    def write_remote_page(self, page: dict):
        """Create or update .md from a transformed Notion page (page contains keys id, path, content …)."""
        target = self.root / page["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        fm = {
            "notion_id": page["id"],
            "last_synced": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            **({"tags": page["tags"]} if page.get("tags") else {}),
        }
        text = frontmatter.dumps(frontmatter.Post(page["content"], **fm))
        target.write_text(text, encoding="utf-8")
