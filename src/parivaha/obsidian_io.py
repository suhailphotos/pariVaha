# src/parivaha/obsidian_io.py
"""Read / write Obsidian markdown with YAML front‑matter."""
from __future__ import annotations

import datetime as dt, hashlib, re
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Optional

import frontmatter

_NOTION_URL_PATTERN = re.compile(r"https://www\.notion\.so/[\w-]+-(?P<id>[0-9a-f]{32})")

@dataclass
class MdDoc:
    path: Path
    front: dict
    content: str
    hash: str

    @property
    def notion_id(self) -> Optional[str]:
        url = self.front.get("notion_url")
        if url and (m := _NOTION_URL_PATTERN.search(url)):
            return m.group("id")
        return None

    @property
    def title(self) -> str:
        first = self.content.splitlines()[0]
        return first.lstrip("# ").strip()

class ObsidianReader:
    def __init__(self, root: Path):
        self.root = root

    def scan(self) -> Dict[str, MdDoc]:
        docs = {}
        for md in self.root.rglob("*.md"):
            post = frontmatter.load(md)
            body_hash = hashlib.md5(post.content.encode()).hexdigest()
            docs[str(md)] = MdDoc(md, post.metadata, post.content, body_hash)
        return docs

class ObsidianWriter:
    def __init__(self, root: Path):
        self.root = root

    def update_doc(self, doc: MdDoc, *, notion_url: str):
        post = frontmatter.Post(doc.content, **{**doc.front, "notion_url": notion_url})
        post["last_synced"] = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
        doc.path.write_text(frontmatter.dumps(post), encoding="utf-8")

    def write_remote_page(self, page: dict):
        """Create / update local .md from a transformed Notion page."""
        target = self.root / page["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        fm = {
            "notion_url": page["url"],
            "last_synced": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            **({"tags": page["tags"]} if page.get("tags") else {}),
        }
        text = frontmatter.dumps(frontmatter.Post(page["content"], **fm))
        target.write_text(text, encoding="utf-8")
