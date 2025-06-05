# src/parivaha/obsidian_io.py
"""Read / write Obsidian markdown with YAML front‑matter."""
# ─── imports ─────────────────────────────────────────────────────────
from __future__ import annotations
import hashlib, re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import frontmatter

_NOTION_URL_PATTERN = re.compile(r"https://www\.notion\.so/[\w-]+-(?P<id>[0-9a-f]{32})")

# ─── helper to map local->notion names using back-map ───────────────
def pretty_name(local_key: str, back_map: dict[str, dict]) -> str:
    """Return the Notion property name given our local field key."""
    return back_map[local_key]["target"]
# --------------------------------------------------------------------

@dataclass
class MdDoc:
    path: Path
    front: dict
    content: str
    hash: str

    @property
    def notion_id(self):
        url = self.front.get("notion_url") or self.front.get("Notion URL")
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
    def __init__(self, root: Path, back_map: dict[str, dict]):
        self.root = root
        self.back_map = back_map                    # <- store once

    # ----------------------------------------------------------------
    def update_doc(self, doc: MdDoc, *, notion_url: str):
        """
        Overwrite YAML front-matter → Last Synced, Notion URL
        and append a markdown link immediately after the H1.
        """
        fm = dict(doc.front)

        fm[pretty_name("last_synced", self.back_map)] = \
            datetime.now(timezone.utc).isoformat(timespec="seconds")
        fm["Notion URL"] = notion_url

        # If the content does NOT already contain a notion link line,
        # inject one just after the first heading.
        body = doc.content
        if "[Open in Notion]" not in body:
            lines = body.splitlines()
            if lines and lines[0].startswith("#"):
                lines.insert(1, f"[Open in Notion]({notion_url})")
                body = "\n".join(lines)

        post = frontmatter.Post(body, **fm)
        doc.path.write_text(frontmatter.dumps(post), encoding="utf-8")
    # ----------------------------------------------------------------

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
