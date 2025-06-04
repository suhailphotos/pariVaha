# ─────────────────────────── src/parivaha/cli.py ────────────────────────────
"""CLI entry‑point – *thin* wrapper delegating to internal services."""
from __future__ import annotations

import click
from pathlib import Path
from dotenv import load_dotenv

from parivaha.config import CONFIG_DIR, ENV_FILE, SYNC_FILE, load_sync_config
from parivaha.sync import SyncService

@click.group()
def main() -> None:
    """Parivaha – Obsidian ⇆ Notion synchroniser."""
    pass

# ---------------------------------------------------------------------------
# parivaha init – copy templates on first run
# ---------------------------------------------------------------------------
@main.command("init")
@click.option("--overwrite", is_flag=True, help="Overwrite existing files if present.")
def init_cmd(overwrite: bool) -> None:
    from parivaha.config import bootstrap_user_config

    bootstrap_user_config(overwrite)
    click.echo("✅ Configuration initialised at ~/.parivaha")

# ---------------------------------------------------------------------------
# parivaha sync – run synchronisation
# ---------------------------------------------------------------------------
@main.command("sync")
@click.option("--vault", help="Only sync the named vault in sync_config.json")
@click.option(
    "--direction",
    type=click.Choice(["pull", "push", "bidirectional"], case_sensitive=False),
    default="bidirectional",
    show_default=True,
)
def sync_cmd(vault: str | None, direction: str) -> None:
    if not SYNC_FILE.exists():
        raise click.ClickException("sync_config.json missing – run `parivaha init` first.")

    if ENV_FILE.exists():
        load_dotenv(ENV_FILE, override=False)

    cfg = load_sync_config(SYNC_FILE)
    SyncService(cfg).run(vault_name=vault, direction=direction.lower())

if __name__ == "__main__":
    main()
