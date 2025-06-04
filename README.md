# pariVaha

**pariVaha** (परिवाह) is a Python package designed to seamlessly sync, transfer, and manage your knowledge between Notion databases and Obsidian vaults.

---

## What is pariVaha?

pariVaha bridges the gap between your Notion and Obsidian knowledge bases, enabling effortless bidirectional transfer and synchronization of notes, pages, and metadata. Whether you want to migrate your digital mind map, keep your notes in sync, or automate your knowledge workflows, pariVaha has you covered.

---

## Features

- **Bidirectional Sync**: Move notes and databases between Notion and Obsidian with ease.
- **Metadata Preservation**: Retain tags, links, and structure during transfer.
- **Custom Mapping**: Configure how your Notion properties map to Obsidian frontmatter or folders.
- **Conflict Resolution**: Smart handling of updates and changes on both sides.
- **Automation Ready**: Integrate pariVaha into your workflows or CI pipelines.

---

## Installation

```
pip install parivaha
```

---

### Quick Start
```
from parivaha import sync
```

### Sync from Notion to Obsidian
```
sync.notion_to_obsidian(notion_token=“YOUR_TOKEN”, obsidian_path=”/path/to/vault”)
```

### Sync from Obsidian to Notion
```
sync.obsidian_to_notion(obsidian_path=”/path/to/vault”, notion_token=“YOUR_TOKEN”)
```

---

## Usage

See the [Documentation](docs/README.md) for advanced configuration, mapping options, and API details.

---

## Why "pariVaha"?

In Sanskrit, "Parivaha" means "to carry, transport, or convey." This package is your digital vehicle, carrying your knowledge smoothly between Notion and Obsidian.

---

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

## Inspiration

pariVaha is inspired by the vision of a unified, portable second brain—where your knowledge flows freely between platforms, just as ideas flow within your mind.

---
