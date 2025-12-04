# Data Sync

Provide scripts to download and sync data

- UBW
- NVA

## Setup
Install `uv`: https://docs.astral.sh/uv/getting-started/installation/

### UBW

```bash
uv run main.py --help
```

### NVA

#### 01 Get data from NVA

```bash
uv run nva.py --help
```

You have to add flags to specify what type of data you want to fetch:

- `--resources` fetches publications
- `--projects` fetches projects
- `--persons` fetches people
- `--categories` fetches categories
- `--funding_sources` fetches funding sources.
