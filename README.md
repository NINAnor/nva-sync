# NVA Sync

## Data mappings

The following table shows how data fields are mapped from the NVA API to the Cristin database format:

| Cristin DB Field | NVA API Source |
|------------------|----------------|
| `PubID` | `None` (auto-generated) |
| `Tittel` | `entityDescription.mainTitle` |
| `Publiseringsaar` | `entityDescription.publicationDate.year` |
| `DatoRegistrert` | `createdDate` |
| `DatoEndret` | `modifiedDate` |
| `Kategori` | `entityDescription.reference.publicationContext.type` |
| `URL` | `id` |
| `KategoriNavn` | `entityDescription.reference.publicationContext.name` |
| `Underkategori` | `None` |
| `Rapportserie` | `None` |
| `Tidsskrift` | `entityDescription.reference.publicationContext.journal` |
| `TidsskriftNiva` | `None` |
| `hefte` | `None` |
| `volum` | `entityDescription.reference.publicationInstance.volume` (safely extracted) |
| `sider` | `entityDescription.reference.publicationInstance.pages` |
| `issn` | `entityDescription.reference.publicationContext.issn` |
| `ForedragArr` | `None` |
| `Foredragsdato` | `None` |
| `Authors` | `None` |
| `Skjul` | `None` |
| `Featured` | `None` |
| `Timestamp` | `None` |
| `Tekst` | `entityDescription.abstract` |
| `Eier` | `resourceOwner.owner` |
| `DateLastModified` | `modifiedDate` (formatted for SQL) |
| `isbn` | `entityDescription.reference.publicationInstance.isbn` |
| `Forlag` | `entityDescription.reference.publicationContext.publisher` |
| `BokNiva` | `None` |
| `Referanse` | `entityDescription.reference.publicationInstance.reference` |
| `doi` | `entityDescription.reference.publicationInstance.doi` |
| `TilPubliste` | `None` |
| `Utgiver` | `publisher.id` |
| `sprak` | `entityDescription.language` (mapped to language codes) |

### Language Code Mapping

The language field uses the following mapping from NVA API language URIs to Cristin codes:

- `http://lexvo.org/id/iso639-3/eng` → `EN`
- `http://lexvo.org/id/iso639-3/nor` → `NO`
- `http://lexvo.org/id/iso639-3/nob` → `NOB`

## Setup
Install `uv`: https://docs.astral.sh/uv/getting-started/installation/

```bash
git init
uv sync --dev
git add .
git commit -m "Initial commit"
uv run pre-commit install # optional
```


### Run
To execute your software you have two options:

**Option 1: Direct execution**
```bash
uv run main.py
```

**Option 2: Run as installed package**
```bash
uvx --from . nva_sync
```

### Development
Just run `uv run main.py` and you are good to go!

### Update from template
To update your project with the latest changes from the template, run:
```bash
uvx --with copier-template-extensions copier update --trust
```

You can keep your previous answers by using:
```bash
uvx --with copier-template-extensions copier update --trust --defaults
```

### (Optional) pre-commit
pre-commit is a set of tools that help you ensure code quality. It runs every time you make a commit.

First, install pre-commit:
```bash
uv tool install pre-commit
```

Then install pre-commit hooks:
```bash
pre-commit install
```

To run pre-commit on all files:
```bash
pre-commit run --all-files
```

### How to install a package
Run `uv add <package-name>` to install a package. For example:
```bash
uv add requests
```

#### Visual studio code
If you are using visual studio code install the recommended extensions


### Tools installed
- uv
- pre-commit (optional)

#### What is an environment variable? and why should I use them?
Environment variables are variables that are not populated in your code but rather in the environment
that you are running your code. This is extremely useful mainly for two reasons:
- security, you can share your code without sharing your passwords/credentials
- portability, you can avoid using hard-coded values like file-system paths or folder names

you can place your environment variables in a file called `.env`, the `main.py` will read from it. Remember to:
- NEVER commit your `.env`
- Keep a `.env.example` file updated with the variables that the software expects
