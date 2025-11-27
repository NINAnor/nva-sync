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
uv run main.py --help
```

You have to add flags to specify what type of data you want to fetch:

- `--resources` fetches publications
- `--projects` fetches projects
- `--persons` fetches people
- `--categories` fetches categories
- `--funding_sources` fetches funding sources.

Question: Why am I only getting 1438 rows in my DLT-table?

#### 02 Get data from Pbase (internal database)

1. Set up sling correctly (connect and authenticate to Pbase).
2. Run the script underneath

```bash
sling run -r repliacte_pbase_cristin.yaml 
```

#### 03 Synchronize the data from NVA to Pbase

WIPWIPWIPWIPWIPWIP

The project includes a data synchronization script (`sync_data.py`) that merges publications from the NVA database (`nva_sync.duckdb`) into the existing Cristin database (`pbase_duck/pbase.duckdb`).

1. **Duplicate Detection**: The script compares publications by title and publication year to avoid duplicates
2. **Data Mapping**: NVA API fields are mapped to Cristin database schema using the mapping table above
3. **Incremental IDs**: New publications receive auto-generated PubID values starting from the highest existing ID + 1

```bash
uv run src/datasync/sync_data.py
```

The script will:

- Analyzes both databases to identify new publications in NVA that don't exist in Cristin -- This is a WIP
- Transform NVA data to match Cristin schema -- WIP
- Insert new records with proper field mapping - WIP
- Report the number of publications added - WIP

Last successful sync added **1,483 new publications** from NVA to the Cristin database, bringing the total from 19,627 to 21,110 records.

# Data mappings

The following table shows how data fields are mapped from the NVA API to the Cristin database format.

| Cristin-table | NVA API Source | NVA DLT table |
|------------------|----------------|----------------|
| `PubID` | `None` (auto-generated) | `None` (auto-generated) |
| `Tittel` | `entityDescription.mainTitle` |`entity_description__main_title` |
| `Publiseringsaar` | `entityDescription.publicationDate.year` | `entity_description__publication_date__year` |
| `DatoRegistrert` | `createdDate` | `created_date` |
| `DatoEndret` | `modifiedDate` | `modified_date` |
| `Kategori` | `entityDescription.reference.publicationContext.type` | `entity_description__reference__publication_context__type` |
| `URL` | `id` | `id` have to add the rest of the link |
| `KategoriNavn` |  `None` | `None` |
| `Underkategori` | `entityDescription.reference.publicationInstance.type` |`entity_description__reference__publication_instance__type` (translate this?) |
| `Rapportserie` | `entityDescription.reference.publicationContext.seriesNumber` | `entity_description__reference__publication_context__series_number` |
| `Tidsskrift` | `entityDescription.reference.publicationContext.journal` | `entity_description__reference__publication_context__name` Is this column supposed to be the same as `Utgiver`? |
| `TidsskriftNiva` | `None` | `None` |
| `hefte` | `None` | `None` |
| `volum` | `None` | `None` |
| `sider` | `entityDescription.reference.publicationInstance.pages.(end/begin)` | `entity_description__reference__publication_instance__pages__(begin/end)` |
| `issn` | `entityDescription.reference.publicationContext.series.onlineIssn` | `entity_description__reference__publication_context__series__online_issn` |
| `ForedragArr` | `None` | `None` |
| `Foredragsdato` | `None` | `None` |
| `Authors` | `entityDescription.contributors` (must be mapped) | Ligger i tabell `resources__entity_description__contributors` under kolonne `identity__name` |
| `Skjul` | `None` | `None` |
| `Featured` | `None` | `None` |
| `Timestamp` | `None` | `None` |
| `Tekst` | `entityDescription.abstract` | `entity_description__abstract` |
| `Eier` | `resourceOwner.owner` (must be mapped, not sure how yet) | resource_owner__owner |
| `DateLastModified` | `modifiedDate` | modified_date |
| `isbn` | `entityDescription.reference.publicationContext.isbnList[0]` | `???` Not sure where to find this in DLT? |
| `Forlag` | `None` | `None` |
| `BokNiva` | `None` | `None` |
| `Referanse` | `None` | `None` |
| `doi` | `entityDescription.reference.doi` | `entity_description__reference__doi` |
| `TilPubliste` | `None` | `None` |
| `Utgiver` | `entityDescription.reference.publisher.name` | `entity_description__reference__publication_context__name` |
| `sprak` | `entityDescription.language` (mapp to language codes) | `entity_description__language` |

* If table not specified it's in `resources` table.
** If column is `None` NVA doesn't have the data

### Language Code Mapping

The language field uses the following mapping from NVA API language URIs to match the ones already existing in the Cristin table:

- `http://lexvo.org/id/iso639-3/eng` → `EN`
- `http://lexvo.org/id/iso639-3/nor` → `NO`
- `http://lexvo.org/id/iso639-3/nob` → `NOB`

There are a few others which must be mapped.

### Authors mapping

The API returns a list of contributors with various properties. We want to map this correctly to the current structure of the database. An example from `Pbase` is: `Aas, Ø., Einum, S., Klemetsen, A.& Skurdal, J.`. The structure is, with an example of three authors: `<lastname>, <letter_of_firstname>., <lastname>, <letter_of_firstname>. & <lastname>, <letter_of_firstname>.`