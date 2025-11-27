#!/usr/bin/env python3

"""
Script to sync data from NVA database to Cristin database.
Identifies new publications in NVA that don't exist in Cristin and adds them.
"""

import logging
import pathlib
from datetime import datetime
from typing import Any

import duckdb
import environ

# Setup environment and logging
env = environ.Env()
BASE_DIR = pathlib.Path(__file__).parent
environ.Env.read_env(str(BASE_DIR / ".env"))

DEBUG = env.bool("DEBUG", default=False)
logging.basicConfig(level=(logging.DEBUG if DEBUG else logging.INFO))
logger = logging.getLogger(__name__)

# Database paths
NVA_DB_PATH = BASE_DIR / "nva_sync.duckdb"
CRISTIN_DB_PATH = BASE_DIR / "pbase_duck" / "pbase.duckdb"


def format_datetime_for_sql(date_obj) -> str | None:
    """Convert datetime object to SQL compatible format."""
    if not date_obj:
        return None
    try:
        # If it's already a datetime object, format it
        if hasattr(date_obj, "strftime"):
            return date_obj.strftime("%Y-%m-%d %H:%M:%S")
        # If it's a string, parse it first
        elif isinstance(date_obj, str):
            dt = datetime.fromisoformat(date_obj.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return None
    except (ValueError, AttributeError, TypeError):
        return None


def map_language_code(language_uri: str) -> str | None:
    """Map NVA language URI to Cristin language code."""
    language_map = {
        "http://lexvo.org/id/iso639-3/eng": "EN",
        "http://lexvo.org/id/iso639-3/nor": "NO",
        "http://lexvo.org/id/iso639-3/nob": "NOB",
    }
    return language_map.get(language_uri)


def extract_volume_safely(volume_data: Any) -> str | None:
    """Safely extract volume value from NVA data."""
    if isinstance(volume_data, dict):
        return volume_data.get("start")
    elif isinstance(volume_data, str):
        return volume_data
    return None


def extract_pages(pages_begin: str, pages_end: str) -> str | None:
    """Combine page begin and end into a range."""
    if pages_begin and pages_end:
        return f"{pages_begin}-{pages_end}"
    elif pages_begin:
        return pages_begin
    elif pages_end:
        return pages_end
    return None


def get_new_publications():
    """
    Find publications in NVA database that don't exist in Cristin database.
    Uses title and publication year as matching criteria.
    """

    # Connect to both databases
    nva_conn = duckdb.connect(str(NVA_DB_PATH))
    cristin_conn = duckdb.connect(str(CRISTIN_DB_PATH))

    try:
        # Get existing publications from Cristin (normalized titles and years)
        existing_query = """
        SELECT LOWER(TRIM(Tittel)) as normalized_title, Publiseringsaar
        FROM Cristin 
        WHERE Tittel IS NOT NULL
        """

        existing_pubs = cristin_conn.execute(existing_query).fetchall()
        existing_set = {(title, year) for title, year in existing_pubs}

        logger.info(
            f"Found {len(existing_set)} existing publications in Cristin database"
        )

        # Get all publications from NVA with required fields
        nva_query = """
        SELECT 
            identifier, 
            entity_description__main_title,
            entity_description__publication_date__year,
            created_date,
            modified_date,
            entity_description__reference__type,
            entity_description__reference__publication_context__type,
            entity_description__reference__publication_context__name,
            entity_description__reference__publication_context__name as journal_name,
            entity_description__reference__publication_context__scientific_value,
            entity_description__reference__publication_instance__issue,
            entity_description__reference__publication_instance__volume,
            entity_description__reference__publication_instance__pages__begin,
            entity_description__reference__publication_instance__pages__end,
            entity_description__reference__publication_context__online_issn,
            entity_description__description,
            resource_owner__owner,
            NULL as isbn,
            NULL as publisher_name,
            NULL as reference,
            entity_description__reference__doi,
            publisher__id,
            entity_description__language
        FROM resources
        WHERE entity_description__main_title IS NOT NULL
        """

        nva_pubs = nva_conn.execute(nva_query).fetchall()

        logger.info(f"Found {len(nva_pubs)} publications in NVA database")

        # Find new publications
        new_publications = []
        for pub in nva_pubs:
            title = pub[1]  # entity_description__main_title
            year = pub[2]  # entity_description__publication_date__year

            if title:
                normalized_title = title.lower().strip()
                if (normalized_title, year) not in existing_set:
                    new_publications.append(pub)

        logger.info(f"Found {len(new_publications)} new publications to add")
        return new_publications

    finally:
        nva_conn.close()
        cristin_conn.close()


def insert_new_publications(new_publications):
    """Insert new publications into Cristin database."""

    if not new_publications:
        logger.info("No new publications to insert")
        return

    cristin_conn = duckdb.connect(str(CRISTIN_DB_PATH))

    try:
        # Get the next PubID (max existing + 1)
        max_id_result = cristin_conn.execute(
            "SELECT COALESCE(MAX(PubID), 0) FROM Cristin"
        ).fetchone()
        next_pub_id = (max_id_result[0] or 0) + 1

        inserted_count = 0

        for pub in new_publications:
            (
                identifier,
                main_title,
                pub_year,
                created_date,
                modified_date,
                ref_type,
                context_type,
                context_name,
                journal_name,
                level,
                issue,
                volume,
                pages_begin,
                pages_end,
                issn,
                abstract,
                owner,
                isbn,
                publisher_name,
                reference,
                doi,
                publisher_id,
                language,
            ) = pub

            # Format dates
            date_registered = (
                format_datetime_for_sql(created_date) if created_date else None
            )
            date_modified = (
                format_datetime_for_sql(modified_date) if modified_date else None
            )

            # Process volume safely
            volume_value = extract_volume_safely(volume)

            # Combine pages
            pages = extract_pages(pages_begin, pages_end)

            # Map language
            language_code = map_language_code(language) if language else None

            # Insert into Cristin table
            insert_query = """
            INSERT INTO Cristin (
                PubID, Tittel, Publiseringsaar, DatoRegistrert, DatoEndret,
                Kategori, url, KategoriNavn, Underkategori, Rapportserie,
                Tidsskrift, TidsskriftNiva, hefte, volum, sider, issn,
                ForedragArr, Foredragdato, Authors, Skjul, Featured,
                Tekst, Eier, DateLastModified, isbn, Forlag, BokNiva,
                Referanse, doi, TilPubliste, Utgiver, sprak
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            values = (
                next_pub_id,  # PubID
                main_title,  # Tittel
                int(pub_year) if pub_year else None,  # Publiseringsaar
                date_registered,  # DatoRegistrert
                date_modified,  # DatoEndret
                context_type,  # Kategori
                identifier,  # url (using NVA identifier)
                context_name,  # KategoriNavn
                None,  # Underkategori
                None,  # Rapportserie
                journal_name,  # Tidsskrift
                level,  # TidsskriftNiva
                issue,  # hefte
                volume_value,  # volum
                pages,  # sider
                issn,  # issn
                None,  # ForedragArr
                None,  # Foredragdato
                None,  # Authors
                None,  # Skjul
                None,  # Featured
                abstract,  # Tekst
                owner,  # Eier
                date_modified,  # DateLastModified
                isbn,  # isbn
                publisher_name,  # Forlag
                None,  # BokNiva
                reference,  # Referanse
                doi,  # doi
                None,  # TilPubliste
                publisher_id,  # Utgiver
                language_code,  # sprak
            )

            try:
                cristin_conn.execute(insert_query, values)
                next_pub_id += 1
                inserted_count += 1

                if inserted_count % 100 == 0:
                    logger.info(f"Inserted {inserted_count} publications...")

            except Exception as e:
                logger.error(f"Error inserting publication '{main_title}': {e}")
                continue

        # Commit changes
        cristin_conn.commit()
        logger.info(f"Successfully inserted {inserted_count} new publications")

    except Exception as e:
        logger.error(f"Error during insertion: {e}")
        cristin_conn.rollback()
        raise
    finally:
        cristin_conn.close()


def main():
    """Main function to sync data from NVA to Cristin database."""
    logger.info("Starting NVA to Cristin data sync...")
    logger.info("WIP :-)")

    try:
        # Find new publications
        new_publications = get_new_publications()

        # Insert new publications
        insert_new_publications(new_publications)

        logger.info("Data sync completed successfully!")

    except Exception as e:
        logger.error(f"Data sync failed: {e}")
        raise


if __name__ == "__main__":
    main()
