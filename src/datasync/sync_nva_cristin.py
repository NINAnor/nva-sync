#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import Any

import duckdb

from .settings import CRISTIN_DB_PATH, NVA_DUCKDB_NAME

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def format_datetime_for_sql(date_obj) -> str | None:
    """Convert datetime object to SQL compatible format."""
    if not date_obj:
        return None
    try:
        if hasattr(date_obj, "strftime"):
            return date_obj.strftime("%Y-%m-%d %H:%M:%S")
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
    }  # TODO: add more mappings as needed
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


def get_authors_for_publication(nva_conn, resource_id: str) -> str | None:
    """Extract authors from contributors table for a specific publication."""
    try:
        authors_query = """
        SELECT c.identity__name
        FROM resources__entity_description__contributors c
        WHERE c._dlt_parent_id = ?
        ORDER BY c._dlt_list_idx
        """

        authors_result = nva_conn.execute(authors_query, [resource_id]).fetchall()

        if authors_result:
            # Format authors as "Last, F." style
            formatted_authors = []
            for (author_name,) in authors_result:
                if author_name:
                    name_parts = author_name.strip().split(" ")
                    if len(name_parts) > 1:
                        first_name = name_parts[0]
                        last_name = " ".join(name_parts[1:])
                        formatted_authors.append(f"{last_name}, {first_name[0]}.")
                    else:
                        formatted_authors.append(author_name)

            return "; ".join(formatted_authors) if formatted_authors else None
    except Exception as e:
        logger.debug(f"Error extracting authors for {resource_id}: {e}")
    return None


def get_isbn_for_publication(nva_conn, resource_id: str) -> str | None:
    """Extract first ISBN from ISBN list table for a specific publication."""
    try:
        isbn_query = """
        SELECT value
        FROM resources__entity_description__reference__publication_context__isbn_list
        WHERE _dlt_parent_id = ?
        ORDER BY _dlt_list_idx
        LIMIT 1
        """

        isbn_result = nva_conn.execute(isbn_query, [resource_id]).fetchone()
        return isbn_result[0] if isbn_result else None
    except Exception as e:
        logger.debug(f"Error extracting ISBN for {resource_id}: {e}")
    return None


def create_url_from_id(nva_id: str) -> str:
    """Creating full URL with NVA identifier."""
    base_url = "https://nva.sikt.no/registration/"
    return f"{base_url}{nva_id}"


def get_new_publications():
    """
    Find publications in NVA database that don't exist in Cristin database.
    Uses title and publication year as matching criteria.
    """

    # connect to both databases
    nva_conn = duckdb.connect(str(NVA_DUCKDB_NAME))
    cristin_conn = duckdb.connect(str(CRISTIN_DB_PATH))

    try:
        # get existing publications from Pbase (normalized titles and years)
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

        # get all publications from NVA with required fields
        nva_query = """
        SELECT
            identifier,
            _dlt_id,
            entity_description__main_title,
            entity_description__publication_date__year,
            created_date,
            modified_date,
            entity_description__reference__publication_context__type,
            entity_description__reference__publication_instance__type,
            entity_description__reference__publication_context__series_number,
            entity_description__reference__publication_context__name,
            entity_description__reference__publication_instance__pages__begin,
            entity_description__reference__publication_instance__pages__end,
            entity_description__reference__publication_context__series__online_issn,
            entity_description__abstract,
            resource_owner__owner,
            entity_description__reference__doi
        FROM resources
        WHERE entity_description__main_title IS NOT NULL
        """

        nva_pubs = nva_conn.execute(nva_query).fetchall()

        logger.info(f"Found {len(nva_pubs)} publications in NVA database")

        new_publications = []
        for pub in nva_pubs:
            _ = pub[0]  # identifier (for URL construction)
            dlt_id = pub[1]  # _dlt_id (for joining with contributors)
            title = pub[2]  # entity_description__main_title
            year = pub[3]  # entity_description__publication_date__year

            if title:
                normalized_title = title.lower().strip()
                if (normalized_title, year) not in existing_set:
                    # get authors and ISBN for this publication using _dlt_id
                    authors = get_authors_for_publication(nva_conn, dlt_id)
                    isbn = get_isbn_for_publication(nva_conn, dlt_id)

                    # add the additional data to the publication tuple
                    pub_with_extras = pub + (authors, isbn)
                    new_publications.append(pub_with_extras)

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
        # get next PubID (max existing + 1)
        max_id_result = cristin_conn.execute(
            "SELECT COALESCE(MAX(PubID), 0) FROM Cristin"
        ).fetchone()
        next_pub_id = (max_id_result[0] or 0) + 1

        inserted_count = 0

        for pub in new_publications:
            (
                identifier,  # NVA ID for URL construction
                dlt_id,  # _dlt_id (not used in insert but part of tuple)
                main_title,  # entity_description__main_title
                pub_year,  # entity_description__publication_date__year
                created_date,  # created_date
                modified_date,  # modified_date
                context_type,  # context type
                instance_type,  # instance type
                series_number,  # series number
                context_name,  # context name
                pages_begin,  # pages begin
                pages_end,  # pages end
                issn,  # issn
                abstract,  # abstract
                owner,  # owner
                doi,  # doi
                authors,  # extracted from contributors table
                isbn,  # extracted from ISBN list table
            ) = pub

            # format dates according to README mappings
            date_registered = (
                format_datetime_for_sql(created_date) if created_date else None
            )
            date_modified_formatted = (
                format_datetime_for_sql(modified_date) if modified_date else None
            )

            # combine pages into range format
            pages = extract_pages(pages_begin, pages_end)

            # create full URL from NVA identifier
            full_url = create_url_from_id(identifier)

            # insert into Cristin table with proper field mapping from README
            insert_query = """
            INSERT INTO Cristin (
                PubID, Tittel, Publiseringsaar, DatoRegistrert, DatoEndret,
                Kategori, url, KategoriNavn, Underkategori, Rapportserie,
                Tidsskrift, TidsskriftNiva, hefte, volum, sider, issn,
                ForedragArr, Foredragdato, Authors, Skjul, Featured,
                Tekst, Eier, DateLastModified, isbn, Forlag, BokNiva,
                Referanse, doi, TilPubliste, Utgiver, sprak
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """

            # map values according to README data mapping table
            values = (
                next_pub_id,  # PubID (auto-generated)
                main_title,  # Tittel
                int(pub_year) if pub_year else None,  # Publiseringsaar
                date_registered,  # DatoRegistrert
                date_modified_formatted,  # DatoEndret
                context_type,  # Kategori
                full_url,  # URL (full NVA URL)
                None,  # KategoriNavn
                instance_type,  # Underkategori
                series_number,  # Rapportserie
                context_name,  # Tidsskrift
                None,  # TidsskriftNiva
                None,  # hefte
                None,  # volum
                pages,  # sider
                issn,  # issn
                None,  # ForedragArr
                None,  # Foredragdato
                authors,  # Authors (from contributors table)
                None,  # Skjul
                None,  # Featured
                abstract,  # Tekst
                owner,  # Eier
                date_modified_formatted,  # DateLastModified
                isbn,  # isbn (from ISBN list table)
                None,  # Forlag
                None,  # BokNiva
                None,  # Referanse
                doi,  # doi
                None,  # TilPubliste
                context_name,  # Utgiver (same as Tidsskrift)
                None,  # sprak
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

        # commit changes
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

    try:
        # find new publications
        new_publications = get_new_publications()

        # insert the new publications
        insert_new_publications(new_publications)

        logger.info("Data sync completed successfully!")

    except Exception as e:
        logger.error(f"Data sync failed: {e}")
        raise


if __name__ == "__main__":
    main()
