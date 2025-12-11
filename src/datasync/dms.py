import duckdb
import orjson
import typer
from pygeometa.schemas.iso19139 import ISO19139OutputSchema

from .libs.helpers import get_anytext
from .settings import log

app = typer.Typer()

iso = ISO19139OutputSchema()


def to_iso19139(metadata: dict) -> str:
    loaded = orjson.loads(metadata)
    log.debug(loaded, id=loaded.get("identification").get("identifier"))
    try:
        xml = iso.write(loaded)
        return xml
    except Exception as e:
        log.exception(e, id=loaded.get("identification").get("identifier"))
        return ""


@app.command()
def generate_csw_metadata(
    base_url: str = "https://s3-int-1/dms-test/dms/tables/", lang="en"
):
    # TODO: localize the output based on the language in input

    conn = duckdb.connect()
    conn.sql("INSTALL yaml FROM community; LOAD yaml; Install spatial; load spatial")

    conn.create_function("to_iso19139", to_iso19139)
    conn.create_function("xml_bag", get_anytext)

    datasets = conn.read_parquet(base_url + "datasets_dataset.parquet").filter(
        "json_keys(metadata) <> []"
    )
    log.debug(datasets)
    rasters = conn.read_parquet(base_url + "datasets_rasterresource.parquet")  # noqa: F841
    vectors = conn.read_parquet(base_url + "datasets_tabularresource.parquet")  # noqa: F841
    contributions = conn.read_parquet(
        base_url + "datasets_datasetcontribution.parquet"
    ).select("* replace (split(rtrim(ltrim(roles, '{'), '}'), ',') as roles)")

    resources = conn.sql("""
            from rasters
            select * replace (ST_GEomFromHEXWKB(extent) as extent),
                'raster' as type
            union all by name
            from vectors select * replace (ST_GEomFromHEXWKB(extent) as extent),
                case when extent is null then 'table' else 'vector' end as type
    """)
    log.debug(resources)

    metadata = resources.select(f"""
        id,
        dataset_id,
        {{
        -- TODO: investigate language
        language: '{lang}',
        charset: 'utf8',
        hierarchylevel: case
            when extent is not null then 'dataset' else 'nonGeographicDataset'
        end,
        datestamp: current_localtimestamp()::date,
        -- TODO: this needs some fixes
        dataseturi: uri,
        }}::json as metadata
                    """)

    log.debug(metadata)

    spatial = resources.select("""
        id,
        {
            datatype: case
                when type = 'table' then 'textTable'
                when type = 'raster' then 'grid'
                else 'vector'
            end,
            -- this needs to be fixed in a smarter way
            geomtype: 'point',
        }::json as spatial
        """)

    log.debug(spatial)

    identification = resources.select("""
        id,
        {
            identifier: id,
            -- TODO: these needs to be read from the dataset
            language: metadata->'language',
            -- TODO: these is probably a combination
            title: title,
            fee: 'None',
            status: 'completed',
            -- TODO: these needs to be read from the dataset
            rights: '',
            -- TODO: these needs to be read from the dataset
            abstract: '',
            url: uri,
            dates: {
                creation: created_at,
                revision: last_modified_at
            },
            extents: {
                spatial: case when extent is not null then [
                    {
                        bbox: [
                            ST_XMIN(extent), ST_YMIN(extent),
                            ST_XMAX(extent), ST_YMAX(extent),
                        ],
                        crs: 4326
                    }
                ] else [{
                    -- default to Norway BBOX
                    bbox: [
                        4.99207807783, 58.0788841824, 31.29341841, 80.6571442736
                    ],
                    crs: 4326
                    }]
                end
            },
            license: {
                -- need to join with dataset to read those!
                "name": coalesce(metadata->>'$.rightsList[0].rights', 'All rights reserved'),
                "uri": coalesce(metadata->>'$.rightsList[0].rightsUri', ''),
            },
            keywords: {
                "default": {
                keywords_type: 'theme',
                keywords: [],
                }
            },
        }::json as identification
    """)  # noqa: E501

    log.debug(identification)

    content_info = resources.select("""
        id,
        {
            type: case
                when type = 'raster' then 'coverage'
                else 'feature_catalogue'
            end,
            dimensions: [],
        }::json as content_info
    """)

    log.debug(content_info)

    contacts = (
        contributions.select("""
                dataset_id,
                unnest(roles) as role,
                {
                    organization: 'Norsk institutt for naturforsking',
                    individualname: last_name || ', ' || first_name,
                    -- default to NINA
                    phone: '' ,
                    positionname: '',
                    fax: '',
                    address: '',
                    postalcode: '',
                    country: 'Norway',
                    email: email,
                    url: 'https://www.nina.no',
                    city: 'Trondheim'
                } as contact,
    """)
        .select(
            "dataset_id, role, contact, row_number() over (partition by dataset_id, role order by contact->>'individualname') as role_order"  # noqa: E501
        )
        .aggregate(
            "dataset_id, json_group_object(lower(role[1]) || role[2:] || case when role_order = 1 then '' else ('_' || role_order) end, contact) as contact",  # noqa: E501
            group_expr="dataset_id",
        )
    )

    log.debug(contacts)

    distribution = resources.select("""
        id,
        {
            file: {
                url: uri,
                type: case when type = 'raster' then 'FILE:RASTER'
                        when type = 'vector' then 'FILE:GEO'
                        else 'download'
                end,
                name: title,
                description: description,
                function: 'download'
            }
        }::json as distribution
        """)

    log.debug(distribution)

    res = conn.sql(
        """
            select m.id, {
                mcf: { version: 1.0 },
                metadata: m.metadata,
                identification: i.identification,
                content_info: ci.content_info,
                spatial: sp.spatial,
                distribution: d.distribution,
                contact: c.contact
            }::json as metadata
            from metadata as m
            join identification as i on m.id = i.id
            join content_info as ci on ci.id = m.id
            join spatial as sp on sp.id = m.id
            join distribution as d on d.id = m.id
            join contacts as c on m.dataset_id = c.dataset_id

        """
    )
    log.debug(res)

    iso_xml = res.select("id, to_iso19139(metadata) as xml")

    log.debug(iso_xml)

    log.debug(iso_xml.select("*, xml_bag(xml) as fts_text"))


if __name__ == "__main__":
    app()
