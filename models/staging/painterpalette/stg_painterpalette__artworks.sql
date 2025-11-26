{{
    config(
        materialized='incremental',
        unique_key = 'artwork_id',
        on_schema_change='sync_all_columns',
        full_refresh = true
    )
}}

with 
source_a5 as (
    select * from
        {{ ref("base_painterpalette__art500k_paintings") }}
),
source_wp as (
    select * from
        {{ ref("base_painterpalette__wikiart_pieces") }}
),
renamed as (
    select
        case
            when contains(a5.painting_name, '##')
            then substr(a5.painting_name, 1, position('##', a5.painting_name)-1)
            else a5.painting_name
        end as artwork_name,
        a5.image_url as image_url,
        a5.media as media,
        case when regexp_like(a5.date, '[^\\d]*')
            then null
            else case when regexp_like(a5.date, '[^\\d]*\\b\\d+-\\d+-\\d+\\b[^\\d]*', 'i') or regexp_like(a5.date, '[^\\d]*\\b\\d+\\/\\d+\\/\\d+\\b[^\\d]*', 'i')
                then year(try_to_date(regexp_substr(a5.date, '\\d+(-|\\/)\\d+(-|\\/)\\d+')))
                when regexp_like(a5.date, '[^\\d]*\\b\\d+-\\d+\\b[^\\d]*', 'i') or regexp_like(a5.date, '[^\\d]*\\b\\d+\\/\\d+\\b[^\\d]*', 'i')
                then regexp_substr(regexp_substr(a5.date, '\\d+(-|\\/)\\d+'), '\\d+')
                when regexp_like(a5.date, '\\b\\d{1,4}\\b')
                then regexp_substr(a5.date, '\\d{1,4}\\b')
                else null
            end                    
        end::integer as year_created,

        case when regexp_like(a5.date, '[^\\d]*')
            then null
            else case when regexp_like(a5.date, '[^\\d]*\\b\\d+-\\d+-\\d+\\b[^\\d]*', 'i') or regexp_like(a5.date, '[^\\d]*\\b\\d+\\/\\d+\\/\\d+\\b[^\\d]*', 'i')
                then month(try_to_date(regexp_substr(a5.date, '\\d+(-|\\/)\\d+(-|\\/)\\d+')))
                when regexp_like(a5.date, '[^\\d]*\\b\\d+-\\d+\\b[^\\d]*', 'i') or regexp_like(a5.date, '[^\\d]*\\b\\d+\\/\\d+\\b[^\\d]*', 'i')
                then regexp_substr(regexp_substr(a5.date, '\\d+(-|\\/)\\d+'), '\\d+', 1, 2)
                else null
            end                    
        end::integer as month_created,

        case when regexp_like(a5.date, '[^\\d]*')
            then null
            else case when regexp_like(a5.date, '.*\\b\\d+-\\d+-\\d+\\b.*', 'i') or regexp_like(a5.date, '.*\\b\\d+\\/\\d+\\/\\d+\\b.*', 'i')
                then day(try_to_date(regexp_substr(a5.date, '\\d+(-|\\/)\\d+(-|\\/)\\d+')))
                else null
            end                    
        end::integer as day_created
    from source_a5 a5
    union
    select
        wp.file_name as artwork_name,
        wp.url as image_url,
        null as media,
        null as year_created,
        null as month_created,
        null as day_created
    from source_wp wp
)

select distinct
    {{ dbt_utils.generate_surrogate_key([ 'r.artwork_name', 'r.image_url', 'r.media' ]) }}::varchar(32) as artwork_id,
    r.artwork_name as artwork_name,
    r.image_url as image_url,
    r.media as media,
    date_from_parts(
        r.year_created,
        ifnull(r.month_created, 1),
        ifnull(r.day_created, 1)
    ) as date_created,
    to_date('2025-11-25') as updated_at
from renamed r


{% if is_incremental() %}

  where r.updated_at > (select max(updated_at) from {{ this }})

{% endif %}