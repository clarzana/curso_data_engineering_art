{{
    config(
        materialized='incremental',
        unique_key = 'artwork_id',
        on_schema_change='sync_all_columns'
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
        a5.painting_name as artwork_name,
        a5.image_url as image_url,
        a5.media as media,
        a5.date as created_date
    from source_a5 a5
    union
    select
        wp.file_name as artwork_name,
        wp.url as image_url,
        null as media,
        null as created_date
    from source_wp wp
)

select distinct
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.artwork_name', 'artworks') ]) }}::varchar(32) as artwork_id,
    r.artwork_name as artwork_name,
    r.image_url as image_url,
    r.media as media,
    r.created_date as created_date_id
from renamed r


{% if is_incremental() %}

  where created_date > (select max(created_date) from {{ this }})

{% endif %}