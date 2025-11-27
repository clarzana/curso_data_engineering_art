{{
    config(
        materialized='incremental',
        unique_key = 'artwork_id',
        on_schema_change='sync_all_columns',
    )
}}
with
source as(
    select * from
        {{ ref("stg_painterpalette__artworks") }}
), renamed as(

    select
        artwork_id,
        artwork_name,
        image_url,
        media,
        date_created,
        updated_at
    from source
)
select * from renamed



{% if is_incremental() %}

  having renamed.updated_at > (select max(renamed.updated_at) from {{ this }})

{% endif %}