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
        date_created
    from source
)
select * from renamed



{% if is_incremental() %}

  where r.updated_at > (select max(updated_at) from {{ this }})

{% endif %}