with
source_a5 as (
    select *
    from
        {{ ref('base_painterpalette__art500k_paintings') }}
),

renamed as (

    select
        media as media_description
    from source_a5 a5

)

select distinct
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('media_description', 'media') ]) }}::varchar(32) as media_id,
    r.media_description::varchar(1024) as media_description
from renamed r
union
select
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'media') ]) }}::varchar(32) as media_id,
    'No known media'::varchar(1024) as media_description
