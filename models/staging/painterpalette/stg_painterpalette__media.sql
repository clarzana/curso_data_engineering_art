with
source_a5 as (
    select *
    from
        {{ ref('base_painterpalette__art500k_paintings') }}
),

renamed as (

    select
        media as medium_description
    from source_a5 a5

)

select distinct
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('medium_description', 'media') ]) }}::varchar(32) as medium_id,
    r.medium_description::varchar(1024) as medium_description
from renamed r
union
select
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'media') ]) }}::varchar(32) as medium_id,
    {{ var('medium_null_message') }}::varchar(1024) as medium_description
