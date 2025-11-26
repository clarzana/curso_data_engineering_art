with
source as (
    select *
    from
        {{ ref('base_painterpalette__painterpalette') }}
),

renamed as (

    select distinct
        gender as gender_description
    from source 
    where gender not like 'http%//%.%' and gender is not null
)

select 
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('gender_description', 'genders') ]) }}::varchar(32) as gender_id,
    r.gender_description::varchar(512) as gender_description
from renamed r
union
select
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'genders') ]) }}::varchar(32) as gender_id,
    {{ var('gender_null_message') }}::varchar(512) as gender_description
