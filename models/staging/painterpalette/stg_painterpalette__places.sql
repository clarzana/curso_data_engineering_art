with
source_pp as (
    select *
    from
        {{ ref('base_painterpalette__painterpalette') }}
),
source_a5 as (
    select *
    from
        {{ ref('base_painterpalette__art500k_paintings') }}
),
source_mo as (
    select *
    from
        {{ ref('base_painterpalette__movements_origins') }}
),
source_demo as (
    select *
    from
        {{ ref('demonyms') }}
),
collect as (

    select distinct
        split_locations.value as place_name
    from source_pp ppl, lateral split_to_table(input => trim(ppl.locations, ', []'), ',') as split_locations
    union by name
    select distinct
        trim(split_loc_with_years.value, '0123456789: ''"-') as place_name
    from source_pp pplwy, lateral split_to_table(input => trim(pplwy.locations_with_years, ', []'), ',') as split_loc_with_years
    where place_name not like ''
    union by name
    select
        split_ppna.value as place_name
    from source_pp ppna, lateral split_to_table(input => ppna.nationality, ',') as split_ppna
    union by name
    select
        ppci.citizenship as place_name
    from source_pp ppci
    union by name
    select distinct
        replace(trim(a5.location, '"'), '""', '"') as place_name
    from source_a5 a5
    union by name
    select distinct
        mo.origin_country as place_name
    from source_mo mo

), renamed as (
    select
        replace(replace(
            case
                when contains(lower(demo.demonym), lower(c.place_name))
                then demo.Country
                else c.place_name
            end
        , ''''), '"')::varchar(512) as place_name
    from collect c
    full join source_demo demo
    on c.place_name = demo.demonym
    where place_name like '%''%'
    
)

select distinct
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.place_name', 'places') ]) }}::varchar(32) as place_id,
    r.place_name
from renamed r
union
select
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'places') ]) }}::varchar(32) as place_id,
    'Unknown place' as place_name
