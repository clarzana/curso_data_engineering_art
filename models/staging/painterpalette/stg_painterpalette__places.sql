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
    union
    select distinct
        trim(split_loc_with_years.value, '0123456789: ''"-') as place_namec
        null as pp_demonym
    from source_pp pplwy, lateral split_to_table(input => trim(ppl.locations_with_years, ', []'), ',') as split_loc_with_years
    where place_name not like ''
    union
    select
        nationality as place_name,
        nationality as pp_demonym
    from source_pp ppna, lateral split_to_table(input => ppna.nationality, ',')
    union
    select
        citizenship as place_name
    from source_pp ppci
    select distinct
        replace(trim(location, '"'), '""', '"') as place_name,
        null as pp_demonym
    from source_a5 a5
    union
    select distinct
        mo.origin_country as place_name,
        null as pp_demonym
    from source_mo mo

), renamed as (
    select
        {{ dbt_utils.generate_surrogate_key([ return_null_substitute('c.place_name', 'places') ]) }}::varchar(32) as place_id,
        case
            when c.place_name ilike demo.demonym
            then demo.Country
            else c.place_name
        end::varchar(512) as place_name,
        demo.country as demonym
    from collect c
    full join source_demo demo
    on c.pp_demonym=demo.demonym
)

select distinct * from renamed
