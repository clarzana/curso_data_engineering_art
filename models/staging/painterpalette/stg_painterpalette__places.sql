with
source_pp as (
    select *
    from
        {{ ref('base_painterpalette__painterpalette') }}
),
source_demo as (
    select *
    from
        {{ ref('demonyms') }}
),
renamed as (

    select
        split_locations.value as place_name
    from source_pp ppl, lateral split_to_table(input => trim(ppl.locations, ', []'), ',') as split_locations
    union
    select
        case
            when ppbp.birth_place like 'http%//%.%'
            then null
            else ppbp.birth_place
        end as place_name
    from source_pp ppbp
    union
    select
        case
            when ppdp.death_place like 'http%//%.%'
            then null
            else ppdp.death_place
        end as place_name
    from source_pp ppdp
    union
    select
        trim(split_loc_with_years.value, '0123456789: ''"-') as place_name
    from source_pp pplwy, lateral split_to_table(input => trim(pplwy.locations_with_years, ', []'), ',') as split_loc_with_years
    where place_name not like ''
    union
    select
        ifnull(demo.Country, split_ppna.value) as place_name
    from source_pp ppna, lateral split_to_table(input => ppna.nationality, ',') as split_ppna
    inner join source_demo demo on contains(demo.demonym, split_ppna.value)
    union
    select
        ppci.citizenship as place_name
    from source_pp ppci

)

select distinct
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.place_name', 'places') ]) }}::varchar(32) as place_id,
    r.place_name as place_name
from renamed r
union
select
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'places') ]) }}::varchar(32) as place_id,
    'Unknown place' as place_name
