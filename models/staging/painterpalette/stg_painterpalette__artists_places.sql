with 

source_pp as (
    select * from
        {{ ref('base_painterpalette__painterpalette') }}
),
source_demo as(
    select * from
        {{ ref('demonyms') }}
),
renamed as (

    select 
        with_years.place_name,
        with_years.artist_name,
        with_years.stay_start_year,
        with_years.stay_end_year
    from (

        select
            trim(split_loc_with_years.value, '0123456789: ,''"-') as place_name,
            pplwy.artist as artist_name,
            regexp_substr(split_year_ranges.value, '\\d{4}', 1, 1) as stay_start_year,
            regexp_substr(split_year_ranges.value, '\\d{4}', 1, 2) as stay_end_year
        from source_pp pplwy,
        lateral flatten(input => parse_json(pplwy.locations_with_years)) as split_loc_with_years,
        lateral split_to_table(input => regexp_substr(split_loc_with_years.value::varchar, '(\\d(\\d|-|,)*\\d)'), ',') as split_year_ranges
    ) as with_years
    full join (
        select
            split_locations.value as place_name,
            ppl.artist as artist_name,
            null as stay_start_year,
            null as stay_end_year
        from source_pp ppl, lateral split_to_table(input => trim(ppl.locations, ', []'), ',') as split_locations
        union
        select
            ifnull(demo.Country, split_ppna.value) as place_name,
            ppna.artist as artist_name,
            null as stay_start_year,
            null as stay_end_year
        from source_pp ppna, lateral split_to_table(input => ppna.nationality, ',') as split_ppna
        inner join source_demo demo on contains(demo.demonym, split_ppna.value)
        union
        select
            ppci.citizenship as place_name,
            ppci.artist as artist_name,
            null as stay_start_year,
            null as stay_end_year
        from source_pp ppci
    ) as without_years
    on with_years.artist_name = without_years.artist_name
)
select distinct
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.place_name', 'places'), return_null_substitute('r.artist_name', 'artists'),
    'r.stay_start_year', 'r.stay_end_year' ]) }}::varchar(32) as artist_place_id,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.place_name', 'places') ]) }}::varchar(32) as place_id,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.artist_name', 'artists') ]) }}::varchar(32) as artist_id,
    r.stay_start_year::integer as stay_start_year,
    r.stay_end_year::integer as stay_end_year
from renamed r
