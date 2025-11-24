with 

source as (

    select * from
        {{ ref('base_painterpalette__painterpalette') }},
        {{ ref('demonyms')}}


),
source_mo as (
    select * from
        {{ ref('base_painterpalette__movements_origins')}}
),
renamed as (

    select distinct
        {{ dbt_utils.generate_surrogate_key(['citizenship'])}}::varchar(32) as country_id,
        ifnull(citizenship, 'Country unknown')::varchar(256) as country_name
    from source
    union
    select distinct
        {{ dbt_utils.generate_surrogate_key(['country'])}}::varchar(32) as country_id,
        ifnull(country, 'Country unknown')::varchar(256) as country_name
    from (
        select
            split_nationality.value as value,
            country,
            demonym
        from source, lateral split_to_table(input => nationality, ',') split_nationality
    ) where value ilike demonym
    union
    select distinct
        {{ dbt_utils.generate_surrogate_key(['mo.origin_country'])}}::varchar(32) as country_id,
        ifnull(mo.origin_country, 'Country unknown')::varchar(256) as country_name
    from source_mo mo
    union
    select
        {{ dbt_utils.generate_surrogate_key(['null'])}}::varchar(32) as country_id,
        'Country unknown'::varchar(256) as country_name
)

select distinct * from renamed