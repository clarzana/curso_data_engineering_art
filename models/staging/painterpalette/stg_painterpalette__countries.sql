with 

source as (

    select * from
        {{ ref('base_painterpalette__painterpalette') }},
        {{ ref('demonyms')}}


),

renamed as (

    select distinct
        {{ dbt_utils.generate_surrogate_key(['citizenship'])}}::varchar(32) as country_id,
        citizenship::varchar(256) as country_name
    from source
    union
    select distinct
        {{ dbt_utils.generate_surrogate_key(['country'])}}::varchar(32 as country_id),
        country::varchar(256) as country_name
    from (
        select
            split_nationality.value as value,
            country,
            demonym
        from source, lateral split_to_table(input => nationality, ',') split_nationality
    ) where value ilike demonym
    union
    select
        md5('nocountry')::varchar(32) as country_id,
        'Unknown'::varchar(256) as country_name
)

select distinct * from renamed