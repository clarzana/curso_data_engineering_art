with
source_a5 as (
    select *
    from
        {{ ref('base_painterpalette__art500k_paintings') }}
),
source_wp as (
    select *
    from
        {{ ref('base_painterpalette__wikiart_pieces') }}
),

renamed as (

    select distinct
        a5genre.value::varchar(256) as genre_name
    from source_a5 a5, lateral split_to_table(input => a5.genre, ',') a5genre
    where a5.genre is not null
    union
    select distinct
        case 
            when wpgenre.value ilike '%no genre%'
            then 'No known genre'
            else wpgenre.value
        end::varchar(256) as genre_name
    from source_wp wp, lateral split_to_table(input => wp.genre, ',') wpgenre
    where wp.genre is not null

)

select distinct
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.genre_name', 'genres') ]) }}::varchar(32) as genre_id,
    r.genre_name:: varchar(256) as genre_name
from renamed r
union
select
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'genres') ]) }}::varchar(32) as genre_id,
    {{ var('genre_null_message') }}::varchar(256) as genre_name

