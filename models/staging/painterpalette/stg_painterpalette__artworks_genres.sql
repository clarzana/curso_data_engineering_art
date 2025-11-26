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

    select
        {{ dbt_utils.generate_surrogate_key([return_null_substitute('recolectados.genre_name', 'genres'), 'recolectados.artwork_name', 'recolectados.image_url']) }}::varchar(32) as artwork_genre_id,
        {{ dbt_utils.generate_surrogate_key([return_null_substitute('recolectados.genre_name', 'genres')]) }}::varchar(32) as genre_id,
        {{ dbt_utils.generate_surrogate_key([ 'recolectados.artwork_name', 'recolectados.image_url']) }}::varchar(32) as artwork_id
    from (
        
        select 
            a5genre.value::varchar(256) as genre_name,
            case
                when contains(a5.painting_name, '##')
                then substr(a5.painting_name, 1, position('##', a5.painting_name)-1)
                else a5.painting_name
            end as artwork_name,
            a5.image_url as image_url
        from source_a5 a5, lateral split_to_table(input => a5.genre, ',') a5genre
        union
        select
            case 
                when wpgenre.value ilike '%no genre%'
                then null
                else wpgenre.value
            end::varchar(256) as genre_name,
            wp.file_name as artwork_name,
            wp.url as image_url
        from source_wp wp, lateral split_to_table(input => wp.genre, ',') wpgenre
    ) as recolectados

)

select * from renamed
