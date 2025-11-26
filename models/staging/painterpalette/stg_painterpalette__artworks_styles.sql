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
        {{ dbt_utils.generate_surrogate_key([return_null_substitute('recolectados.style_name', 'styles'), 'recolectados.artwork_name', 'recolectados.image_url']) }}::varchar(32) as artwork_style_id,
        {{ dbt_utils.generate_surrogate_key([return_null_substitute('recolectados.style_name', 'styles')]) }}::varchar(32) as style_id,
        {{ dbt_utils.generate_surrogate_key([ 'recolectados.artwork_name', 'recolectados.image_url']) }}::varchar(32) as artwork_id
    from (
        
        select 
            trim(a5st_colon.value, ',; ') as style_name,
            case
                when contains(a5.painting_name, '##')
                then substr(a5.painting_name, 1, position('##', a5.painting_name)-1)
                else a5.painting_name
            end as artwork_name,
            a5.image_url as image_url
        from source_a5 a5, lateral split_to_table(input => trim(a5.style, ','), ',') a5st_comma, lateral split_to_table(input => trim(a5st_comma.value, '; '), ';') a5st_colon
        union
        select
            wp.style as style_name,
            wp.file_name as artwork_name,
            wp.url as image_url
        from source_wp wp
    ) as recolectados

)

select * from renamed
