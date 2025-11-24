with
source_st as (
    select *
    from
        {{ ref('base_painterpalette__styles_origins') }}
),

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

source_wp as (
    select *
    from
        {{ ref('base_painterpalette__wikiart_pieces') }}
),

renamed as (

    select
        coalesce(sty_recolectados.style_id, st.style_id)::varchar(32) as style_id,
        sty_recolectados.style_name::varchar(256) as style_name,
        {{ dbt_utils.generate_surrogate_key(['coalesce(st.origin_country, sty_recolectados.origin_country)']) }}::varchar(32) as style_origin_country_id
    from (
        select distinct
            {{ dbt_utils.generate_surrogate_key(['ppst.value']) }} as style_id,
            trim(ppst.value, ',') as style_name,
            null as origin_country
        from source_pp pp, lateral split_to_table(input => trim(pp.styles, ','), ',') ppst
        union
        select distinct
            {{ dbt_utils.generate_surrogate_key(['ppstye.value']) }} as style_id,
            trim(ppstye.value, '{}:0123456789,-') as style_name,
            null as origin_country
        from source_pp pp, lateral split_to_table(input => trim(pp.stylesyears, '{}:0123456789,-'), ',') ppstye
        union
        select distinct
            {{ dbt_utils.generate_surrogate_key(['a5st.value']) }} as style_id,
            trim(a5st.value, ',') as style_name,
            null as origin_country
        from source_a5 a5, lateral split_to_table(input => trim(a5.style, ','), ',') a5st
        union
        select distinct
            {{ dbt_utils.generate_surrogate_key(['wp.style']) }} as style_id,
            wp.style as style_name,
            null as origin_country
        from source_wp wp
    ) as sty_recolectados
    full join (
        select
            {{ dbt_utils.generate_surrogate_key(['source_st.style_name']) }} as style_id,
            style_name,
            origin_country
        from source_st
    ) as st
    on sty_recolectados.style_id = st.style_id
    union
    select
        {{ dbt_utils.generate_surrogate_key(['null'])}}::varchar(32) as style_id,
        'No known style'::varchar(256) as style_name,
        {{ dbt_utils.generate_surrogate_key(['null'])}}::varchar(32) as style_origin_country_id

)

select distinct * from renamed
