with
source_mo as (
    select *
    from
        {{ ref('base_painterpalette__movements_origins') }}
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
        {{ dbt_utils.generate_surrogate_key([ return_null_substitute('coalesce(mov_recolectados.movement_name, mo.movement_name)', 'movements')]) }}::varchar(32) as movement_id,
        mov_recolectados.movement_name::varchar(256) as movement_name,
        {{ dbt_utils.generate_surrogate_key(['coalesce(mo.origin_country, mov_recolectados.origin_country)']) }}::varchar(32) as movement_origin_country_id
    from (
        select distinct
            pp.movement as movement_name,
            null as origin_country
        from source_pp pp
        union
        select distinct
            trim(ppam.value, '{}:0123456789,') as movement_name,
            null as origin_country
        from source_pp pp, lateral split_to_table(input => trim(pp.artmovement, '{}:0123456789,'), ',') ppam
        union
        select distinct
            trim(ppa5.value, '{}:0123456789,') as movement_name,
            null as origin_country
        from source_pp pp, lateral split_to_table(input => trim(pp.art500k_movements, '{}:0123456789,'), ',') ppa5
        union
        select distinct
            a5am.value as movement_name,
            null as origin_country
        from source_a5 a5, lateral split_to_table(input => a5.artmovement, ',') a5am
        union
        select distinct
            wp.movement as movement_name,
            null as origin_country
        from source_wp wp
    ) as mov_recolectados
    full join (
        select
            movement_name,
            origin_country
        from source_mo
    ) as mo
    on mov_recolectados.movement_name = mo.movement_name
    union
    select
        {{ dbt_utils.generate_surrogate_key([return_null_substitute('null', 'movements')])}}::varchar(32) as movement_id,
        'No known movement'::varchar(256) as movement_name,
        {{ dbt_utils.generate_surrogate_key([return_null_substitute('null', 'countries')])}}::varchar(32) as movement_origin_country_id

)

select distinct * from renamed
