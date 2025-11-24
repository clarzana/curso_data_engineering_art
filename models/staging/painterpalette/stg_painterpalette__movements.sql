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
        coalesce(mov_recolectados.movement_id, mo.movement_id)::varchar(32) as movement_id,
        mov_recolectados.movement_name::varchar(256) as movement_name,
        {{ dbt_utils.generate_surrogate_key(['coalesce(mo.origin_country, mov_recolectados.origin_country)']) }}::varchar(32) as movement_origin_country_id
    from (
        select distinct
            {{ dbt_utils.generate_surrogate_key(['pp.movement']) }} as movement_id,
            pp.movement as movement_name,
            null as origin_country
        from source_pp pp
        union
        select distinct
            {{ dbt_utils.generate_surrogate_key(['ppam.value']) }} as movement_id,
            trim(ppam.value, '{}:0123456789,') as movement_name,
            null as origin_country
        from source_pp pp, lateral split_to_table(input => trim(pp.artmovement, '{}:0123456789,'), ',') ppam
        union
        select distinct
            {{ dbt_utils.generate_surrogate_key(['ppa5.value']) }} as movement_id,
            trim(ppa5.value, '{}:0123456789,') as movement_name,
            null as origin_country
        from source_pp pp, lateral split_to_table(input => trim(pp.art500k_movements, '{}:0123456789,'), ',') ppa5
        union
        select distinct
            {{ dbt_utils.generate_surrogate_key(['a5am.value']) }} as movement_id,
            a5am.value as movement_name,
            null as origin_country
        from source_a5 a5, lateral split_to_table(input => a5.artmovement, ',') a5am
        union
        select distinct
            {{ dbt_utils.generate_surrogate_key(['wp.movement']) }} as movement_id,
            wp.movement as movement_name,
            null as origin_country
        from source_wp wp
    ) as mov_recolectados
    full join (
        select
            {{ dbt_utils.generate_surrogate_key(['source_mo.movement_name']) }} as movement_id,
            movement_name,
            origin_country
        from source_mo
    ) as mo
    on mov_recolectados.movement_id = mo.movement_id
    union
    select
        {{ dbt_utils.generate_surrogate_key(['null'])}}::varchar(32) as movement_id,
        'No known movement'::varchar(256) as movement_name,
        {{ dbt_utils.generate_surrogate_key(['null'])}}::varchar(32) as movement_origin_country_id

)

select distinct * from renamed
