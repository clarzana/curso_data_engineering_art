{{
    config(
        materialized='incremental',
        unique_key = 'artist_id',
        on_schema_change='sync_all_columns'
    )
}}

with 

source_pp as (
    select * from
        {{ ref('base_painterpalette__painterpalette') }}
),
source_ej as (
    select * from
        {{ ref("base_painterpalette__exhibitions_journals") }}
),
source_a5 as (
    select * from
        {{ ref("base_painterpalette__art500k_paintings") }}
),
source_wp as (
    select * from
        {{ ref("base_painterpalette__wikiart_pieces") }}
),
principal as (

    select 
        pp.artist as artist_name,
        case
            when pp.gender like 'http%//%.%'
            then null
            else pp.gender
        end as gender,
        concat(
                case
                    when pp.birth_year::integer>0
                    then '+'
                    when pp.birth_year::integer<0
                    then '-'
                    else '+'
                end,
                pp.birth_year::integer::varchar, '0000') as birth_year_id,
        concat(
                case
                    when pp.death_year::integer>0
                    then '+'
                    when pp.death_year::integer<0
                    then '-'
                    else '+'
                end,
                pp.death_year::integer::varchar, '0000') as death_year_id,
        case
            when pp.birth_place like 'http%//%.%'
            then null
            else pp.birth_place
        end as birth_place,
        case
            when pp.death_place like 'http%//%.%'
            then null
            else pp.death_place
        end as death_place,
        pp.contemporary as is_contemporary
    from source_pp pp
), renamed as (
    select
        p.artist_name as artist_name,
        coalesce(p.gender, recolectados.gender) as gender,
        coalesce(p.birth_year_id, recolectados.birth_year_id) as birth_year_id,
        coalesce(p.death_year_id, recolectados.death_year_id) as death_year_id,
        coalesce(p.birth_place, recolectados.birth_place) as birth_place,
        coalesce(p.death_place, recolectados.death_place) as death_place,
        coalesce(p.is_contemporary, recolectados.is_contemporary) as is_contemporary
    from principal p
    full join
    (
        select
            case
                when charindex('after', lower(a5.author_name)) < 1
                then null
                else substr(a5.author_name, 1, charindex('after', lower(a5.author_name)) - 1)
            end as artist_name,
            null as gender,
            null as birth_year_id,
            null as death_year_id,
            null as birth_place,
            null as death_place,
            null as is_contemporary
        from source_a5 a5
        union
        select
            case
                when charindex('after', lower(wp.artist)) < 1
                then null
                when wp.artist ilike '%unknown%' or wp.artist ilike '%not known%' or wp.artist ilike '%not %' or wp.artist ilike '%anony%'
                then null
                else substr(wp.artist, 1, charindex('after', lower(wp.artist)) - 1)
            end as artist_name,
            null as gender,
            null as birth_year_id,
            null as death_year_id,
            null as birth_place,
            null as death_place,
            null as is_contemporary
        from source_wp wp
        union
        select distinct
            split_ej.value::varchar as artist_name,
            null as gender,
            null as birth_year_id,
            null as death_year_id,
            null as birth_place,
            null as death_place,
            null as is_contemporary
        from source_ej ej, lateral flatten(input => ej.artists) as split_ej
    ) as recolectados
    on recolectados.artist_name=p.artist_name
)

select distinct
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.artist_name', 'artists') ]) }}::varchar(32) as artist_id,
    r.artist_name::varchar(512) as artist_name,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.gender', 'artists') ]) }}::varchar(32) as gender_id,
    ifnull(r.birth_year_id, '+00000000')::varchar(16) as birth_year_id,
    ifnull(r.death_year_id, '+00000000')::varchar(16) as death_year_id,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.birth_place', 'places') ]) }}::varchar(32) as birth_place_id,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.death_place', 'places') ]) }}::varchar(32) as death_place_id,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.is_contemporary', 'contemporary_options') ]) }}::varchar(32) as is_contemporary_id,
    to_date('20251125')::date as updated_at
from renamed r
union
select
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'artists') ]) }}::varchar(32) as artist_id,
    'Unknown'::varchar(512) as artist_name,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'artists') ]) }}::varchar(32) as gender_id,
    '+00000000'::varchar(16) as birth_year_id,
    '+00000000'::varchar(16) as death_year_id,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'places') ]) }}::varchar(32) as birth_place_id,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'places') ]) }}::varchar(32) as death_place_id,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'contemporary_options') ]) }}::varchar(32) as is_contemporary_id,
    to_date('20251125')::date as updated_at


{% if is_incremental() %}

  where updated_at > (select max(updated_at) from {{ this }})

{% endif %}