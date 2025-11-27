{{
    config(
        materialized='incremental',
        unique_key = 'artwork_id',
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
        pp.birth_year::integer as birth_year,
        pp.death_year::integer as death_year,
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
        coalesce(p.birth_year, recolectados.birth_year) as birth_year,
        coalesce(p.death_year, recolectados.death_year) as death_year,
        coalesce(p.birth_place, recolectados.birth_place) as birth_place,
        coalesce(p.death_place, recolectados.death_place) as death_place,
        coalesce(p.is_contemporary, recolectados.is_contemporary) as is_contemporary
    from principal p
    full join
    (
        select distinct
            split_artists.value as artist_name,
            gender,
            birth_year,
            death_year,
            birth_place,
            death_place,
            is_contemporary
        from (
            select
                trim(regexp_replace(case
                    when charindex('after', lower(a5.author_name))=1
                    then null
                    when charindex('after', lower(a5.author_name))>1
                    then substr(a5.author_name, 1, charindex('after', lower(a5.author_name)) - 1)
                    else a5.author_name
                end,
                '(\\bpainted by\\b|\\bpossibly\\b|\\bcopy\\b|\\bcase possibly\\b|\\battributed to\\b|\\bartist:\\s*copy\\b|\\bartist:\\s*\\b|\\bcopied by\\b|\\bcopy by\\b)', '', 1, 1, 'i')
                ) as artist_name,
                null as gender,
                null as birth_year,
                null as death_year,
                null as birth_place,
                null as death_place,
                null as is_contemporary
            from source_a5 a5
        ) as a5_unsplit, lateral split_to_table(input => a5_unsplit.artist_name, ' and ') as split_artists
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
            null as birth_year,
            null as death_year,
            null as birth_place,
            null as death_place,
            null as is_contemporary
        from source_wp wp
        union
        select distinct
            split_ej.value::varchar as artist_name,
            null as gender,
            null as birth_year,
            null as death_year,
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
    ifnull(r.birth_year, 9999)::varchar(16) as birth_year,
    ifnull(r.death_year, 9999)::varchar(16) as death_year,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.birth_place', 'places') ]) }}::varchar(32) as birth_place_id,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.death_place', 'places') ]) }}::varchar(32) as death_place_id,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.is_contemporary', 'contemporary_options') ]) }}::varchar(32) as is_contemporary_id,
    to_timestamp_tz('2025-11-25 00:00:00.000 +0100')::timestamp_tz as updated_at
from renamed r
where r.artist_name is not null
union
select
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'artists') ]) }}::varchar(32) as artist_id,
    {{ var('artist_null_message') }}::varchar(512) as artist_name,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'artists') ]) }}::varchar(32) as gender_id,
    9999::varchar(16) as birth_year,
    9999::varchar(16) as death_year,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'places') ]) }}::varchar(32) as birth_place_id,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'places') ]) }}::varchar(32) as death_place_id,
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'contemporary_options') ]) }}::varchar(32) as is_contemporary_id,
    to_timestamp_tz('2025-11-25 00:00:00.000 +0100')::timestamp_tz as updated_at

