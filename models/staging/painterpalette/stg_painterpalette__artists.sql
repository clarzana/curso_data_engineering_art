with 

source_pp as (
    select * from
        {{ ref('base_painterpalette__painterpalette') }}
),
source_a5 as (
    select * from
        {{ ref("base_painterpalette__art500k_paintings") }}
),
source_wp as (
    select * from
        {{ ref("base_painterpalette__wikiart_pieces") }}
),
source_ej as (
    select * from
        {{ ref("base_painterpalette__exhibitions_journals") }}
),
renamed as (

    select 
        -- artist_id,

    from source, lateral flatten(input)

)

select * from renamed