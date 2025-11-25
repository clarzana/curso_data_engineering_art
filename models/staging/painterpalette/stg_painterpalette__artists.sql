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
        
    from source_pp pp
    union
    select

    from source_a5 a5
    union
    select

    from source_wp wp
    union
    select

    from source_ej ej

)

select * from renamed