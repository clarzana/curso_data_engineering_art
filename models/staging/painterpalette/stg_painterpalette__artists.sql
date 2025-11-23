with 

source as (

    select * from
        {{ ref('base_painterpalette__painterpalette') }},
        {{ ref("base_painterpalette__art500k_paintings") }},
        {{ ref("base_painterpalette__wikiart_pieces") }},
        {{ ref("base_painterpalette__exhibitions_journals") }},
        {{ ref("demonyms")}}


),

renamed as (

    select 
        -- artist_id,

    from source, lateral flatten(input)

)

select * from renamed