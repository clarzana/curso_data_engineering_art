with source as (

    select * from {{ source('painterpalette', 'wikiart_pieces') }}

),

renamed as (

    select
        *
    from source

)

select * from renamed