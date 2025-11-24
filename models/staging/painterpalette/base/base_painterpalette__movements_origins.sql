with source as (

    select * from {{ source('painterpalette', 'movements_origins') }}

),

renamed as (

    select
        *
    from source

)

select * from renamed