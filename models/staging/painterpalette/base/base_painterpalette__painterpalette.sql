with source as (

    select * from {{ source('painterpalette', 'painterpalette') }}

),

renamed as (

    select
        *
    from source

)

select * from renamed