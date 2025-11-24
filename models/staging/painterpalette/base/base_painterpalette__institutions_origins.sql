with source as (

    select * from {{ source('painterpalette', 'institutions_origins') }}

),

renamed as (

    select
        *
    from source

)

select * from renamed