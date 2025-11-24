with source as (

    select * from {{ source('painterpalette', 'styles_origins') }}

),

renamed as (

    select
        *
    from source

)

select * from renamed