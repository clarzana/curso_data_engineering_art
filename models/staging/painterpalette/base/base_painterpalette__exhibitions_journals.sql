with source as (

    select * from {{ source('painterpalette', 'exhibitions_journals') }}

),

renamed as (

    select
        *
    from source

)

select * from renamed