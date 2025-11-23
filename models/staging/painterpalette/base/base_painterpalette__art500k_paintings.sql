with source as (

    select * from {{ source('painterpalette', 'art500k_paintings') }}

),

renamed as (

    select
        author_name,
        painting_name,
        image_url,
        genre,
        style,
        nationality,
        paintingschool,
        artmovement,
        field,
        date,
        influencedby,
        media,
        influencedon,
        familyandrelatives,
        pupils,
        location,
        series,
        teachers,
        friendsandcoworkers,
        artinstitution,
    from source

)

select * from renamed