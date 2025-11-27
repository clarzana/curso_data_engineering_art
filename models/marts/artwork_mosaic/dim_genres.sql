with
source_ge as(
    select * from
        {{ ref("stg_painterpalette__genres") }}
),
source_ar as(
    select * from
        {{ ref("stg_painterpalette__artworks_genres") }}
),
renamed as(

    select
        ar.artwork_genre_id
        ge.genre_name,
        ge.artwork_id
    from source_ge ge
    inner join source_ar
    on source_ge.genre_id=source_ar.genre_id
)
select * from renamed
