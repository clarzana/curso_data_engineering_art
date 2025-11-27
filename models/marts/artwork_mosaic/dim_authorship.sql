with
source_arti as(
    select * from
        {{ ref("stg_painterpalette__artists") }}
),
source_au as(
    select * from
        {{ ref("stg_painterpalette__authorships") }}
),
renamed as(

    select
        _au.authorship_id           as authorship_id,
        _au.artwork_id              as artwork_id,
        _au.artist_id               as artist_id
        
    from source_arti _arti
    left join source_au _au         on _arti.artist_id=_au.artist_id
)
select * from renamed
