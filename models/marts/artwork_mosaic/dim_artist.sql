
with
source_arti as(
    select * from
        {{ ref("stg_painterpalette__artists") }}
),
source_au as(
    select * from
        {{ ref("stg_painterpalette__authorships") }}
),
source_pl as(
    select * from
        {{ ref("stg_painterpalette__places") }}
),
source_apl as(
    select * from
        {{ ref("stg_painterpalette__artists_places") }}
),
source_as as(
    select * from
        {{ ref("stg_painterpalette__art_schools") }}
),
source_asa as(
    select * from
        {{ ref("stg_painterpalette__artists_art_schools") }}
),
source_ej as(
    select * from
        {{ ref("stg_painterpalette__exhibitions_journals") }}
),
source_eja as(
    select * from
        {{ ref("stg_painterpalette__artists_exhibitions_journals") }}
),
source_ge as(
    select * from
        {{ ref("stg_painterpalette__genders") }}
),
source_co as(
    select * from
        {{ ref("stg_painterpalette__contemporary_options") }}
),
renamed as(

    select
        _au.artist_id           as artist_id,
        _arti.artist_name           as artist_name,
        _ge.gender_description      as gender,
        _arti.birth_year            as birth_year,
        _arti.death_year            as death_year,
        _plbp.place_name            as birth_place,
        _pldp.place_name            as death_place,
        _as.art_school_name         as art_school,
        _plas.place_name            as art_school_origin_country,
        _co.contemporary_option     as is_contemporary,
        _plapl.place_name           as place_stayed_at,
        _apl.stay_start_year        as place_stay_start_year,
        _apl.stay_end_year          as place_stay_end_year,
        _ej.exhibition_journal_id   as exhibition_journal_featured_in_id,
        _ej.title                   as exhibition_journal_featured_in_title,
        _ej.announcement_date       as exhibition_journal_featured_in_announcement_date,
        _ej.link                    as exhibition_journal_featured_in_announcement_link,
        _ej.subtitle                as exhibition_journal_featured_in_subtitle,
        _arti.updated_at            as updated_at
        
    from source_arti _arti
    left join source_au _au         on _arti.artist_id=_au.artist_id
    left join source_ge _ge         on _arti.gender_id=_ge.gender_id
    left join source_pl _plbp       on _arti.birth_place_id=_plbp.place_id
    left join source_pl _pldp       on _arti.death_place_id=_pldp.place_id
    left join source_asa _asa       on _arti.artist_id=_asa.artist_id
    left join source_as _as         on _asa.art_school_id=_as.art_school_id
    left join source_pl _plas       on _plas.place_id=_as.art_school_origin_country_id
    left join source_co _co         on _arti.is_contemporary_id=_co.contemporary_option_id
    left join source_eja _eja       on _arti.artist_id=_eja.artist_id
    left join source_ej _ej         on _eja.exhibition_journal_id=_ej.exhibition_journal_id
    left join source_apl _apl       on _arti.artist_id=_apl.artist_id
    left join source_pl _plapl      on _apl.place_id=_plapl.place_id
)
select * from renamed r
where artist_id is not null

