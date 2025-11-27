with
source_st as(
    select * from
        {{ ref("stg_painterpalette__styles") }}
),
source_ar as(
    select * from
        {{ ref("stg_painterpalette__artworks_styles") }}
),
source_pl as(
    select * from
        {{ ref("stg_painterpalette__places") }}
),
renamed as(

    select
        ar.artwork_style_id as artwork_style_id,
        ar.artwork_id as artwork_id,
        st.style_name as style_name,
        pl.place_name as style_origin_country

    from source_st st
    inner join source_ar ar
    on st.style_id=ar.style_id
    inner join source_pl pl
    on lower(st.style_origin_country_id)=lower(pl.place_id)
)
select * from renamed
