with
fct_artworks as (
    select * from
        {{ ref('fct_artworks') }}
),
dim_authorships as (
    select * from
        {{ ref('dim_authorships') }}
),
dim_styles as (
    select * from
        {{ ref('dim_styles') }}
),
dim_genres as (
    select * from
        {{ ref('dim_genres') }}
),
dim_media as (
    select * from
        {{ ref('dim_media') }}
),
dim_artists as (
    select * from
        {{ ref('dim_artist') }}
), question as (
    select top 5
        ge.genre_name as genre,
        count(artw.artwork_id) as artworks_count
    from fct_artworks artw
    inner join dim_genres ge on ge.artwork_id=artw.artwork_id
    inner join dim_authorships au on artw.artwork_id=au.artwork_id
    inner join dim_artists art on au.artist_id=art.artist_id
    where art.gender ilike 'female' or art.gender='trans woman'
    group by ge.genre_name
) select * from question