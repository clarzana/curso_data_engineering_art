with
source_ej as (
    select *
    from
        {{ ref('base_painterpalette__exhibitions_journals') }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key([dbt_utils.generate_surrogate_key(['ej.id', 'ej.title']),
        dbt_utils.generate_surrogate_key(['trim(split_artist.value, \'"\')'])]) }}::varchar(32) as artist_exhibition_journal_id,

        {{ dbt_utils.generate_surrogate_key(['ej.id', 'ej.title']) }}::varchar(32) as exhibition_journal_id,
        {{ dbt_utils.generate_surrogate_key(['trim(split_artist.value, \'"\')']) }}::varchar(32) as artist_id,
        contains(ej.title_artists, trim(split_artist.value, '"'))::boolean as in_title
    from source_ej ej, lateral flatten(input => ej.artists) as split_artist
)

select * from renamed
