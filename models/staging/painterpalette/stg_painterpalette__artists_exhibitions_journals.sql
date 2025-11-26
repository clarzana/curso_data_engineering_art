with
source as (
    select *
    from
        {{ ref('base_painterpalette__exhibitions_journals') }}
),
renamed as (

    select

        {{ dbt_utils.generate_surrogate_key(['source.id', 'source.title']) }}::varchar(32) as exhibition_journal_id,
        {{ dbt_utils.generate_surrogate_key(['trim(split_artist.value, \'"\')']) }}::varchar(32) as artist_name
        from painterpalette_dev_bronze_db.bronze_raw.exhibitions_journals ej, lateral flatten(input => ej.artists) as split_artist
    from source
)

select * from renamed
