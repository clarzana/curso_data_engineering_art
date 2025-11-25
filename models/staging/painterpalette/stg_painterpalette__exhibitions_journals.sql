with
source as (
    select *
    from
        {{ ref('base_painterpalette__exhibitions_journals') }}
),
renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['source.id', 'source.title']) }}::varchar(32) as exhibition_journal_id,
        source.title::varchar(512) as title,
        source.subtitle::varchar(1024) as subtitle,
        concat('https://www.e-flux.com', source.eflux_link)::varchar(1024) as link_to_announcement,
        to_varchar(source.announcement_date, 'YYYYMMDD')::varchar(8) as announcement_date_id
    from source
    union
    select
        {{ dbt_utils.generate_surrogate_key([return_null_substitute('null', 'exhibitions_journals')]) }}::varchar(32) as exhibition_journal_id,
        null as title,
        null as subtitle,
        null as link_to_announcement,
        '99999999'::varchar(8) as announcement_date_id
)

select * from renamed
