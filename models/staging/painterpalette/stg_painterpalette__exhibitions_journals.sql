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
        source.announcement_date::date as announcement_date
    from source
)

select * from renamed
