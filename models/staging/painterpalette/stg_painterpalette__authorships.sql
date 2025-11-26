with 

source_a5 as (
    select * from
        {{ ref("base_painterpalette__art500k_paintings") }}
),
source_wp as (
    select * from
        {{ ref("base_painterpalette__wikiart_pieces") }}
),
principal as (
    
    select
        artwork_name,
        split_artists.value as artist_name
    from (
        select 
            case
                when contains(a5.painting_name, '##')
                then substr(a5.painting_name, 1, position('##', a5.painting_name)-1)
                else a5.painting_name
            end as artwork_name,
            trim(regexp_replace(case
                when charindex('after', lower(a5.author_name))=1
                then null
                when charindex('after', lower(a5.author_name))>1
                then substr(a5.author_name, 1, charindex('after', lower(a5.author_name)) - 1)
                else a5.author_name
            end,
            '(\\bpainted by\\b|\\bpossibly\\b|\\bcopy\\b|\\bcase possibly\\b|\\battributed to\\b|\\bartist:\\s*copy\\b|\\bartist:\\s*\\b|\\bcopied by\\b|\\bcopy by\\b)', '', 1, 1, 'i')
            ) as artist_name
        from source_a5 a5
    ) as a5_unsplit, lateral split_to_table(input => a5_unsplit.artist_name, ' and ') as split_artists
    union
    select
        wp.file_name as artwork_name,
        case
            when charindex('after', lower(wp.artist)) < 1
            then null
            when wp.artist ilike '%unknown%' or wp.artist ilike '%not known%' or wp.artist ilike '%not %' or wp.artist ilike '%anony%'
            then null
            else substr(wp.artist, 1, charindex('after', lower(wp.artist)) - 1)
        end as artist_name,
    from source_wp wp
), renamed as (
    select
        p.artist_name as artist_name,
        p.artwork_name as artwork_name
    from principal p
), ids as (
    select
        {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.artist_name', 'artists') ]) }}::varchar(32) as artist_id,
        {{ dbt_utils.generate_surrogate_key([ 'r.artwork_name' ]) }}::varchar(32) as artwork_id
    from renamed r
)
select 
    {{ dbt_utils.generate_surrogate_key([ 'ids.artist_id', 'ids.artwork_id' ]) }}::varchar(32) as authorship_id,
    artist_id,
    artwork_id
from ids
