with 

source_pp as (
    select * from
        {{ ref('base_painterpalette__painterpalette') }}
),
renamed as (

    select
        {{ dbt_utils.generate_surrogate_key([ return_null_substitute('recolectados.art_school_name', "art_schools")])}}::varchar(32) as art_school_id,
        {{ dbt_utils.generate_surrogate_key([ return_null_substitute('recolectados.artist_name', 'artists') ]) }}::varchar(32) as artist_id
    from ( 
        select
            trim(split_school.value, ', ')::varchar(512) as art_school_name,
            pp.artist as artist_name
        from source_pp pp, lateral split_to_table(input => trim(pp.paintingschool, ' ,'), ',') as split_school
    ) as recolectados
)

select distinct * from renamed