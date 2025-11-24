with 

source_pp as (
    select * from
        {{ ref('base_painterpalette__painterpalette') }},
),
source_a5 as (
    select * from
        {{ ref('base_painterpalette__art500k_paintings')}}
),
source_io as (
    select * from
        {{ ref('base_painterpalette__institutions_origins')}}
),
renamed as (

    select
        coalesce(sch_recolectados.art_school_id, sch.art_school_id)::varchar(32) as art_school_id,
        sch_recolectados.art_school_name::varchar(256) as art_school_name,
        {{ dbt_utils.generate_surrogate_key(['coalesce(sch.origin_country, sch_recolectados.origin_country)']) }}::varchar(32) as art_school_origin_country_id
    from ( 
        select distinct
            {{ dbt_utils.generate_surrogate_key(['trim(split_school.value, \', \')'])}}::varchar(32) as art_school_id,
            trim(split_school.value, ', ')::varchar(512) as art_school_name,
            null as origin_country
        from source_pp pp, lateral split_to_table(input => trim(pp.paintingschool, ' ,'), ',') as split_school
        union
        select distinct
            {{ dbt_utils.generate_surrogate_key(['trim(split_school.value, \', \')'])}}::varchar(32) as art_school_id,
            trim(split_school.value, ', ')::varchar(512) as art_school_name,
            null as origin_country
        from source_a5 a5, lateral split_to_table(input => trim(a5.paintingschool, ' ,'), ',') as split_school
    ) as sch_recolectados
    full join (
        select distinct
            {{ dbt_utils.generate_surrogate_key(['io.institution_name'])}}::varchar(32) as art_school_id,
            io.institution_name::varchar(512) as art_school_name,
            case
                when io.origin_country ilike 'unknown'
                then null
                else io.origin_country::varchar(32)
            end as origin_country
        from source_io io
    ) as sch
    on sch_recolectados.art_school_id = sch.art_school_id
    union
    select
        {{ dbt_utils.generate_surrogate_key(['null'])}}::varchar(32) as art_school_id,
        'Art school unknown'::varchar(512) as art_school_name,
        {{ dbt_utils.generate_surrogate_key(['null'])}}::varchar(32) as art_school_origin_country_id
)

select distinct * from renamed