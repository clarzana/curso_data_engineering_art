with 

source as (

    select * from
        {{ ref('base_painterpalette__painterpalette') }}

),
renamed as (

    select
        {{ dbt_utils.generate_surrogate_key([return_null_substitute('trim(split_occupation.value, \'[] \')', 'occupations')]) }}::varchar(32) as occupation_id,
        trim(split_occupation.value, '[] ') as occupation_name
    from source pp, lateral split_to_table(input => trim(pp.occupations, '[] '), ',') split_occupation
    union
    select
        {{ dbt_utils.generate_surrogate_key([return_null_substitute('null', 'occupations')])}}::varchar(32) as occupation_id,
        'Occupation unknown'::varchar(256) as occupation_name
)

select distinct * from renamed