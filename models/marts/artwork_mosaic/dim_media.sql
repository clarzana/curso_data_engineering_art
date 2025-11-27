with
source as(
    select * from
        {{ ref("stg_painterpalette__media") }}
),
renamed as(

    select
        medium_id,
        trim(medium_description, '"')
    from source
)
select * from renamed
