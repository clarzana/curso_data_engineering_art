with
source as (
    select *
    from
        {{ ref('base_painterpalette__painterpalette') }}
),
renamed as (

    select
        s.contemporary::varchar(256) as contemporary_option
    from source s
    where s.contemporary is not null

)

select distinct
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('r.contemporary_option', 'contemporary_options') ]) }}::varchar(32) as contemporary_option_id,
    r.contemporary_option:: varchar(256) as contemporary_option
from renamed r
union
select
    {{ dbt_utils.generate_surrogate_key([ return_null_substitute('null', 'contemporary_options') ]) }}::varchar(32) as contemporary_option_id,
    'Unknown'::varchar(256) as contemporary_option

