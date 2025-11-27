select *
from {{ ref('stg_painterpalette__contemporary_options') }} co
where co.contemporary_option not like 'Yes'
    and co.contemporary_option not like 'No'
    and co.contemporary_option not like {{ var('contemporary_null_message') }}
