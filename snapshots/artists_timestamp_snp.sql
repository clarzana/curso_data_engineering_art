{% snapshot users_timestamp_snp %}

{{
    config(
        target_schema='snapshots',
        unique_key='artist_id',
        strategy='timestamp',
        updated_at='updated_at',
        hard_deletes='invalidate'
    )
}}

select
    artist_id,
    artist_name,
    gender_id,
    birth_year,
    death_year,
    birth_place_id,
    death_place_id,
    is_contemporary_id,
    updated_at
from {{ ref('stg_painterpalette__artists') }}

{% endsnapshot %}