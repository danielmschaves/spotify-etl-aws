

{{ config(materialized='view') }}

with dim_artists as (
    select
        cast(artist_id as int) as artist_id,
        cast(artist_name as varchar) as artist_name
    from {{ source('playlist', 'artists') }}
)

{{ config(materialized='view') }}