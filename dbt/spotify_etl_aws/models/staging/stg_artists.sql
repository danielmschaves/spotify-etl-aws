
{{ config(materialized='table') }}

with stg_artists as (
    select
        cast(artist_id as varchar) as artist_id,
        cast(name as varchar) as artist_name,
        cast(track_id as varchar) as track_id
    from {{ source('playlist', 'artists') }}
)
select * from stg_artists