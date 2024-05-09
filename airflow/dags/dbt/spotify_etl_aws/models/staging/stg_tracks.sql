{{ config(materialized='table') }}

with stg_tracks as (
    select
        cast(track_id as varchar) as track_id,
        cast(name as varchar) as track_name,
        cast(playlist_id as varchar) as playlist_id,
        cast(album_id as varchar) as album_id,
        cast(duration_ms as int) as track_duration_ms,
        cast(popularity as int) as track_popularity,
        cast(explicit as boolean) as track_explicit,
        cast(track_number as int) as track_number,
        cast(album_release_date as date) as album_release_date,
        cast(artist_id as varchar) as artist_id
    from {{ source('playlist', 'tracks') }}
)
select * from stg_tracks