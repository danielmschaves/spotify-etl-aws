-- fact_playlist_tracks:
-- Columns:
-- playlist_id (INT - Foreign Key referencing dim_playlist.playlist_id)
-- track_id (INT - Foreign Key referencing stg_tracks.track_id) Note: Not referencing a dimension table as it's not needed for core analysis
-- track_number (INT)
-- duration_ms (INT)
-- popularity (INT)
-- is_explicit (BOOLEAN)
-- album_release_date (DATE) Denormalized from stg_tracks for faster joins

{{ config(materialized='view') }}

with fact_playlist_tracks as (
    select
        cast(playlist_id as int) as playlist_id,
        cast(track_id as int) as track_id,
        cast(track_number as int) as track_number,
        cast(duration_ms as int) as track_duration_ms,
        cast(popularity as int) as track_popularity,
        cast(is_explicit as boolean) as is_explicit,
        cast(album_release_date as date) as album_release_date
    from {{ source('playlist', 'tracks') }}
    inner join {{ source('playlist', 'albums') }} on stg_tracks.album_id = albums.album_id
)