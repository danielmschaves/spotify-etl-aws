-- fact_playlist_tracks:
-- Columns:
-- playlist_id (INT - Foreign Key referencing dim_playlist.playlist_id)
-- track_id (INT - Foreign Key referencing stg_tracks.track_id) Note: Not referencing a dimension table as it's not needed for core analysis
-- track_number (INT)
-- duration_ms (INT)
-- popularity (INT)
-- is_explicit (BOOLEAN)
-- album_release_date (DATE) Denormalized from stg_tracks for faster joins

{{ config(materialized='table') }}

with fact_playlist_tracks as (
    select
        playlist_id,
        track_id,
        track_number,
        track_duration_ms,
        track_popularity,
        track_explicit,
        album_release_date

    from {{ ref('stg_tracks') }}
    inner join {{ ref('dim_album') }} on stg_tracks.album_id = albums.album_id
)