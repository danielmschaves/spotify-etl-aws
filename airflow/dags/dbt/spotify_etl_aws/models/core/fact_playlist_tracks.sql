
{{ config(materialized='table') }}

with fact_playlist_tracks as (
    select
        playlist_id,
        track_id,
        track_number,
        track_duration_ms,
        track_popularity,
        track_explicit,
        dim_albums.album_release_date

    from {{ ref('stg_tracks') }}
    inner join {{ ref('dim_albums') }} on stg_tracks.album_id = dim_albums.album_id
)

select * from fact_playlist_tracks