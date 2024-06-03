{{ config(materialized='table') }}

with fact_playlist_tracks as (
    select
        pt.playlist_id,
        pt.track_id,
        pt.track_name,
        pt.track_number,
        pt.track_duration_ms,
        pt.track_popularity,
        pt.track_explicit,
        a.album_release_date,
        a.album_name,
        a.album_id,
        art.artist_name,
        art.artist_id
        
    from {{ ref('stg_tracks') }} as pt
    inner join {{ ref('dim_albums') }} as a on pt.album_id = a.album_id
    inner join {{ ref('dim_artists') }} as art on pt.artist_id = art.artist_id
)


select * from fact_playlist_tracks