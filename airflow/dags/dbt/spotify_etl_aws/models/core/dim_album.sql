-- dim_album:
-- Columns:
-- album_id (INT - Primary Key)
-- album_name (VARCHAR)
-- release_date (DATE)
-- total_tracks (INT)
-- popularity (INT)
-- artist_id (INT - Foreign Key referencing dim_artist.artist_id)

{{ config(materialized='table') }}

with dim_album as (
    select
        album_id,
        album_name,
        album_release_date,
        album_total_tracks,
        album_popularity,
        album_artist_id

    from {{ ref('stg_albums') }}
)