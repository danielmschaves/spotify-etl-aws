-- dim_album:
-- Columns:
-- album_id (INT - Primary Key)
-- album_name (VARCHAR)
-- release_date (DATE)
-- total_tracks (INT)
-- popularity (INT)
-- artist_id (INT - Foreign Key referencing dim_artist.artist_id)

{{ config(materialized='view') }}

with dim_album as (
    select
        cast(album_id as int) as album_id,
        cast(album_name as varchar) as album_name,
        cast(release_date as date) as release_date,
        cast(total_tracks as int) as total_tracks,
        cast(popularity as int) as popularity,
        cast(artist_id as int) as artist_id
    from {{ source('playlist', 'albums') }}
)