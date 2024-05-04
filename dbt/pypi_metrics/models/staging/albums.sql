-- 3. Albums Model:

-- Purpose: Details about albums, including release dates and the tracks they contain.
-- Columns:
-- album_id: INT (Primary Key)
-- name: VARCHAR
-- release_date: DATE
-- total_tracks: INT
-- popularity: INT
-- artist_id: INT (Foreign Key to Artists)

{{ config(materialized='view') }}

with albums as (
    select
        cast(album_id as int) as album_id,
        cast(name as varchar) as album_name,
        cast(release_date as date) as album_release_date,
        cast(total_tracks as int) as album_total_tracks,
        cast(popularity as int) as album_popularity,
        cast(artist_id as int) as artist_id

    from {{ source('playlist', 'albums') }}
)