-- 4. Artists Model:

-- Purpose: Information about artists, including their names and tracks.
-- Columns:
-- artist_id: INT (Primary Key)
-- name: VARCHAR
-- track_id: INT (Foreign Key to Tracks)

{{ config(materialized='view') }}

with stg_artists as (
    select
        cast(artist_id as varchar) as artist_id,
        cast(name as varchar) as artist_name,
        cast(track_id as varchar) as track_id
    from {{ source('playlist', 'artists') }}
)