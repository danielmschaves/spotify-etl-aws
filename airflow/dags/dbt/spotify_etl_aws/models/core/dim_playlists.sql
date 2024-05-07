-- dim_playlist:
-- Columns:
-- playlist_id (INT - Primary Key)
-- playlist_name (VARCHAR)
-- description (VARCHAR)
-- owner_id (VARCHAR)
-- followers (INT)
-- is_public (BOOLEAN)
-- Fact Table:

{{ config(materialized='table') }}

with dim_playlists as (
    select
        playlist_id,
        playlist_name,
        playlist_description,
        playlist_owner_id,
        playlist_followers,
        playlist_public
        
    from {{ ref('stg_playlists') }}
)

