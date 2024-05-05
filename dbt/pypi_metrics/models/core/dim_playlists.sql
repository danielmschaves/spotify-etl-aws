-- dim_playlist:
-- Columns:
-- playlist_id (INT - Primary Key)
-- playlist_name (VARCHAR)
-- description (VARCHAR)
-- owner_id (VARCHAR)
-- followers (INT)
-- is_public (BOOLEAN)
-- Fact Table:

{{ config(materialized='view') }}

with dim_playlists as (
    select
        cast(playlist_id as int) as playlist_id,
        cast(playlist_name as varchar) as playlist_name,
        cast(description as varchar) as description,
        cast(owner_id as varchar) as owner_id,
        cast(followers as int) as followers,
        cast(is_public as boolean) as is_public
    from {{ source('playlist', 'playlists') }}
)

