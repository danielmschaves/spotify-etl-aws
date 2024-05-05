{{ config(materialized='view') }}

-- create first view in model, casting columns to correct data types, renaming columns and setting primary and foreign keys
-- this view will be used to create the final playlists table

with stg_playlists as (
    select
        cast(id as int) as playlist_id,
        cast(name as varchar) as playlist_name,
        cast(description as varchar) as playlist_description,
        cast(wner_id as varchar) as playlist_owner_id,
        cast(followers as int) as playlist_followers,
        cast(public as boolean) as playlist_public,

    -- from the database in mother duck named playlist, silver schema, table playlists
    
    from {{ source('playlist', 'playlists') }}

)