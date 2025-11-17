
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
select * from dim_playlists