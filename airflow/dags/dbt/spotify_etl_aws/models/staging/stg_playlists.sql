{{ config(materialized='table') }}

with stg_playlists as (
    select
        cast(id as varchar) as playlist_id,
        cast(name as varchar) as playlist_name,
        cast(description as varchar) as playlist_description,
        cast(owner_id as varchar) as playlist_owner_id,  
        cast(followers as int) as playlist_followers,
        cast(public as boolean) as playlist_public
    from {{ source('playlist', 'playlists') }}
)
select * from stg_playlists