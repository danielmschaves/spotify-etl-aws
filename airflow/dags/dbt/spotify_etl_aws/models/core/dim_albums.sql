


{{ config(materialized='table') }}

select
    album_id,
    album_name,
    album_release_date,
    album_total_tracks,
    album_track_id

from {{ref ('stg_albums')}}
