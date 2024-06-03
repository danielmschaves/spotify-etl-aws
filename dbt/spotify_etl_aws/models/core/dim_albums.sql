{{ config(materialized='table') }}

select
    distinct(album_id),
    album_name,
    album_release_date,
    album_total_tracks

from {{ref ('stg_albums')}}