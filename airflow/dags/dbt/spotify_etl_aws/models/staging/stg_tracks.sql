{{ config(materialized='table') }}

with stg_tracks as (
    select
        cast(track_id as varchar) as track_id,
        cast(name as varchar) as track_name,
        cast(playlist_id as varchar) as playlist_id,
        cast(album_id as varchar) as album_id,
        cast(duration_ms as int) as track_duration_ms,
        cast(popularity as int) as track_popularity,
        cast(explicit as boolean) as track_explicit,
        cast(track_number as int) as track_number,
        case
            when length(album_release_date) = 4 then cast(concat(album_release_date, '-01-01') as date)
            when length(album_release_date) = 7 then cast(concat(album_release_date, '-01') as date)
            when length(album_release_date) = 10 then cast(album_release_date as date)
            else null
        end as album_release_date,
        cast(artist_id as varchar) as artist_id
    from {{ source('playlist', 'tracks') }}
)

select * from stg_tracks
