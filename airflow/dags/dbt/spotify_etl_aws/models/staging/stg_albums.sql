{{ config(materialized='table') }}

with stg_albums as (
    select
        cast(album_id as varchar) as album_id,
        cast(name as varchar) as album_name,
        case
            when length(release_date) = 4 then cast(concat(release_date, '-01-01') as date)
            when length(release_date) = 7 then cast(concat(release_date, '-01') as date)
            when length(release_date) = 10 then cast(release_date as date)
            else null
        end as album_release_date,
        cast(total_tracks as int) as album_total_tracks
    from {{ source('playlist', 'albums') }}
)

select * from stg_albums
