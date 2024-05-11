{{ config(materialized='table' ) }}

with stg_albums as (
    select
        cast(album_id as varchar) as album_id,
        cast(name as varchar) as album_name,
        cast(release_date as date) as album_release_date,
        cast(total_tracks as int) as album_total_tracks

    from {{ source('playlist', 'albums') }}
)

select * from stg_albums