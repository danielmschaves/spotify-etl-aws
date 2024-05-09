
{{ config(materialized='table') }}

with dim_artists as (
    select
        artist_id,
        artist_name
        
    from {{ ref('stg_artists') }}
)

select * from dim_artists