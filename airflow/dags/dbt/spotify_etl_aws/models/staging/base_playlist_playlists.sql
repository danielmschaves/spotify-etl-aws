with source as (
      select * from {{ source('playlist', 'playlists') }}
),
renamed as (
    select
        

    from source
)
select * from renamed
  