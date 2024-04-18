from pydantic import BaseModel, Field

class Artist(BaseModel):
    id: str = Field(..., description="The Spotify ID for the artist")
    name: str = Field(..., description="The name of the artist")
    popularity: int = Field(..., description="The popularity of the artist on a scale of 0 to 100")
    followers: int = Field(..., description="The number of followers the artist has")
    genres: List[str] = Field(..., description="List of genres the artist is associated with")
    image_url: str = Field(..., description="URL to the artist's image")

class Album(BaseModel):
    id: str = Field(..., description="The Spotify ID for the album")
    name: str = Field(..., description="The name of the album")
    release_date: str = Field(..., description="Release date of the album")
    total_tracks: int = Field(..., description="Total number of tracks in the album")
    album_type: str = Field(..., description="The type of the album, e.g., single or album")
    artist_ids: List[str] = Field(..., description="List of artist IDs associated with the album")
    cover_image_url: str = Field(..., description="URL to the album cover image")

class Track(BaseModel):
    id: str = Field(..., description="The Spotify ID for the track")
    name: str = Field(..., description="The name of the track")
    album_id: str = Field(..., description="ID of the album the track belongs to")
    artist_ids: List[str] = Field(..., description="List of artist IDs associated with the track")
    duration_ms: int = Field(..., description="Duration of the track in milliseconds")
    popularity: int = Field(..., description="Popularity of the track on a scale of 0 to 100")
    explicit: bool = Field(..., description="Whether the track has explicit content")
    track_number: int = Field(..., description="The track number on its album")
