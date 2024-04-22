from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Track  

class Playlist(BaseModel):
    id: str = Field(..., description="The Spotify ID for the playlist")
    name: str = Field(..., description="The name of the playlist")
    description: str = Field(..., description="Description of the playlist")
    owner_id: str = Field(..., description="Spotify ID of the user who owns the playlist")
    snapshot_id: str = Field(..., description="Version identifier for the playlist's contents")
    followers: int = Field(..., description="The number of followers the playlist has")
    public: bool = Field(..., description="Whether the playlist is publicly accessible")
    images: List[Dict] = Field(..., description="List of cover images for the playlist")
    tracks: List[Track] = Field(..., description="Tracks included in the playlist")

class Track(BaseModel):
    id: str = Field(..., description="The Spotify ID for the track")
    name: str = Field(..., description="The name of the track")
    album_id: str = Field(..., description="ID of the album the track belongs to")
    artist_ids: List[str] = Field(..., description="List of artist IDs associated with the track")
    duration_ms: int = Field(..., description="Duration of the track in milliseconds")
    popularity: int = Field(..., description="Popularity of the track on a scale of 0 to 100")
    explicit: bool = Field(..., description="Whether the track has explicit content")
    track_number: int = Field(..., description="The track number on its album")
    is_local: bool = Field(..., description="Whether the track is a local file")
