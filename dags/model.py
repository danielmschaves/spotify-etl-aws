from pydantic import BaseModel, HttpUrl, Field, validator
from typing import List, Optional, Dict

class Image(BaseModel):
    height: Optional[int]
    url: HttpUrl
    width: Optional[int]

class Artist(BaseModel):
    id: str
    name: str
    popularity: Optional[int]
    followers: Optional[int]
    genres: List[str] = []
    image_url: Optional[HttpUrl]

class Album(BaseModel):
    id: str
    name: str
    release_date: str
    total_tracks: int
    album_type: str
    artist_ids: List[str]
    cover_image_url: Optional[HttpUrl]

class Track(BaseModel):
    id: str
    name: str
    album_id: Optional[str]
    artist_ids: List[str]
    duration_ms: int
    popularity: Optional[int]
    explicit: Optional[bool]
    track_number: Optional[int]

    @validator('album_id', pre=True, always=True)
    def extract_album_id(cls, v, values, **info):
        if 'album' in values and 'id' in values['album']:
            return values['album']['id']
        return v

    @validator('artist_ids', pre=True)
    def extract_artist_ids(cls, v, values, **info):
        artists = values.get('artists', [])
        return [artist.get('id') for artist in artists if 'id' in artist]


class PlaylistTrack(BaseModel):
    """
    This model represents a single track within a playlist.
    """
    id: str
    name: str
    # Add other relevant track fields as needed (e.g., duration_ms, artist_ids)

class Playlist(BaseModel):
    id: str
    name: str
    description: Optional[str]
    owner_id: Optional[Dict[str, str]] = None
    snapshot_id: str
    followers: Dict[str, Optional[int]] = {}
    public: bool
    images: List[Image]
    tracks: List[PlaylistTrack] = []
    
    class Config:
        from_attributes = True

# Update: Set a default empty list for 'tracks'
    