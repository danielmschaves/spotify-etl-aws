from pydantic import BaseModel, Field, HttpUrl
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
    
class Album(BaseModel):
    id: str
    name: str
    release_date: str
    total_tracks: int
    album_type: str
 
class Track(BaseModel):
    id: str
    name: str
    album: Album  # Nested Album model
    artist_ids: List[str]
    duration_ms: int
    popularity: Optional[int]
    explicit: Optional[bool]
    track_number: Optional[int]

    @staticmethod
    def from_dict(data: dict) -> 'Track':
        return Track(
            id=data['id'],
            name=data['name'],
            album=Album(**data['album']),
            artist_ids=[artist['id'] for artist in data['artists']],
            duration_ms=data['duration_ms'],
            popularity=data.get('popularity'),
            explicit=data.get('explicit', False),
            track_number=data['track_number']
        )

class Playlist(BaseModel):
    id: str
    name: str
    description: Optional[str]
    owner_id: str
    snapshot_id: str
    followers: int
    public: bool
    tracks: List[Track] = Field(default_factory=list)

    @staticmethod
    def from_dict(data: dict) -> 'Playlist':
        return Playlist(
            id=data['id'],
            name=data['name'],
            description=data.get('description'),
            owner_id=data['owner']['id'],
            snapshot_id=data['snapshot_id'],
            followers=data['followers']['total'],
            public=data['public'],
            tracks=[Track.from_dict(track['track']) for track in data['tracks']['items']]
        )
