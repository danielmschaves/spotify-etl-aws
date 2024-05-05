from pydantic import BaseModel, Field, HttpUrl, root_validator, validator
from typing import List, Optional, Dict

class Image(BaseModel):
    height: Optional[int]
    url: HttpUrl
    width: Optional[int]

class Track(BaseModel):
    id: str
    name: str
    album_id: str = Field(..., alias='album')
    artist_ids: List[str] = Field(..., alias='artists')
    duration_ms: int
    popularity: int
    explicit: bool
    track_number: int

class Playlist(BaseModel):
    id: str
    name: str
    description: Optional[str]
    owner_id: str = Field(..., alias='owner')
    snapshot_id: str
    followers: int
    public: bool
    images: List[Image]
    tracks: List[Track]

    @root_validator(pre=True)
    def extract_fields(cls, values):
        if 'owner' in values:
            values['owner_id'] = values['owner']['id']
        if 'followers' in values:
            values['followers'] = values['followers']['total']
        if 'tracks' in values:
            values['tracks'] = values['tracks']['items']  # Assuming the track details are nested under 'items'
        return values

    @validator('tracks', pre=True, each_item=True)
    def unpack_track_details(cls, track):
        if 'track' in track:
            return track['track']
        return track
