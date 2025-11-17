"""Spotify API client for fetching data."""

from typing import Dict, List, Optional
import requests
from requests.auth import HTTPBasicAuth
from loguru import logger


class SpotifyAPIClient:
    """
    Client for interacting with the Spotify API.

    Attributes:
        base_url: Base URL for the Spotify API.
        client_id: Spotify API client ID.
        client_secret: Spotify API client secret.
        session: HTTP session for making requests.
        access_token: Bearer token for API authentication.
    """

    def __init__(self, base_url: str, client_id: str, client_secret: str) -> None:
        """
        Initialize the Spotify API client.

        Args:
            base_url: Base URL for the Spotify API.
            client_id: Spotify API client ID.
            client_secret: Spotify API client secret.
        """
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.session = requests.Session()
        self.access_token = self.refresh_access_token()

    def refresh_access_token(self) -> str:
        """
        Retrieves a new access token from the Spotify API.

        Returns:
            Access token string.

        Raises:
            requests.HTTPError: If token retrieval fails.
        """
        url = "https://accounts.spotify.com/api/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"grant_type": "client_credentials"}

        response = self.session.post(
            url,
            headers=headers,
            data=data,
            auth=HTTPBasicAuth(self.client_id, self.client_secret),
        )
        if response.status_code != 200:
            logger.error(f"Failed to retrieve token: {response.status_code} - {response.text}")
        response.raise_for_status()
        return response.json()["access_token"]

    def _make_request(
        self, endpoint: str, params: Optional[Dict[str, str]] = None
    ) -> Optional[Dict]:
        """
        Makes an API request to the specified endpoint.

        Args:
            endpoint: The API endpoint.
            params: Optional parameters for the request.

        Returns:
            The response data as a dictionary, or None if the request fails.
        """
        url = f"{self.base_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        try:
            response = self.session.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(
                f"HTTP error occurred: {e.response.status_code} {e.response.reason} for URL {url}"
            )
            if e.response.status_code == 401:  # Unauthorized access, refresh token
                self.access_token = self.refresh_access_token()
                return self._make_request(endpoint, params)  # Retry the request
        except requests.exceptions.ConnectionError:
            logger.error("Connection error occurred")
        except requests.exceptions.Timeout:
            logger.error("Timeout occurred")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception: {e}")
        return None

    def search(
        self,
        query: str,
        search_type: str,
        genre: Optional[str] = None,
        limit: Optional[int] = 20,
        playlist_id: Optional[str] = None,
    ) -> Optional[List[Dict]]:
        """
        Generic search function for different Spotify entities.

        Args:
            query: The search query.
            search_type: Type of search (e.g., 'track', 'artist', 'playlist').
            genre: Genre to filter the search results (e.g., 'rock', 'jazz').
            limit: Limit the number of items to return.
            playlist_id: Specific ID of the playlist to fetch directly.

        Returns:
            List of entities data, or None if the request fails.
        """
        if playlist_id:
            endpoint = f"playlists/{playlist_id}"
        else:
            query_string = f"{query} genre:{genre}" if genre else query
            params = {"q": query_string, "type": search_type, "limit": limit}
            endpoint = "search"

        response = self._make_request(endpoint, params if not playlist_id else None)
        if response:
            if playlist_id:
                items = [response]  # Wrap the playlist response in a list
            else:
                items = response.get(search_type + "s", {}).get("items", [])

            logger.info(
                f"Search for {search_type}s '{query}' with genre '{genre}' returned {len(items)} items."
            )
            return items
        else:
            logger.error(
                f"Failed to retrieve {search_type}s for query '{query}' with genre '{genre}'"
            )
        return None

