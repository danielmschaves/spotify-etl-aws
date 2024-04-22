import requests
from requests.auth import HTTPBasicAuth
import os

# Replace these with your actual client_id and client_secret
client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")

def refresh_access_token(client_id, client_secret):
    url = "https://accounts.spotify.com/api/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "client_credentials"}
    response = requests.post(url, headers=headers, data=data, auth=HTTPBasicAuth(client_id, client_secret))
    if response.status_code == 200:
        print("Token retrieved successfully!")
        return response.json()['access_token']
    else:
        print(f"Failed to retrieve token: {response.status_code} - {response.text}")

token = refresh_access_token(client_id, client_secret)
print(f"Access Token: {token}")
