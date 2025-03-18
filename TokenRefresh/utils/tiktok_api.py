import requests
import logging
from urllib.parse import urlencode
import os

class TikTokAPI:
    def __init__(self):
        self.client_key = os.getenv('TIKTOK_CLIENT_KEY')
        self.client_secret = os.getenv('TIKTOK_CLIENT_SECRET')
        
        if not self.client_key or not self.client_secret:
            raise ValueError("TIKTOK_CLIENT_KEY and TIKTOK_CLIENT_SECRET must be set")

        logging.debug(f"TIKTOK_CLIENT_KEY: {self.client_key}")
        logging.debug(f"TIKTOK_CLIENT_SECRET: {self.client_secret}")

        self.token_url = "https://open.tiktokapis.com/v2/oauth/token/"
        self.user_info_url = "https://open.tiktokapis.com/v2/user/info/"
        logging.debug(f"TikTokAPI initialized with token_url: {self.token_url}")

    def refresh_access_token(self, refresh_token):
        data = {
            'client_key': self.client_key,
            'client_secret': self.client_secret,
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Cache-Control': 'no-cache'
        }
        logging.debug(f"Refreshing access token with refresh_token: {refresh_token}")
        response = requests.post(self.token_url, data=urlencode(data), headers=headers)
        logging.debug(f"Response Status Code: {response.status_code}")
        logging.debug(f"Response Text: {response.text}")
        response.raise_for_status()
        return response.json()

    def get_user_info(self, access_token):
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
        }
        # Specify the fields we want to retrieve
        params = {
            'fields': 'display_name,avatar_url,follower_count'
        }
        logging.debug(f"Fetching user info with access token: {access_token}")
        response = requests.get(self.user_info_url, headers=headers, params=params)
        logging.debug(f"Response Status Code: {response.status_code}")
        logging.debug(f"Response Text: {response.text}")
        response.raise_for_status()

        user_info = response.json().get('data', {}).get('user', {})
        logging.debug(f"User Info: {user_info}")

        # Return user info as a dictionary
        return {
            'profile_image': user_info.get('avatar_url'),
            'display_name': user_info.get('display_name'),
            'follower_count': user_info.get('follower_count')
        }
