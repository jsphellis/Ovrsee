import logging
import os
import requests
from urllib.parse import urlencode
from tenacity import retry, stop_after_attempt, wait_exponential

class TikTokAPI:
    def __init__(self):
        self.platform_name = 'TikTok'
        self.client_key = os.getenv('TIKTOK_CLIENT_KEY')
        self.client_secret = os.getenv('TIKTOK_CLIENT_SECRET')
        self.video_list_url = "https://open.tiktokapis.com/v2/video/list/"

    def fetch_video_list(self, access_token, open_id):
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        params = {
            'fields': 'cover_image_url,id,title,video_description,duration,embed_link,like_count,comment_count,share_count,view_count,create_time'
        }
        data = {
            'open_id': open_id,
            'max_count': 20
        }
        url = self.video_list_url + '?' + urlencode(params)
        response = self.make_request('POST', url, headers=headers, data=data)
        logging.info("Video list fetched successfully")
        return response.json()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def make_request(self, method, url, headers=None, params=None, data=None):
        try:
            response = requests.request(method, url, headers=headers, params=params, json=data)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTPError: {e.response.status_code} - {e.response.text}")
            raise

