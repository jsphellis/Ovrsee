import os
import logging
import json
from dotenv import load_dotenv
import firebase_admin
import pytz
from datetime import datetime, timedelta
from firebase_admin import credentials, firestore
from utils.tiktok_api import TikTokAPI
from concurrent.futures import ThreadPoolExecutor
from google.cloud import functions_v1

# Load environment variables from .env file
load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO)

def initialize_firebase():
    firebase_creds_path = os.getenv('FIREBASE_CREDENTIALS_JSON')
    logging.info(f"Firebase Credentials Path: {firebase_creds_path}")

    if not firebase_creds_path or not os.path.exists(firebase_creds_path):
        raise ValueError("Firebase credentials JSON file path is empty or does not exist")

    with open(firebase_creds_path, 'r') as f:
        firebase_creds = json.load(f)

    cred = credentials.Certificate(firebase_creds)
    firebase_admin.initialize_app(cred)
    logging.info("Firebase initialized successfully")
    return firestore.client()

# Initialize Firebase
db = initialize_firebase()

def process_account(user_id, account_data):
    """
    Process a single TikTok account for a given user.
    """
    logging.info(f"Processing account {account_data['username']} for user {user_id}")
    
    platform_api = TikTokAPI()
    access_token = account_data['tokens']['access_token']
    open_id = account_data['tokens'].get('open_id')
    
    if not open_id:
        logging.error(f"open_id not found for TikTok user {user_id}, account: {account_data['username']}")
        return

    account_username = account_data['username']

    # Fetch video list using TikTok API
    try:
        video_list = platform_api.fetch_video_list(access_token, open_id)
        logging.info(f"Fetched video list for user {user_id}, account {account_username}")
        store_new_videos(db, platform_api, user_id, 'TikTok', account_username, video_list)
    except Exception as e:
        logging.error(f"Failed to fetch video list for user {user_id}, account {account_username}: {e}")

def check_new_videos(uid):
    """
    Function to check for new videos for all TikTok accounts
    of a specific user and update Firestore with new videos.
    """
    logging.info(f"Starting video check for user {uid}")
    
    # Get the TikTok account details for the user
    accounts_ref = db.collection('users').document(uid).collection('SocialMediaPlatforms').document('TikTok').collection('Accounts')
    accounts = [acc.to_dict() for acc in accounts_ref.stream()]

    if not accounts:
        logging.error(f"No TikTok accounts found for user {uid}")
        return

    # Use ThreadPoolExecutor to process all accounts concurrently
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_account, uid, account) for account in accounts]
        for future in futures:
            try:
                future.result()  # Wait for each thread to finish
            except Exception as e:
                logging.error(f"Error processing account: {str(e)}")

    logging.info(f"Video scan completed for all accounts for user {uid}")

def store_new_videos(db, platform_api, user_id, platform, account_username, media_list):
    videos_ref = db.collection('users').document(user_id).collection('SocialMediaPlatforms').document(platform).collection('Accounts').document(account_username).collection('Videos')
    video_data_list = media_list.get('data', {}).get('videos', [])
    fetched_video_ids = set()
    current_time = datetime.now(pytz.utc)  # This is a timezone-aware datetime

    for media in video_data_list:
        media_id = media['id']
        fetched_video_ids.add(media_id)

        video_doc_ref = videos_ref.document(media_id)
        existing_video = video_doc_ref.get()

        # Handle create_time conversion
        create_time = media.get('create_time', '')
        if isinstance(create_time, int):  # If create_time is a Unix timestamp
            create_time_calculation = datetime.utcfromtimestamp(create_time)  # Convert to datetime
            create_time_calculation = pytz.utc.localize(create_time_calculation)  # Make timezone-aware (UTC)

        # Skip videos that are older than 24 hours
        if current_time - create_time_calculation > timedelta(hours=24):
            logging.info(f"Video {media_id} is older than 24 hours. Skipping.")
            continue

        new_video_data = {
            'title': media.get('title', ''),
            'description': media.get('video_description', ''),
            'create_time': create_time,
            'share_url': media['embed_link'],
            'thumbnail_url': media.get('cover_image_url', ''),
            'is_up': True,
            'is_tracked': True,
            'is_in_plan': False
        }

        # If the video is new (not existing), add it
        if not existing_video.exists:
            logging.info(f"New video detected within last 24 hours: {media_id}")
            video_doc_ref.set(new_video_data)
        else:
            existing_data = existing_video.to_dict()
            if 'is_in_plan' in existing_data:
                new_video_data['is_in_plan'] = existing_data['is_in_plan']
            
            if existing_data != new_video_data:
                logging.info(f"Updating video data for {media_id}")
                video_doc_ref.set(new_video_data)
            else:
                logging.info(f"No changes detected for video {media_id}, skipping update.")

    logging.info(f'Successfully stored new videos for user {user_id}, platform {platform}, and account {account_username}')

def video_refresh_http(request):
    """
    Cloud Function HTTP trigger. This function runs the TikTok video scan for all accounts of the user.
    Handles CORS preflight requests.
    """
    # Handle CORS preflight request
    if request.method == 'OPTIONS':
        # Allows GET and POST requests from any origin with the Content-Type
        # header and caches preflight response for an hour
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    # Handle actual POST request
    headers = {
        'Access-Control-Allow-Origin': '*',
    }

    request_json = request.get_json()
    if not request_json or 'uid' not in request_json:
        return {'error': 'User ID (uid) not provided'}, 400, headers

    uid = request_json['uid']
    try:
        check_new_videos(uid)
        return {'status': f'Video scan completed for all TikTok accounts of user {uid}'}, 200, headers
    except Exception as e:
        logging.error(f"Error during video scan: {str(e)}")
        return {'error': str(e)}, 500, headers

# If executed as a standalone script
if __name__ == '__main__':
    video_refresh_http(None)