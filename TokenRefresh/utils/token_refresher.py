import logging
from datetime import datetime
import os
import firebase_admin
from firebase_admin import credentials, firestore
import requests
from utils.tiktok_api import TikTokAPI

class TokenRefresher:
    def __init__(self):
        firebase_creds_json = os.getenv('FIREBASE_CREDENTIALS_JSON')
        if not firebase_creds_json:
            raise ValueError("FIREBASE_CREDENTIALS_JSON environment variable not set or is empty.")
        
        cred = credentials.Certificate(firebase_creds_json)
        firebase_admin.initialize_app(cred)
        self.db = firestore.client()

    def get_creator_user_ids(self):
        users_ref = self.db.collection('users')
        users_with_accounts = []
        
        for user in users_ref.stream():
            # Query the Accounts subcollection directly under the 'TikTok' document
            accounts_ref = user.reference.collection('SocialMediaPlatforms').document('TikTok').collection('Accounts')
            
            # Check if the Accounts subcollection contains any documents
            if accounts_ref.limit(1).get():
                users_with_accounts.append(user.id)
        
        return users_with_accounts

    def get_account_data(self, user_id):
        accounts_ref = self.db.collection('users').document(user_id).collection('SocialMediaPlatforms').document('TikTok').collection('Accounts')
        return [acc.to_dict() for acc in accounts_ref.stream()]

    def store_tokens(self, user_id, account_username, tokens, user_info):
        account_data = {
            'username': account_username,
            'display_name': user_info.get('display_name'),  # Store `display_name`
            'profileImage': user_info.get('profile_image'),  # Store profile image
            'follower_count': user_info.get('follower_count'),  # Store follower count
            'tokens': tokens,
            'updatedAt': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }
        doc_ref = self.db.collection('users').document(user_id).collection('SocialMediaPlatforms').document('TikTok').collection('Accounts').document(account_username)
        doc_ref.set(account_data, merge=True)
        logging.info(f'Successfully stored data for user {user_id}, account {account_username}')

    def refresh_token(self, user_id, account_data):
        refresh_token = account_data['tokens'].get('refresh_token')
        if refresh_token:
            try:
                tiktok_api = TikTokAPI()
                new_tokens = tiktok_api.refresh_access_token(refresh_token)
                
                if 'error' in new_tokens:
                    logging.error(f"Failed to refresh token for user {user_id}, TikTok account {account_data['username']}: {new_tokens['error_description']}")
                else:
                    user_info = tiktok_api.get_user_info(new_tokens['access_token'])
                    self.store_tokens(user_id, account_data['username'], new_tokens, user_info)
                    logging.info(f"Successfully refreshed token and account info for user {user_id}, TikTok account {account_data['username']}")
                    
            except requests.exceptions.HTTPError as http_err:
                logging.error(f"HTTP error occurred: {http_err.response.text}")
                if http_err.response.status_code == 401: 
                    logging.warning(f"Refresh token is invalid or expired for user {user_id}, TikTok account {account_data['username']}. Re-authentication required.")
            except Exception as e:
                logging.error(f"Error refreshing token for user {user_id}, TikTok account {account_data['username']}: {e}")
        else:
            logging.warning(f"No refresh token found for user {user_id}, TikTok account {account_data['username']}")

    def run(self):
        for user_id in self.get_creator_user_ids():
            for account_data in self.get_account_data(user_id):
                self.refresh_token(user_id, account_data)
