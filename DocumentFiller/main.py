import os
import logging
import json
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import SERVER_TIMESTAMP
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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

class DailyUpdater:
    def __init__(self, max_workers=10):
        self.db = firestore.client()
        self.max_workers = max_workers
        self.thread_local = threading.local()

    def get_db(self):
        if not hasattr(self.thread_local, "db"):
            self.thread_local.db = firestore.client()
        return self.thread_local.db

    def update_user_account_count(self):
        """Updates each user's SocialMediaPlatforms with the count of accounts inside the Accounts collection."""
        db = self.get_db()
        users_ref = db.collection('users')
        users = users_ref.stream()

        for user in users:
            logging.info(f"Checking accounts for user: {user.id}")
            platforms_ref = user.reference.collection('SocialMediaPlatforms').document('TikTok')
            accounts_ref = platforms_ref.collection('Accounts')

            # Check if the Accounts subcollection contains any documents
            if accounts_ref.limit(1).get():
                # Count the number of accounts and update the SocialMediaPlatforms -> TikTok document
                account_count = len(list(accounts_ref.stream()))

                # Ensure the TikTok document exists before updating
                if platforms_ref.get().exists:
                    platforms_ref.update({
                        'account_count': account_count,
                        'updated_at': SERVER_TIMESTAMP
                    })
                    logging.info(f"Updated account count ({account_count}) for user {user.id}")
                else:
                    logging.warning(f"Document does not exist for user {user.id} in SocialMediaPlatforms/TikTok. Creating it...")
                    platforms_ref.set({
                        'account_count': account_count,
                        'updated_at': SERVER_TIMESTAMP
                    })
                    logging.info(f"Created and updated account count ({account_count}) for user {user.id}")
            else:
                logging.info(f"No accounts found for user {user.id} in SocialMediaPlatforms/TikTok.")

    def run(self):
        """Runs the daily updater for TikTok account counts."""
        db = self.get_db()

        # Update account counts for all users
        logging.info("Updating account counts for all users...")
        self.update_user_account_count()

        logging.info("Daily updates completed for all users.")

def document_filler_http(request):
    logging.info("Starting Document Filler...")
    updater = DailyUpdater()
    updater.run()

    return "Document filler completed successfully."

if __name__ == '__main__':
    document_filler_http(None)
