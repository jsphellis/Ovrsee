import os
import logging
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore

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

# Function to convert create_time to datetime
def convert_create_time(create_time):
    if isinstance(create_time, dict) and 'seconds' in create_time:
        # Convert from map format with seconds and nanoseconds
        return datetime.utcfromtimestamp(create_time['seconds'] + create_time.get('nanoseconds', 0) / 1e9)
    elif isinstance(create_time, int):
        # Convert from Unix epoch in seconds
        return datetime.utcfromtimestamp(create_time)
    else:
        logging.warning(f"Unrecognized format for create_time: {create_time}")
        return None

# Function to check and update the create_time field in videos
def process_content_plan_videos(org_id, plan_id):
    videos_ref = db.collection('organizations').document(org_id).collection('contentPlans').document(plan_id).collection('videos').stream()

    for video in videos_ref:
        video_data = video.to_dict()
        video_id = video.id

        if 'create_time' in video_data:
            original_create_time = video_data['create_time']
            converted_create_time = convert_create_time(original_create_time)

            if converted_create_time:
                # Update the video document with the correct create_time format
                logging.info(f"Updating create_time for video {video_id} in content plan {plan_id}. Original: {original_create_time}, Converted: {converted_create_time}")
                video.reference.update({'create_time': converted_create_time})
            else:
                logging.warning(f"Failed to convert create_time for video {video_id} in content plan {plan_id}")
        else:
            logging.warning(f"create_time field missing for video {video_id} in content plan {plan_id}")

# Process organizations and their content plans
def process_content_plans():
    orgs_ref = db.collection('organizations').stream()

    for org in orgs_ref:
        org_id = org.id
        content_plans_ref = db.collection('organizations').document(org_id).collection('contentPlans').stream()

        for plan in content_plans_ref:
            plan_id = plan.id
            logging.info(f"Processing content plan {plan_id} in organization {org_id}")
            process_content_plan_videos(org_id, plan_id)

if __name__ == '__main__':
    logging.info("Starting conversion of create_time...")

    # Process content plans and update create_time
    process_content_plans()

    logging.info("Conversion of create_time completed.")
