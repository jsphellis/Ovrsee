import os
import json
import logging
from datetime import datetime, timedelta
from google.cloud import firestore
import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv
from pytz import UTC  # Ensure UTC handling for datetime
from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp  # Correct import for Firestore Timestamp

# Load environment variables from .env file
load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Initialize Firebase
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

# Firebase client
db = initialize_firebase()

def get_organization_name(org_id):
    # Fetch organization name
    org_ref = db.collection('organizations').document(org_id)
    org_data = org_ref.get().to_dict()
    return org_data.get('name', 'Unknown Organization')

def move_to_historical_content_plan(ref_id, plan_id, plan_data, ref_type, additional_field, completion_percentage, metrics):
    print(additional_field)
    # Remove new_view_count and retain other fields
    if 'new_view_count' in plan_data:
        del plan_data['new_view_count']

    # Fields we are keeping
    retained_fields = {
        'dateCreated': plan_data['dateCreated'],
        'managerId': plan_data['managerId'],
        'numberOfDays': plan_data['numberOfDays'],
        'numberOfVideos': plan_data['numberOfVideos'],
        'requireW9': plan_data.get('requireW9', False),  # Default to False if not present
        'retainerAmount': plan_data['retainerAmount'],
        'startDate': plan_data['startDate'],
        'status': plan_data['status'],
        'completion_percentage': completion_percentage
    }

    # Add the provided metrics
    retained_fields['metrics'] = metrics

    if ref_type == 'organization':
        print(f"Moving content plan {plan_id} to organization's historicalContentPlans.")
        retained_fields['userId'] = additional_field  # Add userId for organization entry
        historical_ref = db.collection('organizations').document(ref_id).collection('historicalContentPlans')
    elif ref_type == 'user':
        print(f"Moving content plan {plan_id} to user's historicalContentPlans.")
        retained_fields['organizationName'] = additional_field  # Add organization name for user entry
        historical_ref = db.collection('users').document(ref_id).collection('historicalContentPlans')

    # Convert any Firestore timestamp objects to ISO format
    for key, value in retained_fields.items():
        if isinstance(value, firestore.SERVER_TIMESTAMP.__class__):
            retained_fields[key] = value.to_date().isoformat()  # Convert Firestore Timestamp to ISO format
        elif isinstance(value, datetime):
            retained_fields[key] = value.isoformat()  # Convert datetime object to ISO format

    # Set the historical content plan in Firestore (no deletion of the original)
    historical_ref.document(plan_id).set(retained_fields)

    # Print out the simulated historical data move
    print(f"Simulated Historical Data for {ref_type}:\n{json.dumps(retained_fields, indent=2)}\n")

def process_historical_content_plan():
    current_date = datetime.utcnow().date()

    # Fetch all organizations
    organizations_ref = db.collection('organizations')
    organizations = organizations_ref.stream()

    for org in organizations:
        org_id = org.id
        org_name = get_organization_name(org_id)
        content_plans_ref = organizations_ref.document(org_id).collection('contentPlans')
        active_plans = content_plans_ref.where('status', '==', 'active').stream()

        for plan in active_plans:
            plan_data = plan.to_dict()
            plan_id = plan.id
            user_id = plan_data['userId']
            start_date = plan_data['startDate'].date()
            number_of_days = plan_data['numberOfDays']
            end_date = start_date + timedelta(days=number_of_days)

            # Check if content plan has expired
            if current_date >= end_date:
                print(f"Content plan {plan_id} has expired. Moving to historical content plans.")

                # Calculate completion percentage based on unique post days
                unique_days_count = calculate_unique_post_days(org_id, plan_id, start_date, end_date)  # Add start_date and end_date
                completion_percentage = (unique_days_count / number_of_days) * 100
                print(f"Completion Percentage: {completion_percentage}%")

                # Update the content plan status to "completed"
                plan_data['status'] = 'completed'

                # Fetch the most recent daily metrics for both organization and user
                metrics = fetch_latest_metrics(org_id, plan_id)

                # Move to organization and user historicalContentPlans
                move_to_historical_content_plan(org_id, plan_id, plan_data, 'organization', user_id, completion_percentage, metrics)
                move_to_historical_content_plan(user_id, plan_id, plan_data, 'user', org_name, completion_percentage, metrics)

                # Simulate keeping the original content plan intact (no deletion)
                print(f"Deleting original content plan {plan_id} in active content plans.\n")
                content_plans_ref.document(plan_id).delete()

def fetch_latest_metrics(org_id, plan_id):
    # Fetch the most recent daily entry and remove unwanted fields (timestamp and new_view_count)
    metrics_ref = db.collection('organizations').document(org_id).collection('contentPlans').document(plan_id).collection('metrics').document('daily').collection('data')
    latest_daily_entry = metrics_ref.order_by('timestamp', direction=firestore.Query.DESCENDING).limit(1).get()
    
    if latest_daily_entry:
        daily_data = latest_daily_entry[0].to_dict()
        if 'timestamp' in daily_data:
            del daily_data['timestamp']
        if 'new_view_count' in daily_data:
            del daily_data['new_view_count']
        return daily_data
    else:
        return {}  # Return an empty map if no metrics are available

def calculate_unique_post_days(org_id, plan_id, start_date, end_date):
    # Count unique days for video posts using 'create_time'
    videos_ref = db.collection('organizations').document(org_id).collection('contentPlans').document(plan_id).collection('videos')
    videos = videos_ref.stream()

    unique_days = set()
    print(f"\nCalculating unique post days for content plan {plan_id}...\n")

    for video in videos:
        video_data = video.to_dict()

        # Ensure 'create_time' exists and is a valid format
        if 'create_time' in video_data:
            create_time = video_data['create_time']
            post_date = None

            # Handle Firestore Timestamp object
            if isinstance(create_time, Timestamp):
                post_date = datetime.utcfromtimestamp(create_time.seconds).date()  # Convert Firestore Timestamp to date
            elif isinstance(create_time, datetime):
                post_date = create_time.date()  # Already a datetime object
            else:
                logging.warning(f"create_time format is unexpected for video {video.id} in content plan {plan_id}. Skipping this video.")
                continue  # Skip the entry if it cannot be parsed

            # Check if the post date is within the content plan's start and end dates
            if start_date <= post_date <= end_date:
                # Print out the video and its calculated post date
                print(f"Video ID: {video.id} | create_time: {create_time} | Parsed UTC Date: {post_date}")
                unique_days.add(post_date)
            else:
                logging.warning(f"Video {video.id} has a post date {post_date} outside the content plan date range {start_date} - {end_date}. Skipping.")
        else:
            logging.warning(f"No create_time field found for video {video.id} in content plan {plan_id}.")

    print(f"\nUnique Days Set: {unique_days}")
    return len(unique_days)

def historical_content_plan_http(request):
    process_historical_content_plan()

if __name__ == "__main__":
    historical_content_plan_http(None)
