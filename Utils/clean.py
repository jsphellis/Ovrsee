import os
import json
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore

def load_firebase_credentials():
    try:
        with open('firebase_credentials.json') as f:
            firebase_creds = json.load(f)
        return firebase_creds
    except Exception as e:
        print(f"Failed to load Firebase credentials: {e}")
        return None

def initialize_firestore(creds):
    try:
        cred = credentials.Certificate(creds)
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        return db
    except Exception as e:
        print(f"Failed to initialize Firestore: {e}")
        return None

def get_creator_ids_by_emails(db, emails):
    creator_ids = []
    try:
        users_ref = db.collection('users')
        for email in emails:
            users = users_ref.where('email', '==', email).get()
            for user in users:
                print(f"Found user: {user.id} for email: {email}")
                creator_ids.append(user.id)
    except Exception as e:
        print(f"Error retrieving creator IDs: {e}")
    return creator_ids

def process_creator_videos(db, creator_id):
    print(f"Processing creator: {creator_id}")
    try:
        platforms_ref = db.collection('users').document(creator_id).collection('SocialMediaPlatforms')
        platforms = platforms_ref.get()
        if not platforms:
            print(f"No platforms found for creator: {creator_id}")
            return

        for platform in platforms:
            print(f"  Processing platform: {platform.id}")
            accounts_ref = platform.reference.collection('Accounts')
            accounts = accounts_ref.get()
            if not accounts:
                print(f"  No accounts found for platform: {platform.id}")
                continue

            for account in accounts:
                print(f"    Processing account: {account.id}")
                videos_ref = account.reference.collection('Videos')
                videos = videos_ref.get()
                if not videos:
                    print(f"    No videos found for account: {account.id}")
                    continue

                for video in videos:
                    print(f"      Processing video: {video.id}")
                    metrics_ref = video.reference.collection('Metrics')
                    metrics = metrics_ref.get()
                    if not metrics:
                        print(f"      No metrics found for video: {video.id}")
                        continue

                    for metric in metrics:
                        metric_data = metric.to_dict()
                        timestamp = metric_data.get('timestamp')

                        if isinstance(timestamp, datetime):
                            timestamp_str = timestamp.strftime('%Y%m%d-%H%M')
                        else:
                            timestamp_str = timestamp

                        try:
                            # Parse the timestamp string
                            timestamp = datetime.strptime(timestamp_str, '%Y%m%d-%H%M')

                            # Keep only entries that are on the hour
                            if timestamp.minute == 0:
                                print(f"        Keeping metric with timestamp {timestamp_str} (on the hour)")
                            else:
                                print(f"        Deleting metric with timestamp {timestamp_str} (not on the hour)")
                                metric.reference.delete()
                        except ValueError:
                            print(f"        Could not parse timestamp {timestamp_str} for metric {metric.id} in video {video.id}")
                        except TypeError:
                            print(f"        Unexpected type for timestamp: {timestamp}")

    except Exception as e:
        print(f"Error processing creator {creator_id}: {e}")

def main():
    print("Script started")
    print("Starting timestamp conversion process...")

    firebase_creds = load_firebase_credentials()
    if firebase_creds is None:
        return

    db = initialize_firestore(firebase_creds)
    if db is None:
        return

    # List of emails to process
    emails = [
        'creator@gmail.com',
        'jsphellis.2020@gmail.com'
    ]

    creator_ids = get_creator_ids_by_emails(db, emails)
    if not creator_ids:
        print("No creators found for the provided emails.")
        return

    for creator_id in creator_ids:
        process_creator_videos(db, creator_id)

    print("Script finished")

if __name__ == "__main__":
    main()
