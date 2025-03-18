import logging
import pytz
from datetime import datetime, timedelta
from firebase_admin import firestore
from utils.tiktok_api import TikTokAPI
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

logging.basicConfig(level=logging.INFO)

class MetricsScraper:
    def __init__(self, max_workers=10):
        self.db = firestore.client()
        self.eastern = pytz.timezone('America/New_York')
        self.max_workers = max_workers
        self.thread_local = threading.local()

    def get_db(self):
        if not hasattr(self.thread_local, "db"):
            self.thread_local.db = firestore.client()
        return self.thread_local.db

    def get_users_with_linked_accounts(self):
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

    def process_account(self, user_id, account_data):
        db = self.get_db()
        platform_api = TikTokAPI()
        access_token = account_data['tokens']['access_token']
        
        open_id = account_data['tokens'].get('open_id')
        if not open_id:
            logging.error(f"open_id not found for TikTok user {user_id}, account: {account_data.get('username')}")
            return
        
        account_username = account_data['username']
        
        try:
            video_list = platform_api.fetch_video_list(access_token, open_id)
            logging.info(f"Fetched video list for user {user_id}, account {account_username}")
            self.store_videos_and_metrics(platform_api, user_id, platform_api.platform_name, account_username, video_list)
        except Exception as e:
            logging.error(f"Failed to fetch video list for user {user_id}, account {account_username}: {e}")

    def store_videos_and_metrics(self, platform_api, user_id, platform, account_username, media_list):
        try:
            videos_ref = self.db.collection('users').document(user_id).collection('SocialMediaPlatforms').document(platform).collection('Accounts').document(account_username).collection('Videos')
            video_data_list = media_list.get('data', {}).get('videos', [])
            fetched_video_ids = set()
            current_time = datetime.now(pytz.utc)
            
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

                if not existing_video.exists:
                    # Only add new videos if they're less than 24 hours old
                    if current_time - create_time_calculation <= timedelta(hours=24):
                        logging.info(f"New video detected within last 24 hours: {media_id}")
                        video_doc_ref.set(new_video_data)
                    else:
                        logging.info(f"Video {media_id} is older than 24 hours. Not adding to database.")
                        continue
                else:
                    existing_data = existing_video.to_dict()
                    # Preserve the current 'is_in_plan' value if it exists
                    if 'is_in_plan' in existing_data:
                        new_video_data['is_in_plan'] = existing_data['is_in_plan']
                    
                    if existing_data != new_video_data:
                        logging.info(f"Updating video data for {media_id}")
                        video_doc_ref.set(new_video_data)
                    else:
                        logging.info(f"No changes detected for video {media_id}, skipping update.")

                # Store metrics for all videos (new and existing)
                current_view_count = media['view_count']
                new_view_count = 0

                metrics_collection = video_doc_ref.collection('Metrics')
                latest_metric = metrics_collection.order_by('timestamp', direction=firestore.Query.DESCENDING).limit(1).get()
                
                if latest_metric:
                    last_view_count = latest_metric[0].to_dict().get('view_count', 0)
                    new_view_count = max(0, current_view_count - last_view_count)

                metrics = {
                    'comment_count': media['comment_count'],
                    'like_count': media['like_count'],
                    'view_count': current_view_count,
                    'share_count': media['share_count'],
                    'new_view_count': new_view_count,
                    'timestamp': current_time
                }

                eastern_timestamp = current_time.astimezone(pytz.timezone('America/New_York'))
                formatted_timestamp = self.format_timestamp(eastern_timestamp)

                metrics_ref = video_doc_ref.collection('Metrics').document(formatted_timestamp)
                metrics_ref.set(metrics)

                self.handle_historical_data_and_cleanup(video_doc_ref, eastern_timestamp)

                logging.info(f"Metrics added to Metrics collection for video {media_id}")

            # Update is_up to False for videos that are no longer available
            all_videos = videos_ref.stream()
            for video in all_videos:
                if video.id not in fetched_video_ids:
                    video.reference.update({
                        'is_up': False,
                        'is_tracked': False
                    })
                    logging.info(f"Video {video.id} is no longer available. Updated is_up and is_tracked to False.")

            logging.info(f'Successfully stored videos and metrics for user {user_id}, platform {platform}, and account {account_username}')
        except Exception as e:
            logging.error(f'Error storing videos and metrics in Firestore: {e}')

    def handle_historical_data_and_cleanup(self, video_doc_ref, current_timestamp):
        metrics_ref = video_doc_ref.collection('Metrics')
        historical_metrics_ref = video_doc_ref.collection('HistoricalMetrics')

        # Get the latest metric from Metrics
        latest_metric = metrics_ref.order_by('timestamp', direction=firestore.Query.DESCENDING).limit(1).get()

        if not latest_metric:
            return  # No metrics to process

        latest_metric_data = latest_metric[0].to_dict()
        latest_timestamp = latest_metric_data['timestamp']

        # Check if it's a new day
        if latest_timestamp.date() < current_timestamp.date():
            # Calculate the end of the previous day
            previous_day_end = current_timestamp.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(microseconds=1)
            
            # Get the last metric from the previous day
            last_metric_previous_day = metrics_ref.where('timestamp', '<=', previous_day_end).order_by('timestamp', direction=firestore.Query.DESCENDING).limit(1).get()

            if last_metric_previous_day:
                last_metric_data = last_metric_previous_day[0].to_dict()
                
                # Use the date of the previous day for the historical document
                historical_date = previous_day_end.strftime('%Y%m%d')
                
                # Add to HistoricalMetrics with the correct timestamp
                historical_metric_data = last_metric_data.copy()
                historical_metric_data['timestamp'] = previous_day_end  # Ensure the timestamp is set to the end of the previous day
                historical_metrics_ref.document(historical_date).set(historical_metric_data)
                
                logging.info(f"Added historical metric for date: {historical_date}")

        # Cleanup old metrics from Metrics
        cutoff_time = current_timestamp - timedelta(hours=48)
        old_metrics = metrics_ref.where('timestamp', '<', cutoff_time).stream()

        for old_metric in old_metrics:
            old_metric.reference.delete()
            logging.info(f"Deleted old metric with timestamp: {old_metric.to_dict()['timestamp']}")

    def format_timestamp(self, timestamp):
        return timestamp.strftime('%Y%m%d-%H%M')

    def run(self):
        users_with_accounts = self.get_users_with_linked_accounts()
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for user_id in users_with_accounts:
                for account_data in self.get_account_data(user_id):
                    futures.append(executor.submit(self.process_account, user_id, account_data))
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"An error occurred while processing an account: {e}")

        logging.info("Metric scraping completed for all accounts.")
